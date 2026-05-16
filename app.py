from fastapi import FastAPI, Response, HTTPException, Request
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import FileResponse
import feedparser
import html
import re
import time
import os
import hashlib
import hmac
from pymongo import UpdateOne
from motor.motor_asyncio import AsyncIOMotorClient
import asyncio
from contextlib import asynccontextmanager
import httpx
import trafilatura
from cachetools import TTLCache
import io
import urllib.parse

try:
    from PIL import Image
except ImportError:
    pass

full_content_cache = TTLCache(maxsize=200, ttl=86400)
image_cache = TTLCache(maxsize=500, ttl=86400 * 7)

prefetch_queue = asyncio.Queue()

feedparser.USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"

SECRET_KEY = os.environ.get("SECRET_KEY", os.urandom(32).hex()).encode("utf-8")


def sign_url(url: str) -> str:
    return hmac.new(SECRET_KEY, url.encode("utf-8"), hashlib.sha256).hexdigest()


def verify_url(url: str, sign: str) -> bool:
    expected_sign = sign_url(url)
    return hmac.compare_digest(expected_sign, sign)


MONGO_URI = os.environ.get("MONGO_URI", "")
space_id_raw = os.environ.get("SPACE_ID", "default_space")
space_id_safe = space_id_raw.replace("/", "_").replace("-", "_").replace(".", "_")

_mongo_client = None


def get_db_collections():
    global _mongo_client
    if not MONGO_URI:
        return None, None

    if _mongo_client is None:
        _mongo_client = AsyncIOMotorClient(MONGO_URI, serverSelectionTimeoutMS=5000)

    db = _mongo_client["news_sites_db"]
    return db[f"news_items_{space_id_safe}"], db[f"news_meta_{space_id_safe}"]


def get_image_collection():
    global _mongo_client
    if not MONGO_URI or _mongo_client is None:
        return None
    db = _mongo_client["news_sites_db"]
    return db[f"images_{space_id_safe}"]


if MONGO_URI:
    print("初始化 MongoDB 异步客户端...")
else:
    print("未配置 MONGO_URI，以纯内存模式运行")

RSS_FEEDS = {
    "importnews": {
        "name": "要闻",
        "url": "https://www.chinanews.com.cn/rss/importnews.xml",
    },
    "china": {"name": "时政", "url": "https://www.chinanews.com.cn/rss/china.xml"},
    "world": {"name": "国际", "url": "https://www.chinanews.com.cn/rss/world.xml"},
    "society": {"name": "社会", "url": "https://www.chinanews.com.cn/rss/society.xml"},
    "culture": {"name": "文娱", "url": "https://www.chinanews.com.cn/rss/culture.xml"},
    "sports": {"name": "体育", "url": "https://www.chinanews.com.cn/rss/sports.xml"},
    "life": {"name": "生活", "url": "https://www.chinanews.com.cn/rss/life.xml"},
    "health": {"name": "健康", "url": "https://www.chinanews.com.cn/rss/jk.xml"},
    "law": {"name": "法治", "url": "https://www.chinanews.com.cn/rss/fz.xml"},
    "creative": {
        "name": "即时",
        "url": "https://www.chinanews.com.cn/rss/scroll-news.xml",
    },
    "tech": {"name": "科技", "url": "https://www.solidot.org/index.rss"},
    "theory": {"name": "理论", "url": "https://www.chinanews.com.cn/rss/theory.xml"},
}

CACHE_TTL = 600
news_cache = {cat_id: {"timestamp": 0, "items": []} for cat_id in RSS_FEEDS.keys()}


class FakeItem:
    def __init__(self, data):
        self.__dict__.update(data)

    def get(self, key, default=None):
        return getattr(self, key, default)


def serialize_item(item, cat_id):
    pub_parsed = None
    if item.get("published_parsed"):
        pub_parsed = time.mktime(item.published_parsed)
    return {
        "cat_id": cat_id,
        "title": item.get("title", ""),
        "link": item.get("link", ""),
        "summary": item.get("summary", item.get("description", "")),
        "published": item.get("published", ""),
        "published_parsed": pub_parsed,
        "fetch_time": time.time(),
    }


def deserialize_item(doc):
    if doc.get("published_parsed"):
        doc["published_parsed"] = time.localtime(doc["published_parsed"])
    return FakeItem(doc)


async def load_all_from_db():
    news_col, meta_col = get_db_collections()
    if news_col is None:
        return
    try:
        await news_col.create_index([("cat_id", 1), ("published_parsed", -1)])
        await news_col.create_index([("link", 1)], unique=True)

        for cat_id in RSS_FEEDS.keys():
            docs = (
                await news_col.find({"cat_id": cat_id})
                .sort("published_parsed", -1)
                .limit(200)
                .to_list(length=200)
            )
            if docs:
                news_cache[cat_id]["items"] = [deserialize_item(doc) for doc in docs]
                meta = await meta_col.find_one({"_id": cat_id})
                if meta:
                    news_cache[cat_id]["timestamp"] = meta.get("last_sync", 0)
        print("成功从 MongoDB 加载缓存数据")
    except Exception as e:
        print(f"恢复缓存失败: {e}")


async def sync_feed(cat_id: str):
    if cat_id not in RSS_FEEDS:
        return

    cache = news_cache[cat_id]
    current_time = time.time()

    try:

        def _parse():
            return feedparser.parse(RSS_FEEDS[cat_id]["url"])

        feed = await asyncio.to_thread(_parse)
        new_entries = feed.entries
        if not new_entries:
            return

        existing_links = {item.get("link", "") for item in cache["items"]}
        to_save = []
        added_count = 0

        for item in new_entries:
            if item.get("link", "") not in existing_links:
                cache["items"].insert(added_count, item)
                to_save.append(serialize_item(item, cat_id))

                prefetch_queue.put_nowait((item.get("link", ""), cat_id))

                added_count += 1

        if to_save:
            ops = [
                UpdateOne({"link": item["link"]}, {"$set": item}, upsert=True)
                for item in to_save
            ]
            col, meta_col = get_db_collections()
            if col is not None:
                try:
                    await col.bulk_write(ops, ordered=False)
                    await meta_col.update_one(
                        {"_id": cat_id},
                        {"$set": {"last_sync": current_time}},
                        upsert=True,
                    )
                    last_docs = (
                        await col.find({"cat_id": cat_id})
                        .sort("published_parsed", -1)
                        .skip(500)
                        .limit(1)
                        .to_list(length=1)
                    )
                    if last_docs:
                        cutoff_time = last_docs[0]["published_parsed"]
                        await col.delete_many(
                            {"cat_id": cat_id, "published_parsed": {"$lt": cutoff_time}}
                        )
                except Exception as ex:
                    print(f"MongoDB 写入失败 ({cat_id}): {ex}")

        cache["items"] = cache["items"][:500]
        cache["timestamp"] = current_time
        return True
    except Exception as e:
        print(f"同步失败 ({cat_id}): {e}")
        return False


async def background_refresher():
    await asyncio.sleep(5)
    while True:
        for cat_id in RSS_FEEDS.keys():
            await sync_feed(cat_id)
            await asyncio.sleep(2)
        await asyncio.sleep(CACHE_TTL)


async def prefetch_worker():
    """后台异步提取新闻正文及缓存图片的队列工作任务"""
    await asyncio.sleep(10)
    print("后台预抓取工作线程已启动，等待新文章加入队列...")
    while True:
        try:
            item_link, cat = await prefetch_queue.get()
            print(f"[预抓取] 正在处理新文章: {item_link}")
            full_content = await fetch_article_content(item_link, cat)
            if full_content:
                img_urls = re.findall(r"\[IMAGE:(.*?)\]", full_content)
                for img_url in img_urls:
                    print(f"  -> [预抓取] 发现图片，准备缓存: {img_url}")
                    await fetch_and_cache_image(img_url)
                    await asyncio.sleep(1)
            prefetch_queue.task_done()
            await asyncio.sleep(2)
        except Exception as e:
            print(f"预抓取队列任务出错: {e}")
            await asyncio.sleep(5)


@asynccontextmanager
async def lifespan(app: FastAPI):
    await load_all_from_db()
    asyncio.create_task(background_refresher())
    asyncio.create_task(prefetch_worker())
    yield
    global _mongo_client
    if _mongo_client is not None:
        _mongo_client.close()


app = FastAPI(lifespan=lifespan)
app.add_middleware(GZipMiddleware, minimum_size=500)


async def get_news_items(cat_id: str):
    if cat_id not in RSS_FEEDS:
        cat_id = "importnews"
    cache = news_cache[cat_id]
    if not cache["items"]:
        await sync_feed(cat_id)
    return cache["items"]


def generate_xhtml_response(title, body_content):
    xhtml_str = f"""<?xml version="1.0" encoding="utf-8"?>
<!DOCTYPE html PUBLIC "-//WAPFORUM//DTD XHTML Mobile 1.0//EN" "http://www.wapforum.org/DTD/xhtml-mobile10.dtd">
<html xmlns="http://www.w3.org/1999/xhtml" xml:lang="zh-CN" lang="zh-CN">
<head>
    <title>{title}</title>
    <meta http-equiv="Cache-Control" content="no-cache, no-store, must-revalidate" />
    <meta http-equiv="Pragma" content="no-cache" />
    <meta http-equiv="Expires" content="0" />
    <link rel="shortcut icon" href="/favicon.ico?v=1" type="image/x-icon" />
    <link rel="apple-touch-icon" href="/speeddial-icon.png?v=1" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=2.0, user-scalable=yes" />
    <style type="text/css">
        body {{ background-color: whitesmoke; color: black; margin: 0; padding: 0; }}
        a {{ color: darkblue; text-decoration: none; }}
        a:visited {{ color: purple; }}
        a:hover {{ text-decoration: underline; }}
        .header {{ background-color: #3B5998; color: white; padding: 4px 6px; font-weight: bold; }}
        .content {{ padding: 6px; line-height: 1.6; text-align: justify; word-wrap: break-word; }}
        .content b {{ color: black; }}
        hr {{ border: 0; border-bottom: 1px solid silver; margin: 6px 0; }}
        select, input {{ border: 1px solid silver; background-color: white; margin-top: 4px; }}
        input[type="submit"] {{ background-color: gainsboro; padding: 2px 6px; }}
        .nav {{ background-color: gainsboro; padding: 6px; border-top: 1px solid silver; text-align: center; }}
        .item {{ padding: 1px 1px; display: block; }}
        .odd {{ background-color: lightgray; }}
        .even {{ background-color: white; }}
    </style>
</head>
<body>
    {body_content}
</body>
</html>"""
    headers = {
        "Cache-Control": "public, max-age=300",
        "Connection": "keep-alive",
        "Keep-Alive": "timeout=15, max=100",
    }
    return Response(
        content=xhtml_str.encode("utf-8"),
        media_type="text/html; charset=utf-8",
        headers=headers,
    )


@app.get("/")
async def get_index(request: Request, cat: str = "importnews", page: int = 1):
    if cat not in RSS_FEEDS:
        cat = "importnews"
    today_date = time.strftime("%Y%m%d")
    nav_links = []
    for cat_key, cat_info in RSS_FEEDS.items():
        if cat_key == cat:
            nav_links.append(f"<b>{cat_info['name']}</b>")
        else:
            nav_links.append(
                f'<a href="/?cat={cat_key}&amp;d={today_date}">{cat_info["name"]}</a>'
            )

    nav_html = ""
    for i in range(0, len(nav_links), 4):
        nav_html += f'{" | ".join(nav_links[i : i + 4])}<br/>\n'

    items = await get_news_items(cat)
    PAGE_SIZE = 20
    start_idx = (page - 1) * PAGE_SIZE
    end_idx = start_idx + PAGE_SIZE
    page_items = items[start_idx:end_idx]

    list_html = ""
    if len(items) == 0:
        list_html = "该频道暂无内容或源站拦截<br/>\n"
    else:
        for i, item in enumerate(page_items):
            real_index = start_idx + i
            safe_title = html.escape(item.get("title", "无标题"))
            link = item.get("link", "")
            item_hash = (
                hashlib.md5(link.encode("utf-8")).hexdigest()
                if link
                else str(real_index)
            )
            css_class = "odd" if i % 2 == 0 else "even"
            list_html += f'<div class="item {css_class}">[{real_index+1}]<a href="/article/{cat}/{item_hash}?d={today_date}">{safe_title}</a></div>\n'

    page_nav_html = ""
    if page > 1:
        page_nav_html += (
            f'<a href="/?cat={cat}&amp;page={page-1}&amp;d={today_date}">[上一页]</a> '
        )
    if end_idx < len(items):
        page_nav_html += (
            f'<a href="/?cat={cat}&amp;page={page+1}&amp;d={today_date}">[下一页]</a>'
        )
    if page_nav_html:
        page_nav_html += f"<br/>(第{page}页)"

    body_content = f"""
    <div class="header">WAP今日新闻</div>
    <div class="content">
        {nav_html}
        <hr/>
        {list_html}
        <hr/>
        {page_nav_html}
    </div>
    """
    return generate_xhtml_response("今日新闻", body_content)


async def fetch_article_content(item_link: str, cat: str):
    full_content = full_content_cache.get(item_link)
    if full_content or not item_link or cat == "tech":
        return full_content

    try:
        async with httpx.AsyncClient(timeout=8.0) as client:
            headers = {"User-Agent": feedparser.USER_AGENT}
            async with client.stream("GET", item_link, headers=headers) as resp:
                if resp.status_code == 200:
                    chunks = []
                    size = 0
                    async for chunk in resp.aiter_bytes():
                        chunks.append(chunk)
                        size += len(chunk)
                        if size > 2 * 1024 * 1024:
                            print(f"文章正文超过 2MB 限制: {item_link}")
                            break
                    downloaded = b"".join(chunks).decode("utf-8", errors="ignore")
                    extracted = None

                    if "chinanews.com" in item_link:
                        match = re.search(
                            r'<div class="left_zw">(.*?)<!--正文end-->',
                            downloaded,
                            re.DOTALL,
                        )
                        if not match:
                            match = re.search(
                                r'<div class="left_zw">(.*?)<div class="clear"></div>',
                                downloaded,
                                re.DOTALL,
                            )
                        if match:
                            raw_content = match.group(1)

                            def repl_img(m):
                                src = m.group(1).strip()
                                abs_src = urllib.parse.urljoin(item_link, src)
                                return f"\n[IMAGE:{abs_src}]\n"

                            text = re.sub(
                                r'<img\b(?:[^>"\']|"[^"]*"|\'[^\']*\')*?src="([^"]+)"(?:[^>"\']|"[^"]*"|\'[^\']*\')*>',
                                repl_img,
                                raw_content,
                                flags=re.IGNORECASE,
                            )
                            text = re.sub(r"<!--.*?-->", "", text, flags=re.DOTALL)
                            text = re.sub(
                                r"<script.*?>.*?</script>",
                                "",
                                text,
                                flags=re.DOTALL | re.IGNORECASE,
                            )
                            text = re.sub(
                                r"<style.*?>.*?</style>",
                                "",
                                text,
                                flags=re.DOTALL | re.IGNORECASE,
                            )
                            text = re.sub(
                                r'<(?:[^>"\']|"[^"]*"|\'[^\']*\')*>', " ", text
                            )
                            lines = [
                                line.strip()
                                for line in text.split("\n")
                                if line.strip()
                            ]
                            extracted = "\n".join(lines)

                    if not extracted:
                        extracted = await asyncio.to_thread(
                            trafilatura.extract, downloaded, favor_precision=True
                        )

                    if extracted and len(extracted) > 10:
                        full_content = extracted
                        full_content_cache[item_link] = full_content
                        col, _ = get_db_collections()
                        if col is not None:
                            try:
                                await col.update_one(
                                    {"link": item_link},
                                    {"$set": {"full_content": full_content}},
                                )
                            except Exception as ex:
                                print(f"全文更新失败: {ex}")
    except Exception as e:
        print(f"抓取全文失败 ({item_link}): {e}")

    return full_content


@app.get("/article/{cat}/{item_id}")
async def get_article(request: Request, cat: str, item_id: str):
    if cat not in RSS_FEEDS:
        raise HTTPException(status_code=404, detail="Category not found")
    items = await get_news_items(cat)

    item = None
    for it in items:
        link = it.get("link", "")
        if (hashlib.md5(link.encode("utf-8")).hexdigest() if link else "") == item_id:
            item = it
            break

    if not item and item_id.isdigit():
        idx = int(item_id)
        if 0 <= idx < len(items):
            item = items[idx]
    if not item:
        raise HTTPException(status_code=404, detail="News not found")

    item_link = item.get("link", "")
    safe_title = html.escape(item.get("title", "无标题"))

    full_content = await fetch_article_content(item_link, cat)
    if not full_content:
        full_content = getattr(item, "full_content", None) or item.get("full_content")

    display_content = (
        full_content
        if full_content
        else item.get("summary", item.get("description", "暂无详细内容"))
    )

    if full_content:
        noise_pattern = r".*?新闻精选：|相关阅读|推荐阅读|猜你喜欢|版权声明"
        match = re.search(noise_pattern, display_content)
        if match:
            display_content = display_content[: match.start()]

        cleaned_lines = []
        simple_title = re.sub(r"[^\w]", "", item.get("title", ""))
        for line in display_content.split("\n"):
            line_strip = html.unescape(line).strip().replace("\xa0", " ")
            if not line_strip:
                continue
            if line_strip in ("分享", "评论", "顶部", "参与互动", "版权声明"):
                break
            if len(cleaned_lines) < 3:
                simple_line = re.sub(r"[^\w]", "", line_strip)
                if simple_line == simple_title:
                    continue
            if (
                re.match(r"^[\-\d\s\:\u4e00-\u9fa5]+$", line_strip)
                and "年" in line_strip
                and "月" in line_strip
            ):
                continue
            cleaned_lines.append("　　" + html.escape(line_strip))

        safe_desc = "<br/><br/>".join(cleaned_lines)

        def img_replacer(match):
            img_url = html.unescape(match.group(1))
            safe_img_url = urllib.parse.quote(img_url)
            sign = sign_url(img_url)
            return f'<br/><div align="center"><img src="/proxy-image?url={safe_img_url}&amp;sign={sign}" alt="新闻图片" style="max-width: 98%; margin: 2px 0; border: 0;" /></div>'

        safe_desc = re.sub(r"　　\[IMAGE:(.*?)\]", img_replacer, safe_desc)
    else:
        clean_desc = (
            html.unescape(re.sub(r"<.*?>", "", display_content))
            .strip()
            .replace("\xa0", " ")
        )
        safe_desc = "　　" + html.escape(clean_desc)

    today_date = time.strftime("%Y%m%d")
    pub_parsed = item.get("published_parsed")
    pub_date = (
        time.strftime("%Y-%m-%d %H:%M", pub_parsed)
        if pub_parsed
        else item.get("published", "暂无时间信息")
    )

    body_content = f"""
    <div class="header">新闻详情</div>
    <div class="content">
        <b>{safe_title}</b><br/>
        {html.escape(pub_date)}
        <hr/>
        {safe_desc}<br/>
    </div>
    <div class="nav">
        <a href="/?cat={cat}&amp;d={today_date}">[返回{RSS_FEEDS[cat]["name"]}频道]</a>
    </div>
    """
    return generate_xhtml_response("新闻详情", body_content)


async def fetch_and_cache_image(url: str):
    if not url or not url.startswith(("http://", "https://")):
        return None
    if url in image_cache:
        return image_cache[url]

    img_col = get_image_collection()
    if img_col is not None:
        try:
            doc = await img_col.find_one({"_id": url})
            if doc and doc.get("img_data"):
                img_data = doc["img_data"]
                image_cache[url] = img_data
                return img_data
        except Exception as e:
            print(f"MongoDB 图片读取失败: {e}")

    try:
        async with httpx.AsyncClient(timeout=10.0, follow_redirects=True) as client:
            async with client.stream(
                "GET", url, headers={"User-Agent": feedparser.USER_AGENT}
            ) as resp:
                if resp.status_code == 200:
                    chunks = []
                    downloaded_size = 0
                    async for chunk in resp.aiter_bytes():
                        chunks.append(chunk)
                        downloaded_size += len(chunk)
                        if downloaded_size > 15 * 1024 * 1024:
                            print(f"图片超过 15MB 限制: {url}")
                            return None
                    img_data = b"".join(chunks)

                    if "Image" in globals():
                        try:

                            def process_image():
                                Image.MAX_IMAGE_PIXELS = 10000000
                                img = Image.open(io.BytesIO(img_data))
                                if img.mode in ("RGBA", "P", "LA"):
                                    bg = Image.new("RGB", img.size, (255, 255, 255))
                                    if img.mode == "RGBA":
                                        bg.paste(img, mask=img.split()[3])
                                    else:
                                        bg.paste(
                                            img.convert("RGBA"),
                                            mask=img.convert("RGBA").split()[3],
                                        )
                                    img = bg
                                elif img.mode != "RGB":
                                    img = img.convert("RGB")
                                max_width = 240
                                if img.width > max_width:
                                    ratio = max_width / img.width
                                    img = img.resize(
                                        (max_width, int(img.height * ratio)),
                                        getattr(Image, "Resampling", Image).LANCZOS,
                                    )
                                out = io.BytesIO()
                                img.save(out, format="JPEG", quality=65, optimize=True)
                                return out.getvalue()

                            img_data = await asyncio.to_thread(process_image)
                        except Exception as e:
                            print(f"图片压缩失败: {e}")

                    image_cache[url] = img_data
                    if img_col is not None:
                        try:
                            await img_col.update_one(
                                {"_id": url},
                                {"$set": {"img_data": img_data}},
                                upsert=True,
                            )
                        except Exception as e:
                            print(f"MongoDB 图片保存失败: {e}")
                    return img_data
    except Exception as e:
        print(f"代理图片失败 ({url}): {e}")
    return None


@app.get("/proxy-image")
async def proxy_image(url: str, sign: str = ""):
    if not sign or not verify_url(url, sign):
        raise HTTPException(
            status_code=403, detail="Invalid signature or unauthorized URL"
        )

    img_data = await fetch_and_cache_image(url)
    if img_data:
        return Response(
            content=img_data,
            media_type="image/jpeg",
            headers={"Cache-Control": "public, max-age=86400"},
        )
    return Response(status_code=404)


@app.get("/favicon.ico")
def favicon():
    if os.path.exists("favicon.ico"):
        return FileResponse(
            "favicon.ico",
            media_type="image/x-icon",
            headers={"Cache-Control": "max-age=0"},
        )
    return Response(status_code=404)


@app.get("/speeddial-icon.png")
def speeddial_icon():
    if os.path.exists("speeddial-icon.png"):
        return FileResponse(
            "speeddial-icon.png",
            media_type="image/png",
            headers={"Cache-Control": "public, max-age=60"},
        )
    return Response(status_code=404)
