import argparse
import asyncio
import json
import random
import re
import time
from pathlib import Path
from typing import Any, Dict, List
from urllib.parse import urlparse

from playwright.async_api import TimeoutError as PlaywrightTimeoutError
from playwright.async_api import async_playwright


EXTRACT_COMMENTS_JS = r"""
() => {
  const clean = (value) => (value || "").replace(/\s+/g, " ").trim();

  const pickText = (root, selectors) => {
    for (const selector of selectors) {
      const el = root.querySelector(selector);
      const text = clean(el && el.innerText);
      if (text) return text;
    }
    return "";
  };

  const pickOwnText = (root) => {
    const clone = root.cloneNode(true);
    clone.querySelectorAll("button, svg, img, video, canvas, [aria-hidden='true']").forEach((el) => el.remove());
    return clean(clone.innerText);
  };

  const nodeKey = (node) => {
    if (!node.dataset.scrapeKey) {
      node.dataset.scrapeKey = Math.random().toString(36).slice(2);
    }
    return node.dataset.scrapeKey;
  };

  const itemSelectors = [
    "[data-e2e='comment-item']",
    "[data-e2e*='comment-item']",
    "div[class*='DivCommentItemContainer']",
    "div[class*='CommentItem']",
    "[data-e2e*='review-item']",
    "[data-e2e*='rating-review']",
    "[class*='reviewItem']",
    "[class*='ReviewItem']",
    "[class*='commentItem']"
  ];

  const nodes = [];
  const seen = new Set();
  for (const selector of itemSelectors) {
    document.querySelectorAll(selector).forEach((node) => {
      const text = pickOwnText(node);
      if (text.length < 2) return;
      const key = nodeKey(node);
      if (!seen.has(key)) {
        seen.add(key);
        nodes.push(node);
      }
    });
  }

  // Fallback for pages that do not expose stable data attributes.
  if (nodes.length === 0) {
    document.querySelectorAll("article, li, div").forEach((node) => {
      const text = pickOwnText(node);
      if (text.length < 10 || text.length > 900) return;
      if (!/(like|likes|reply|回复|赞|评论|review|rating|评价|条评价|天前|小时前|分钟前|\d{4}-\d{1,2}-\d{1,2})/i.test(text)) return;
      const rect = node.getBoundingClientRect();
      if (rect.width < 120 || rect.height < 24) return;
      const key = nodeKey(node);
      if (!seen.has(key)) {
        seen.add(key);
        nodes.push(node);
      }
    });
  }

  const usernameSelectors = [
    "[data-e2e*='comment-username']",
    "[data-e2e*='review-user']",
    "[data-e2e*='user-name']",
    "a[href^='/@']",
    "a[href*='/@']",
    "[class*='UserName']",
    "[class*='Username']",
    "[class*='userName']",
    "[class*='nickname']",
    "[class*='Nickname']"
  ];

  const commentSelectors = [
    "[data-e2e*='comment-level']",
    "[data-e2e*='comment-text']",
    "[data-e2e*='review-content']",
    "[data-e2e*='review-text']",
    "[class*='CommentText']",
    "[class*='commentText']",
    "[class*='ReviewText']",
    "[class*='reviewText']",
    "p"
  ];

  const timeSelectors = [
    "[data-e2e*='comment-time']",
    "[data-e2e*='review-time']",
    "time",
    "[datetime]",
    "[class*='Time']",
    "[class*='time']",
    "[class*='Date']",
    "[class*='date']"
  ];

  const likeSelectors = [
    "[data-e2e*='comment-like-count']",
    "[data-e2e*='like-count']",
    "[data-e2e*='review-like']",
    "[aria-label*='like' i]",
    "[aria-label*='赞']",
    "[class*='Like']",
    "[class*='like']"
  ];

  const parseLikeCount = (node) => {
    const direct = pickText(node, likeSelectors);
    const aria = Array.from(node.querySelectorAll("[aria-label]"))
      .map((el) => el.getAttribute("aria-label"))
      .map(clean)
      .find((text) => /(like|likes|赞)/i.test(text));
    const raw = direct || aria || "";
    const match = raw.match(/(\d+(?:[.,]\d+)?\s*[kKmMwW万]?)/);
    return match ? clean(match[1]) : "";
  };

  const parseFallbackCommentText = (node, username, publishTime, likeCount) => {
    const lines = pickOwnText(node).split(/\n| {2,}/).map(clean).filter(Boolean);
    const blacklist = new Set([
      clean(username).toLowerCase(),
      clean(publishTime).toLowerCase(),
      clean(likeCount).toLowerCase(),
      "reply",
      "回复",
      "like",
      "likes",
      "赞"
    ]);
    const candidate = lines
      .filter((line) => !blacklist.has(line.toLowerCase()))
      .filter((line) => !/^(reply|回复|like|likes|赞)$/i.test(line))
      .sort((a, b) => b.length - a.length)[0];
    return candidate || "";
  };

  return nodes.map((node) => {
    const username = pickText(node, usernameSelectors);
    const publishTime = pickText(node, timeSelectors);
    const likeCount = parseLikeCount(node);
    let commentText = pickText(node, commentSelectors);
    if (!commentText || clean(commentText).toLowerCase() === clean(username).toLowerCase()) {
      commentText = parseFallbackCommentText(node, username, publishTime, likeCount);
    }

    return {
      username: username || "",
      comment_text: clean(commentText),
      publish_time: publishTime || "",
      like_count: likeCount || ""
    };
  }).filter((item) => item.comment_text.length > 0);
}
"""


SCROLL_COMMENTS_JS = r"""
() => {
  const candidates = [
    "[data-e2e='comment-list']",
    "[data-e2e*='comment-list']",
    "[class*='CommentList']",
    "[class*='comment-list']",
    "[data-e2e*='review-list']",
    "[class*='ReviewList']",
    "[class*='review-list']",
    "main"
  ];

  const scrollables = [];
  for (const selector of candidates) {
    document.querySelectorAll(selector).forEach((el) => {
      if (el.scrollHeight > el.clientHeight + 60) scrollables.push(el);
      let parent = el.parentElement;
      while (parent && parent !== document.body) {
        if (parent.scrollHeight > parent.clientHeight + 60) {
          scrollables.push(parent);
          break;
        }
        parent = parent.parentElement;
      }
    });
  }

  document.querySelectorAll("div, section, aside").forEach((el) => {
    const style = window.getComputedStyle(el);
    if (!/(auto|scroll)/.test(style.overflowY)) return;
    if (el.scrollHeight > el.clientHeight + 200) scrollables.push(el);
  });

  const unique = Array.from(new Set(scrollables));
  const target = unique.sort((a, b) => {
    const aArea = a.clientWidth * a.clientHeight;
    const bArea = b.clientWidth * b.clientHeight;
    return b.scrollHeight - a.scrollHeight || bArea - aArea;
  })[0] || document.scrollingElement || document.documentElement;

  const before = target.scrollTop;
  const distance = Math.max(420, Math.floor(target.clientHeight * (0.65 + Math.random() * 0.35)));
  target.scrollBy({ top: distance, behavior: "smooth" });
  window.scrollBy({ top: Math.floor(distance * 0.25), behavior: "smooth" });

  return {
    targetTag: target.tagName,
    before,
    after: target.scrollTop,
    scrollHeight: target.scrollHeight,
    clientHeight: target.clientHeight
  };
}
"""


TIKTOK_COMMENT_API_FETCH_JS = r"""
async ({ videoId, cursor, count }) => {
  const endpoint = `/api/comment/list/?aid=1988&app_name=tiktok_web&count=${count}&cursor=${cursor}&aweme_id=${videoId}`;
  const response = await fetch(endpoint, {
    credentials: "include",
    headers: {
      "accept": "application/json, text/plain, */*"
    }
  });
  const text = await response.text();
  let payload = null;
  try {
    payload = JSON.parse(text);
  } catch (error) {
    payload = { parse_error: String(error), raw_sample: text.slice(0, 500) };
  }
  return {
    ok: response.ok,
    status: response.status,
    endpoint,
    payload
  };
}
"""


YOUTUBE_EXTRACT_COMMENTS_JS = r"""
() => {
  const clean = (value) => (value || "").replace(/\s+/g, " ").trim();

  const pickText = (root, selectors) => {
    for (const selector of selectors) {
      const el = root.querySelector(selector);
      const text = clean(el && el.innerText);
      if (text) return text;
    }
    return "";
  };

  const parseLikeCount = (text) => {
    const cleaned = clean(text);
    if (!cleaned) return "0";
    const match = cleaned.match(/(\d+(?:[.,]\d+)?\s*[kKmMwW万]?)/);
    return match ? clean(match[1]) : "0";
  };

  return Array.from(document.querySelectorAll("ytd-comment-thread-renderer"))
    .map((node) => {
      const username = pickText(node, [
        "#author-text span",
        "#author-text",
        "a#author-text",
        "yt-formatted-string#author-text"
      ]);

      const commentText = pickText(node, [
        "#content-text",
        "yt-attributed-string#content-text",
        "yt-formatted-string#content-text"
      ]);

      const publishTime = pickText(node, [
        "#published-time-text a",
        "#published-time-text",
        "yt-formatted-string.published-time-text"
      ]);

      const likeText = pickText(node, [
        "#vote-count-middle",
        "#vote-count-left",
        "span#vote-count-middle"
      ]);

      return {
        username,
        comment_text: commentText,
        publish_time: publishTime,
        like_count: parseLikeCount(likeText)
      };
    })
    .filter((item) => item.comment_text.length > 0);
}
"""


YOUTUBE_SCROLL_COMMENTS_JS = r"""
() => {
  const distance = Math.max(700, Math.floor(window.innerHeight * (0.75 + Math.random() * 0.35)));
  window.scrollBy({ top: distance, behavior: "smooth" });
  return {
    scrollY: window.scrollY,
    scrollHeight: document.documentElement.scrollHeight,
    innerHeight: window.innerHeight
  };
}
"""


YOUTUBE_UNAVAILABLE_MARKERS = [
    "This video isn't available anymore",
    "Video unavailable",
    "This video is unavailable",
    "This video has been removed",
    "Comments are turned off",
    "此视频无法观看",
    "该视频无法观看",
    "评论已关闭",
]


def normalize_comment(item: Dict[str, Any]) -> Dict[str, str]:
    return {
        "username": str(item.get("username", "")).strip(),
        "comment_text": str(item.get("comment_text", "")).strip(),
        "publish_time": str(item.get("publish_time", "")).strip(),
        "like_count": str(item.get("like_count", "")).strip(),
    }


def dedupe_key(item: Dict[str, str]) -> str:
    joined = "|".join(
        [
            item.get("username", ""),
            item.get("comment_text", ""),
            item.get("publish_time", ""),
        ]
    )
    return re.sub(r"\s+", " ", joined).strip().lower()


async def human_delay(min_seconds: float = 0.8, max_seconds: float = 2.2) -> None:
    await asyncio.sleep(random.uniform(min_seconds, max_seconds))


def is_youtube_url(url: str) -> bool:
    hostname = (urlparse(url.strip()).hostname or "").lower()
    return hostname == "youtube.com" or hostname.endswith(".youtube.com") or hostname == "youtu.be"


def parse_tiktok_video_id(url: str) -> str:
    """从 TikTok 视频 URL 中提取 aweme/video ID，用于 Web 评论接口分页抓取。"""
    parsed = urlparse(url.strip())
    path = parsed.path or ""
    match = re.search(r"/video/(\d+)", path)
    if match:
        return match.group(1)

    # 兼容少量短链跳转后的纯数字片段。
    match = re.search(r"(\d{12,})", url)
    return match.group(1) if match else ""


def format_tiktok_publish_time(value: Any) -> str:
    """把 TikTok create_time 转为易读时间；异常时保留 recently。"""
    try:
        timestamp = int(value)
        if timestamp <= 0:
            return "recently"
        return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(timestamp))
    except Exception:
        return "recently"


def normalize_tiktok_api_comment(item: Dict[str, Any]) -> Dict[str, str]:
    """把 TikTok Web 评论接口字段规整成后续 AI 诊断统一消费的结构。"""
    user = item.get("user") if isinstance(item.get("user"), dict) else {}
    unique_id = str(user.get("unique_id") or "").strip()
    nickname = str(user.get("nickname") or "").strip()
    username = f"@{unique_id}" if unique_id else nickname
    text = str(item.get("text") or item.get("share_info", {}).get("desc") or "").strip()
    return {
        "username": username or "@anonymous",
        "comment_text": text,
        "publish_time": format_tiktok_publish_time(item.get("create_time")),
        "like_count": str(item.get("digg_count") or 0),
    }


async def collect_tiktok_api_comments(page: Any, url: str, limit: int) -> List[Dict[str, str]]:
    """
    TikTok 桌面页经常只暴露评论总数，不渲染评论 DOM。
    页面加载后改走同源 Web 评论接口，可在不伪造数据的前提下分页获取真实评论。
    """
    video_id = parse_tiktok_video_id(url)
    if not video_id:
        print("TikTok API fallback skipped: video ID not found in URL.")
        return []

    comments_by_key: Dict[str, Dict[str, str]] = {}
    cursor = 0
    has_more = True
    max_rounds = max(2, min(12, (limit + 49) // 50 + 2))

    for round_index in range(max_rounds):
        if not has_more or len(comments_by_key) >= limit:
            break

        count = min(50, max(1, limit - len(comments_by_key)))
        try:
            result = await page.evaluate(
                TIKTOK_COMMENT_API_FETCH_JS,
                {"videoId": video_id, "cursor": cursor, "count": count},
            )
        except Exception as exc:
            print(f"TikTok API fallback request failed: {exc}")
            break

        status = result.get("status")
        payload = result.get("payload") if isinstance(result.get("payload"), dict) else {}
        raw_comments = payload.get("comments") if isinstance(payload.get("comments"), list) else []
        print(f"TikTok API round {round_index + 1}: status {status}, received {len(raw_comments)} comments")

        for raw_item in raw_comments:
            if not isinstance(raw_item, dict):
                continue
            item = normalize_tiktok_api_comment(raw_item)
            if not item["comment_text"]:
                continue
            key = dedupe_key(item)
            if key:
                comments_by_key[key] = item

        next_cursor = payload.get("cursor")
        try:
            cursor = int(next_cursor)
        except Exception:
            cursor += len(raw_comments)
        has_more = bool(payload.get("has_more")) and len(raw_comments) > 0

        await human_delay(0.6, 1.4)

    return list(comments_by_key.values())[:limit]


async def collect_comments(
    url: str,
    limit: int,
    headless: bool,
    timeout_ms: int,
) -> List[Dict[str, str]]:
    comments_by_key: Dict[str, Dict[str, str]] = {}
    stale_rounds = 0
    max_rounds = max(45, limit // 2)

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=headless,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--disable-dev-shm-usage",
                "--no-sandbox",
            ],
        )
        context = await browser.new_context(
            viewport={"width": 1440, "height": 950},
            locale="en-US",
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
        )
        page = await context.new_page()

        try:
            print(f"Opening: {url}")
            await page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
            try:
                await page.wait_for_load_state("networkidle", timeout=15_000)
            except PlaywrightTimeoutError:
                print("Network did not become idle; continuing with visible content.")

            await human_delay(2.0, 4.5)

            api_comments = await collect_tiktok_api_comments(page, url, limit)
            if api_comments:
                print(f"TikTok API fallback collected {len(api_comments)} real comments.")
                return api_comments

            for round_index in range(max_rounds):
                batch = await page.evaluate(EXTRACT_COMMENTS_JS)
                before_count = len(comments_by_key)

                for raw_item in batch:
                    item = normalize_comment(raw_item)
                    if not item["comment_text"]:
                        continue
                    key = dedupe_key(item)
                    if key:
                        comments_by_key[key] = item

                current_count = len(comments_by_key)
                print(f"Round {round_index + 1}: collected {current_count}/{limit}")

                if current_count >= limit:
                    break

                stale_rounds = stale_rounds + 1 if current_count == before_count else 0
                if stale_rounds >= 10:
                    print("No new comments after multiple scrolls; stopping early.")
                    break

                try:
                    await page.evaluate(SCROLL_COMMENTS_JS)
                except Exception as exc:
                    print(f"Scroll failed once, falling back to mouse wheel: {exc}")
                    await page.mouse.wheel(0, random.randint(520, 980))

                await human_delay(1.2, 3.2)

        except PlaywrightTimeoutError as exc:
            raise RuntimeError(f"Page load timed out: {exc}") from exc
        except Exception as exc:
            raise RuntimeError(f"Failed to collect comments: {exc}") from exc
        finally:
            await context.close()
            await browser.close()

    return list(comments_by_key.values())[:limit]


async def accept_youtube_consent(page: Any) -> None:
    """尽量处理 YouTube/Google 的同意弹窗；没有弹窗时静默跳过。"""
    candidate_names = [
        "Accept all",
        "I agree",
        "Agree",
        "同意",
        "全部接受",
        "接受全部",
    ]
    for name in candidate_names:
        try:
            button = page.get_by_role("button", name=re.compile(name, re.IGNORECASE))
            if await button.count() > 0:
                await button.first.click(timeout=2_000)
                await human_delay(0.8, 1.4)
                return
        except Exception:
            continue


async def collect_youtube_comments(
    url: str,
    limit: int,
    headless: bool,
    timeout_ms: int,
) -> List[Dict[str, str]]:
    """轻量抓取 YouTube 视频评论，用于本地联调 TikTok 不可用时的测试源。"""
    comments_by_key: Dict[str, Dict[str, str]] = {}
    stale_rounds = 0
    max_rounds = min(18, max(8, limit // 5))

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=headless,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--disable-dev-shm-usage",
                "--no-sandbox",
            ],
        )
        context = await browser.new_context(
            viewport={"width": 1440, "height": 950},
            locale="en-US",
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
        )
        page = await context.new_page()

        try:
            print(f"Opening YouTube: {url}")
            await page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
            await accept_youtube_consent(page)

            try:
                await page.wait_for_selector("ytd-watch-flexy, ytd-app", timeout=20_000)
            except PlaywrightTimeoutError:
                print("YouTube shell did not fully appear; continuing with visible content.")

            await human_delay(2.0, 4.0)

            try:
                body_text = await page.locator("body").inner_text(timeout=5_000)
                lowered_body = body_text.lower()
                if any(marker.lower() in lowered_body for marker in YOUTUBE_UNAVAILABLE_MARKERS):
                    print("YouTube video appears unavailable or comments are disabled; returning no comments.")
                    return []
            except Exception:
                pass

            # 先滚到评论区附近，YouTube 评论通常在首屏下方懒加载。
            await page.mouse.wheel(0, 1_200)
            await human_delay(1.2, 2.4)

            for round_index in range(max_rounds):
                batch = await page.evaluate(YOUTUBE_EXTRACT_COMMENTS_JS)
                before_count = len(comments_by_key)

                for raw_item in batch:
                    item = normalize_comment(raw_item)
                    if not item["comment_text"]:
                        continue
                    key = dedupe_key(item)
                    if key:
                        comments_by_key[key] = item

                current_count = len(comments_by_key)
                print(f"YouTube round {round_index + 1}: collected {current_count}/{limit}")

                if current_count >= limit:
                    break

                stale_rounds = stale_rounds + 1 if current_count == before_count else 0
                if stale_rounds >= 5:
                    print("No new YouTube comments after multiple scrolls; stopping early.")
                    break

                try:
                    await page.evaluate(YOUTUBE_SCROLL_COMMENTS_JS)
                except Exception as exc:
                    print(f"YouTube scroll failed once, falling back to mouse wheel: {exc}")
                    await page.mouse.wheel(0, random.randint(720, 1_220))

                await human_delay(1.2, 3.0)

        except PlaywrightTimeoutError as exc:
            raise RuntimeError(f"YouTube page load timed out: {exc}") from exc
        except Exception as exc:
            raise RuntimeError(f"Failed to collect YouTube comments: {exc}") from exc
        finally:
            await context.close()
            await browser.close()

    return list(comments_by_key.values())[:limit]


async def main() -> None:
    parser = argparse.ArgumentParser(
        description="Scrape TikTok video comments or TikTok Shop product reviews with Playwright."
    )
    parser.add_argument("url", help="TikTok video URL or TikTok Shop product/review page URL.")
    parser.add_argument("--limit", type=int, default=100, help="Number of comments/reviews to collect.")
    parser.add_argument("--output", default="raw_comments.json", help="Output JSON file path.")
    parser.add_argument("--timeout-ms", type=int, default=60_000, help="Page load timeout in milliseconds.")
    parser.add_argument(
        "--headful",
        action="store_true",
        help="Run with a visible browser window for debugging. Default is headless.",
    )
    args = parser.parse_args()

    if args.limit <= 0:
        raise ValueError("--limit must be greater than 0")

    if is_youtube_url(args.url):
        comments = await collect_youtube_comments(
            url=args.url,
            limit=args.limit,
            headless=not args.headful,
            timeout_ms=args.timeout_ms,
        )
    else:
        comments = await collect_comments(
            url=args.url,
            limit=args.limit,
            headless=not args.headful,
            timeout_ms=args.timeout_ms,
        )

    output_path = Path(args.output)
    output_path.write_text(
        json.dumps(comments, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    print(f"Saved {len(comments)} comments to {output_path.resolve()}")


if __name__ == "__main__":
    asyncio.run(main())
