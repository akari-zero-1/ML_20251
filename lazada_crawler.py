from playwright.sync_api import sync_playwright
import time
import csv
import os
import re


def crawl_lazada(keyword="shirts", start_page=1, end_page=10):
    results = []
    category_value = keyword.strip() or keyword

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                       "AppleWebKit/537.36 (KHTML, like Gecko) "
                       "Chrome/120.0.0.0 Safari/537.36"
        )
        page = context.new_page()

        # Hit homepage once to obtain baseline cookies before calling the JSON endpoint directly.
        page.goto("https://www.lazada.vn/", wait_until="networkidle", timeout=60000)

        headers = {
            "accept": "application/json, text/plain, */*",
            "x-requested-with": "XMLHttpRequest",
            "referer": "https://www.lazada.vn/",
        }

        for i in range(start_page, end_page + 1):
            print(f"👉 Crawling page {i}")

            response = context.request.get(
                "https://www.lazada.vn/catalog/",
                params={
                    "ajax": "true",
                    "_keyori": "ss",
                    "from": "input",
                    "page": i,
                    "q": keyword,
                },
                headers=headers,
                timeout=60_000,
            )

            status = response.status
            content_type = (response.headers.get("content-type") or "").lower()

            if status != 200:
                print(f"⚠️ Request failed with status {status} for page {i}")
                time.sleep(1)
                continue

            if "application/json" not in content_type:
                print(f"⚠️ Unexpected content-type {content_type or 'unknown'} for page {i} (status {status})")
                time.sleep(1)
                continue

            try:
                data = response.json()
            except Exception as exc:
                print(f"⚠️ Failed to parse catalog response for page {i}: {exc}")
                time.sleep(1)
                continue

            items = data.get("mods", {}).get("listItems", [])

            for p_item in items:
                raw_url = (
                    p_item.get("productUrl")
                    or p_item.get("itemUrl")
                    or p_item.get("itemUrlWrap")
                    or p_item.get("itemUrlPC")
                )

                if raw_url:
                    if raw_url.startswith("//"):
                        raw_url = "https:" + raw_url
                    elif raw_url.startswith("/"):
                        raw_url = "https://www.lazada.vn" + raw_url
                else:
                    # Expose missing URL so we know to adjust mapping if Lazada changes payload keys again.
                    nid = p_item.get("nid") or p_item.get("itemId")
                    print(f"⚠️ Missing product URL for item {nid or '[unknown id]'} on page {i}")

                results.append({
                    "ten_san_pham": p_item.get("name"),
                    "gia_sale": p_item.get("price"),
                    "gia_goc": p_item.get("originalPrice"),
                    "rating": p_item.get("ratingScore"),
                    "so_review": p_item.get("review"),
                    "link_anh": p_item.get("image"),
                    "shop": p_item.get("sellerName"),
                    "category": category_value,
                    "url_san_pham": raw_url,
                })

            time.sleep(1)

        browser.close()

    return results


def _slugify_keyword(keyword: str) -> str:
    slug = re.sub(r"[^a-z0-9_-]+", "_", keyword.strip().lower())
    slug = slug.strip("_")
    return slug or "keyword"


def save_to_csv(data, keyword, filename=None):
    if not data:
        print("⚠️ Không có dữ liệu để lưu.")
        return None

    keyword_slug = _slugify_keyword(keyword)
    folder = keyword_slug
    os.makedirs(folder, exist_ok=True)

    if not filename:
        timestamp = time.strftime("%Y%m%d_%H%M%S")
        filename = f"lazada_products_{keyword_slug}_{timestamp}.csv"

    filepath = os.path.join(folder, filename)

    with open(filepath, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=data[0].keys())
        writer.writeheader()
        writer.writerows(data)

    return filepath


if __name__ == "__main__":
    keyword = input("Nhập từ khóa (mặc định 'shirts'): ").strip() or "shirts"

    try:
        start_page = int(input("Nhập trang bắt đầu: ").strip() or "1")
        end_page = int(input("Nhập trang kết thúc: ").strip() or str(start_page))
    except ValueError:
        print("⚠️ Trang phải là số. Dùng mặc định 1 -> 10.")
        start_page, end_page = 1, 10

    if end_page < start_page:
        print("⚠️ Trang kết thúc phải >= trang bắt đầu. Đổi về cùng giá trị.")
        end_page = start_page

    data = crawl_lazada(keyword, start_page, end_page)
    print(f"\n✅ Tổng sản phẩm lấy được: {len(data)}")
    saved = save_to_csv(data, keyword)
    if saved:
        print(f"📁 Đã lưu file {saved}")
