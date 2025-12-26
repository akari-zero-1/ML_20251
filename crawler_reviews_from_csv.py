import csv
import random
import time
import re
from pathlib import Path
from typing import List, Dict, Any

BASE_DIR = Path(__file__).resolve().parent
CSV_PATTERN = "lazada_products_*.csv"
OUTPUT = BASE_DIR / f"crawler_reviews_{int(time.time())}.csv"

# Vietnamese first names and last names for realistic fake data
FIRST_NAMES = [
    "Minh", "Hương", "Anh", "Lan", "Nam", "Hoa", "Tuấn", "Linh", "Phương", "Hùng",
    "Mai", "Dũng", "Thảo", "Việt", "Trang", "Quang", "Ngọc", "Đức", "Hạnh", "Bảo",
    "Hà", "Khang", "Thu", "Long", "Nhung", "Cường", "Hiền", "Thanh", "Vy", "Tâm",
    "Châu", "Hoàng", "Yến", "Khánh", "My", "Giang", "Trung", "Thùy", "Toàn", "Thư",
    "Sơn", "Xuân", "Kim", "Dương", "Bình", "Phúc", "Thy", "Hải", "Nga", "Khoa",
    "Loan", "Tiến", "Như", "Đạt", "Tú", "Vân", "Cẩm", "An", "Hiếu", "Diệu",
    "Khanh", "Trí", "Thắng", "Nhật", "Tùng", "Trinh", "Quyên", "Thịnh", "Hân", "Kiên",
    "Oanh", "Trân", "Thế", "Huyền", "Phát", "Tuyết", "Thuận", "Hằng", "Thiện", "Thúy",
    "Hải", "Hồng", "Tân", "Vũ", "Lâm", "Diễm", "Trường", "Thành", "Phong", "Hương",
    "Hiệp", "Tiên", "Huy", "Tín", "Liên", "Quỳnh", "Lợi", "Thạch", "Hoài", "Thảo"
]

LAST_NAMES = [
    "Nguyễn", "Trần", "Lê", "Phạm", "Hoàng", "Huỳnh", "Phan", "Vũ", "Võ", "Đặng",
    "Bùi", "Đỗ", "Hồ", "Ngô", "Dương", "Lý", "Đinh", "Trương", "Đào", "Cao",
    "Mai", "Lưu", "Lâm", "Tô", "Chu", "Tạ", "Đoàn", "Hà", "Thái", "Tăng",
    "Thạch", "Văn", "Triệu", "La", "Trịnh", "Kiều", "Ông", "Thân", "Tống", "Bành",
    "Ninh", "Quách", "Mạc", "Tiêu", "Hứa", "Thang", "Từ", "Âu", "Phó", "Khương",
    "Vương", "Tôn", "Lục", "Cù", "Lương", "Lộc", "Nghiêm", "Diệp", "Đoàn", "Viên"
]

# Cap to avoid exploding rows; adjust if you really want full so_review count.
# Reduced to 10 to keep file size manageable for VS Code



def collect_products() -> List[Dict[str, Any]]:
    products: List[Dict[str, Any]] = []
    for sub in BASE_DIR.iterdir():
        if not sub.is_dir():
            continue
        for csv_path in sub.glob(CSV_PATTERN):
            with csv_path.open(newline="", encoding="utf-8-sig") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    products.append(row)
    return products


def make_name() -> str:
    return random.choice(FIRST_NAMES) + " " + random.choice(LAST_NAMES)


def extract_product_id(url: str) -> str:
    
    if not url:
        return ""
    match = re.search(r'-i(\d+)\.html', url)
    if match:
        return match.group(1)
    return url  # Return original if pattern not found


def generate_fake_reviews(products: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    rows = []
    for p in products:
        rating = p.get("rating") or p.get("Rating")
        review_count = p.get("so_review") or p.get("review_count")
        url = p.get("url_san_pham") or p.get("url")
        
        # Skip if no URL
        if not url:
            continue
            
        try:
            review_count = int(float(review_count)) if review_count else 0
        except Exception:
            review_count = 0
        try:
            target_rating = float(rating) if rating else None
        except Exception:
            target_rating = None

        # Generate exactly the same number of reviews as the product has
        n_reviews = review_count
        
        # Only generate reviews if we have valid rating and review count
        if n_reviews > 0 and target_rating is not None and target_rating > 0:
            # Generate ratings that average to target_rating
            ratings = generate_ratings_with_average(n_reviews, target_rating)
            product_id = extract_product_id(url)
            
            for i in range(n_reviews):
                rows.append({
                    "product_id": product_id,
                    "buyerName": make_name(),
                    "rating": round(ratings[i], 2),  # Keep 2 decimals for accuracy
                    "review_index": i + 1,
                })
    return rows


def generate_ratings_with_average(n: int, target_avg: float) -> List[float]:
    """Generate n ratings that average EXACTLY to target_avg."""
    if n == 0:
        return []
    
    # Clamp target between 1 and 5
    target_avg = max(1.0, min(5.0, target_avg))
    target_sum = target_avg * n
    
    # Generate n random integer ratings weighted toward target
    ratings = []
    for _ in range(n):
        if target_avg >= 4.0:
            # High rating: more 4s and 5s
            rating = random.choices([1, 2, 3, 4, 5], weights=[1, 2, 5, 15, 25])[0]
        elif target_avg >= 3.0:
            # Medium-high rating: balanced distribution
            rating = random.choices([1, 2, 3, 4, 5], weights=[3, 5, 15, 12, 8])[0]
        else:
            # Lower rating: more 1s, 2s, 3s
            rating = random.choices([1, 2, 3, 4, 5], weights=[15, 12, 10, 5, 2])[0]
        ratings.append(rating)
    
    # Adjust ratings to match exact target sum
    current_sum = sum(ratings)
    diff = target_sum - current_sum
    
    # Distribute the difference across ratings
    if diff != 0:
        # Sort indices by how much adjustment room they have
        indices = list(range(n))
        random.shuffle(indices)  # Randomize which ratings to adjust
        
        for idx in indices:
            if abs(diff) < 0.01:  # Close enough
                break
            
            # Calculate how much we can adjust this rating
            if diff > 0:  # Need to increase
                max_increase = 5.0 - ratings[idx]
                increase = min(diff, max_increase)
                ratings[idx] += increase
                diff -= increase
            else:  # Need to decrease
                max_decrease = ratings[idx] - 1.0
                decrease = min(abs(diff), max_decrease)
                ratings[idx] -= decrease
                diff += decrease
    
    # Convert to float and round to 2 decimals
    ratings = [round(float(r), 2) for r in ratings]
    
    # Shuffle so adjusted ratings aren't clustered
    random.shuffle(ratings)
    
    return ratings


def main():
    products = collect_products()
    if not products:
        print("Không tìm thấy dữ liệu sản phẩm.")
        return

    fake_rows = generate_fake_reviews(products)
    if not fake_rows:
        print("Không tạo được fake review.")
        return

    fieldnames = ["product_id", "buyerName", "rating", "review_index"]
    with OUTPUT.open("w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(fake_rows)

    print(f"Đã tạo {len(fake_rows)} reviews → {OUTPUT}")


if __name__ == "__main__":
    main()
