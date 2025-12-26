"""
Script để crawl fake reviews từ file CSV đã merge
"""
import csv
import random
import time
import re
from pathlib import Path
from typing import List, Dict, Any

# Vietnamese names
FIRST_NAMES = [
    "Minh", "Hương", "Anh", "Lan", "Nam", "Hoa", "Tuấn", "Linh", "Phương", "Hùng",
    "Mai", "Dũng", "Thảo", "Việt", "Trang", "Quang", "Ngọc", "Đức", "Hạnh", "Bảo",
    "Hà", "Khang", "Thu", "Long", "Nhung", "Cường", "Hiền", "Thanh", "Vy", "Tâm",
    "Châu", "Hoàng", "Yến", "Khánh", "My", "Giang", "Trung", "Thùy", "Toàn", "Thư",
    "Sơn", "Xuân", "Kim", "Dương", "Bình", "Phúc", "Thy", "Hải", "Nga", "Khoa",
]

LAST_NAMES = [
    "Nguyễn", "Trần", "Lê", "Phạm", "Hoàng", "Huỳnh", "Phan", "Vũ", "Võ", "Đặng",
    "Bùi", "Đỗ", "Hồ", "Ngô", "Dương", "Lý", "Đinh", "Trương", "Đào", "Cao",
]


def make_name() -> str:
    return f"{random.choice(LAST_NAMES)} {random.choice(FIRST_NAMES)}"


def extract_product_id(url: str) -> str:
    """Extract product ID from Lazada URL"""
    if not url:
        return ""
    match = re.search(r'-i(\d+)\.html', url)
    if match:
        return match.group(1)
    return url


def generate_ratings_with_average(n: int, target_avg: float) -> List[float]:
    """Generate n ratings that average to target_avg"""
    if n == 0:
        return []
    
    target_avg = max(1.0, min(5.0, target_avg))
    target_sum = target_avg * n
    
    # Generate ratings weighted toward target
    ratings = []
    for _ in range(n):
        if target_avg >= 4.0:
            rating = random.choices([1, 2, 3, 4, 5], weights=[1, 2, 5, 15, 25])[0]
        elif target_avg >= 3.0:
            rating = random.choices([1, 2, 3, 4, 5], weights=[3, 5, 15, 12, 8])[0]
        else:
            rating = random.choices([1, 2, 3, 4, 5], weights=[15, 12, 10, 5, 2])[0]
        ratings.append(rating)
    
    # Adjust to match exact target sum
    current_sum = sum(ratings)
    diff = target_sum - current_sum
    
    if diff != 0:
        indices = list(range(n))
        random.shuffle(indices)
        
        for idx in indices:
            if abs(diff) < 0.01:
                break
            
            if diff > 0:
                max_increase = 5.0 - ratings[idx]
                increase = min(diff, max_increase)
                ratings[idx] += increase
                diff -= increase
            else:
                max_decrease = ratings[idx] - 1.0
                decrease = min(abs(diff), max_decrease)
                ratings[idx] -= decrease
                diff += decrease
    
    ratings = [round(float(r), 2) for r in ratings]
    random.shuffle(ratings)
    
    return ratings


def crawl_reviews_from_csv(csv_file: str, output_file: str = None):
    """
    Crawl fake reviews từ file CSV đã merge
    
    Args:
        csv_file: Đường dẫn tới file CSV merged
        output_file: Tên file output (mặc định: reviews_TIMESTAMP.csv)
    """
    
    print("=" * 60)
    print("CRAWL REVIEWS FROM MERGED CSV")
    print("=" * 60)
    
    csv_path = Path(csv_file)
    if not csv_path.exists():
        print(f"[ERROR] File không tồn tại: {csv_file}")
        return None
    
    print(f"\n[INFO] Đọc file: {csv_path.name}")
    
    # Đọc products từ CSV
    products = []
    try:
        with csv_path.open(newline="", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            for row in reader:
                products.append(row)
    except Exception as e:
        print(f"[ERROR] Lỗi khi đọc file: {e}")
        return None
    
    print(f"[INFO] Đã đọc {len(products)} sản phẩm")
    
    # Generate fake reviews
    reviews = []
    products_with_reviews = 0
    total_reviews = 0
    
    for i, p in enumerate(products, 1):
        # Tìm các trường có thể chứa rating và review count
        rating = (p.get("rating") or p.get("Rating") or 
                 p.get("rating_score") or p.get("ratingScore"))
        review_count = (p.get("so_review") or p.get("review_count") or 
                       p.get("review") or p.get("reviews"))
        url = (p.get("url_san_pham") or p.get("url") or 
              p.get("productUrl") or p.get("item_url"))
        product_name = (p.get("ten_san_pham") or p.get("name") or 
                       p.get("product_name") or "Unknown")
        
        if not url:
            continue
        
        try:
            review_count = int(float(review_count)) if review_count else 0
        except:
            review_count = 0
        
        try:
            target_rating = float(rating) if rating else None
        except:
            target_rating = None
        
        if review_count > 0 and target_rating is not None and target_rating > 0:
            products_with_reviews += 1
            total_reviews += review_count
            
            # Generate ratings
            ratings = generate_ratings_with_average(review_count, target_rating)
            product_id = extract_product_id(url)
            
            for j, r in enumerate(ratings, 1):
                reviews.append({
                    "product_id": product_id,
                    "user_id": random.randint(1000, 99999),
                    "buyerName": make_name(),
                    "rating": r,
                    "review_index": j,
                })
            
            # Progress indicator
            if i % 100 == 0:
                print(f"[PROGRESS] Đã xử lý {i}/{len(products)} sản phẩm...")
    
    print(f"\n[INFO] Đã tạo {len(reviews)} reviews từ {products_with_reviews} sản phẩm")
    
    if not reviews:
        print("[WARN] Không có review nào được tạo!")
        return None
    
    # Tạo tên file output
    if not output_file:
        timestamp = int(time.time())
        output_file = f"reviews_{timestamp}.csv"
    
    output_path = Path(output_file)
    
    # Ghi file
    fieldnames = ["product_id", "user_id", "buyerName", "rating", "review_index"]
    with output_path.open("w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(reviews)
    
    print(f"\n[SUCCESS] Đã lưu {len(reviews)} reviews vào: {output_path.name}")
    
    # Thống kê
    print("\n[STATS] Thống kê reviews:")
    print(f"  - Tổng reviews: {len(reviews)}")
    print(f"  - Số sản phẩm có review: {products_with_reviews}")
    print(f"  - TB reviews/sản phẩm: {len(reviews)/products_with_reviews:.1f}")
    
    # Rating distribution
    ratings_dist = {}
    for r in reviews:
        rating_int = int(r['rating'])
        ratings_dist[rating_int] = ratings_dist.get(rating_int, 0) + 1
    
    print("\n  Phân bố rating:")
    for rating in sorted(ratings_dist.keys()):
        count = ratings_dist[rating]
        pct = count / len(reviews) * 100
        print(f"    {rating} sao: {count} ({pct:.1f}%)")
    
    return output_path


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1:
        # Run with argument
        csv_file = sys.argv[1]
        output_file = sys.argv[2] if len(sys.argv) > 2 else None
        crawl_reviews_from_csv(csv_file, output_file)
    else:
        # Interactive mode
        print("CRAWL REVIEWS FROM MERGED CSV")
        print("-" * 60)
        
        # Tìm file merged
        merged_files = list(Path(".").glob("merged_*.csv"))
        
        if not merged_files:
            print("\n[ERROR] Không tìm thấy file merged_*.csv!")
            print("Vui lòng chạy merge_csv.py trước.")
        else:
            print("\nDanh sách file merged:")
            for i, f in enumerate(merged_files, 1):
                print(f"{i}. {f.name}")
            
            if len(merged_files) == 1:
                choice = "1"
            else:
                choice = input(f"\nChọn file (1-{len(merged_files)}): ").strip()
            
            try:
                idx = int(choice) - 1
                if 0 <= idx < len(merged_files):
                    csv_file = str(merged_files[idx])
                    crawl_reviews_from_csv(csv_file)
                else:
                    print("[ERROR] Lựa chọn không hợp lệ!")
            except ValueError:
                print("[ERROR] Vui lòng nhập số!")
