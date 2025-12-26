"""
Script để merge các file CSV và loại bỏ sản phẩm trùng lặp dựa vào ID
"""
import pandas as pd
import os
import glob
from datetime import datetime


def merge_csv_files(pattern="**/*.csv", output_file=None, id_column="item_id"):
    """
    Merge tất cả file CSV và loại bỏ trùng lặp dựa vào ID sản phẩm
    
    Args:
        pattern: Pattern để tìm file CSV (mặc định: **/*.csv tìm tất cả file trong subfolders)
        output_file: Tên file output (mặc định: merged_products_TIMESTAMP.csv)
        id_column: Tên cột ID để check trùng lặp (mặc định: item_id)
    """
    
    print(f"[INFO] Đang tìm file CSV với pattern: {pattern}")
    
    # Tìm tất cả file CSV
    csv_files = glob.glob(pattern, recursive=True)
    csv_files = [f for f in csv_files if not f.startswith("merged_")]  # Bỏ qua file merged cũ
    
    if not csv_files:
        print("[WARN] Không tìm thấy file CSV nào!")
        return None
    
    print(f"[INFO] Tìm thấy {len(csv_files)} file CSV")
    
    # Đọc và merge tất cả file
    all_data = []
    total_rows = 0
    
    for csv_file in csv_files:
        try:
            df = pd.read_csv(csv_file, encoding='utf-8-sig')
            rows = len(df)
            total_rows += rows
            all_data.append(df)
            print(f"[INFO] Đọc {rows} dòng từ: {os.path.basename(csv_file)}")
        except Exception as e:
            print(f"[WARN] Lỗi khi đọc {csv_file}: {e}")
    
    if not all_data:
        print("[WARN] Không có dữ liệu để merge!")
        return None
    
    # Merge tất cả DataFrame
    merged_df = pd.concat(all_data, ignore_index=True)
    print(f"\n[INFO] Tổng số dòng trước khi loại trùng: {total_rows}")
    
    # Loại bỏ trùng lặp dựa vào ID
    # Kiểm tra cột ID có tồn tại không
    if id_column not in merged_df.columns:
        # Thử tìm cột ID khác
        possible_id_columns = ['item_id', 'id', 'product_id', 'url_san_pham']
        id_column = None
        for col in possible_id_columns:
            if col in merged_df.columns:
                id_column = col
                break
        
        if not id_column:
            print(f"[WARN] Không tìm thấy cột ID. Các cột có sẵn: {list(merged_df.columns)}")
            print("[INFO] Sẽ loại trùng dựa vào tất cả cột")
            merged_df = merged_df.drop_duplicates()
        else:
            print(f"[INFO] Sử dụng cột '{id_column}' để loại trùng")
            merged_df = merged_df.drop_duplicates(subset=[id_column], keep='first')
    else:
        print(f"[INFO] Loại bỏ trùng lặp dựa vào cột '{id_column}'")
        merged_df = merged_df.drop_duplicates(subset=[id_column], keep='first')
    
    unique_rows = len(merged_df)
    duplicates = total_rows - unique_rows
    
    print(f"[INFO] Số dòng sau khi loại trùng: {unique_rows}")
    print(f"[INFO] Đã loại bỏ {duplicates} dòng trùng lặp ({duplicates/total_rows*100:.1f}%)")
    
    # Tạo tên file output
    if not output_file:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = f"merged_products_{timestamp}.csv"
    
    # Lưu file
    merged_df.to_csv(output_file, index=False, encoding='utf-8-sig')
    print(f"\n[SUCCESS] Đã lưu file merge vào: {output_file}")
    
    # Hiển thị thống kê
    print("\n[STATS] Thống kê dữ liệu:")
    print(f"  - Tổng sản phẩm unique: {unique_rows}")
    if 'category' in merged_df.columns:
        print(f"  - Số category: {merged_df['category'].nunique()}")
        print("\n  Top 5 category:")
        for cat, count in merged_df['category'].value_counts().head().items():
            print(f"    {cat}: {count} sản phẩm")
    
    return output_file


def merge_by_keyword(keyword, output_file=None):
    """
    Merge các file CSV của một keyword cụ thể
    
    Args:
        keyword: Từ khóa (tên folder)
        output_file: Tên file output
    """
    pattern = f"{keyword}/*.csv"
    if not output_file:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = f"merged_{keyword}_{timestamp}.csv"
    
    return merge_csv_files(pattern=pattern, output_file=output_file)


if __name__ == "__main__":
    import sys
    
    print("=" * 60)
    print("MERGE CSV FILES - Loại bỏ trùng lặp")
    print("=" * 60)
    
    # Nếu có argument từ command line
    if len(sys.argv) > 1:
        keyword = sys.argv[1]
        print(f"\n[INFO] Merge file cho keyword: {keyword}")
        result = merge_by_keyword(keyword)
    else:
        # Interactive mode
        print("\nChọn chế độ:")
        print("1. Merge tất cả file CSV trong thư mục hiện tại")
        print("2. Merge file của một keyword cụ thể")
        
        choice = input("\nNhập lựa chọn (1 hoặc 2): ").strip()
        
        if choice == "1":
            # Merge tất cả
            result = merge_csv_files()
        elif choice == "2":
            # Merge theo keyword
            keyword = input("Nhập keyword (tên folder): ").strip()
            if keyword:
                result = merge_by_keyword(keyword)
            else:
                print("[ERROR] Keyword không được để trống!")
        else:
            print("[ERROR] Lựa chọn không hợp lệ!")
    
    print("\n" + "=" * 60)
