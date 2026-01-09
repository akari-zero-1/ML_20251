import pandas as pd
from datetime import datetime
import os
import glob

# Lấy tất cả file CSV trong thư mục hiện tại
csv_files = glob.glob('*.csv')
print(f"Tìm thấy {len(csv_files)} file CSV:\n")

# Đọc tất cả file CSV
dataframes = []
for file in csv_files:
    print(f"Đang đọc file {file}...")
    df = pd.read_csv(file)
    print(f"  -> {len(df)} dòng")
    dataframes.append(df)

if not dataframes:
    print("Không tìm thấy file CSV nào!")
    exit()

# Merge tất cả dataframe (ghép theo chiều dọc)
df_merged = pd.concat(dataframes, ignore_index=True)
print(f"\nTổng số dòng sau khi merge: {len(df_merged)}")

# Loại bỏ các dòng trùng lặp nếu có (dựa trên url_san_pham)
if 'url_san_pham' in df_merged.columns:
    df_merged_unique = df_merged.drop_duplicates(subset=['url_san_pham'], keep='first')
    print(f"Sau khi loại bỏ trùng lặp: {len(df_merged_unique)} dòng")
    print(f"Đã loại bỏ {len(df_merged) - len(df_merged_unique)} dòng trùng lặp")
else:
    df_merged_unique = df_merged.drop_duplicates()
    print(f"Sau khi loại bỏ trùng lặp: {len(df_merged_unique)} dòng")

# Lưu file kết quả
output_file = f'product_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
df_merged_unique.to_csv(output_file, index=False, encoding='utf-8-sig')
print(f"\nĐã lưu file kết quả: {output_file}")

# Hiển thị thông tin về các cột
print("\nThông tin về dữ liệu:")
print(df_merged_unique.info())