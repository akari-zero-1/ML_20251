import pandas as pd

# Đọc 2 file CSV
df1 = pd.read_csv('archive/Restaurant reviews.csv')
df2 = pd.read_csv('archive/restaurant_reviews_10k.csv')

# Hiển thị kích thước ban đầu
print(f"File 1: {len(df1)} dòng")
print(f"File 2: {len(df2)} dòng")
print(f"Tổng: {len(df1) + len(df2)} dòng")

# Gộp 2 file
merged_df = pd.concat([df1, df2], ignore_index=True)
print(f"\nSau khi gộp: {len(merged_df)} dòng")

# Loại bỏ trùng lặp
merged_df_no_duplicates = merged_df.drop_duplicates()
print(f"Sau khi loại bỏ trùng lặp: {len(merged_df_no_duplicates)} dòng")
print(f"Số dòng trùng lặp đã xóa: {len(merged_df) - len(merged_df_no_duplicates)}")

# Lưu file kết quả
output_file = 'archive/merged_restaurant_reviews.csv'
merged_df_no_duplicates.to_csv(output_file, index=False)
print(f"\nĐã lưu file kết quả vào: {output_file}")
