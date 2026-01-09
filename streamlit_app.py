# import streamlit as st
# import pandas as pd
# import numpy as np
# import pickle
# import time
# import os
# from sklearn.feature_extraction.text import TfidfVectorizer
# from sklearn.neighbors import NearestNeighbors
# from sklearn.decomposition import TruncatedSVD
# from sklearn.metrics.pairwise import cosine_similarity
# import warnings
# warnings.filterwarnings('ignore')

# # --- CẤU HÌNH TRANG ---
# st.set_page_config(
#     page_title="Product Recommendation System",
#     page_icon="🛍️",
#     layout="wide"
# )

# # --- CSS TÙY CHỈNH ---
# st.markdown("""
# <style>
#     .main-header { font-size: 2.5rem; font-weight: bold; color: #4ECDC4; text-align: center; margin-bottom: 2rem; }
#     .product-card { border-radius: 10px; padding: 15px; background-color: #f9f9f9; box-shadow: 0 4px 6px rgba(0,0,0,0.1); }
# </style>
# """, unsafe_allow_html=True)

# # --- HÀM XỬ LÝ DỮ LIỆU ---
# @st.cache_data
# def load_data():
#     """Load và chuẩn bị dữ liệu"""
#     try:
#         df = pd.read_csv('amazon_products_cleaned.csv')
#         # Lấy sample 10,000 sản phẩm để tốc độ nhanh hơn
#         df_rec = df.sample(n=min(10000, len(df)), random_state=42).reset_index(drop=True)
        
#         # Tạo combined_features từ processed_text (giống notebook)
#         if 'processed_text' in df_rec.columns:
#             df_rec['combined_features'] = df_rec['processed_text']
#         elif 'combined_features' not in df_rec.columns:
#             # Fallback: tạo combined_features từ các cột có sẵn
#             df_rec['combined_features'] = (
#                 df_rec['name'].fillna('') + ' ' + 
#                 df_rec['sub_category'].fillna('') + ' ' + 
#                 df_rec['main_category'].fillna('')
#             ).str.lower()
        
#         # Đảm bảo có popularity_score
#         if 'popularity_score' not in df_rec.columns:
#             df_rec['popularity_score'] = df_rec['ratings'] * np.log1p(df_rec.get('no_of_ratings', 0))
            
#         return df_rec
#     except Exception as e:
#         st.error(f"Lỗi khi load dữ liệu: {e}")
#         return None

# @st.cache_resource
# def build_models(_df_rec):
#     """Xây dựng models khi chưa có file lưu sẵn"""
#     tfidf = TfidfVectorizer(max_features=3000, stop_words='english')
#     tfidf_matrix = tfidf.fit_transform(_df_rec['combined_features'])
    
#     knn_model = NearestNeighbors(n_neighbors=20, metric='cosine', algorithm='brute')
#     knn_model.fit(tfidf_matrix)
    
#     svd = TruncatedSVD(n_components=100, random_state=42)
#     svd_matrix = svd.fit_transform(tfidf_matrix)
    
#     return tfidf_matrix, knn_model, svd_matrix

# @st.cache_resource
# def load_trained_model(_df_rec):
#     """Hàm sửa lỗi NameError: Kiểm tra file pkl, nếu không có thì build mới"""
#     model_path = 'models_data.pkl'
    
#     if os.path.exists(model_path):
#         try:
#             with open(model_path, 'rb') as f:
#                 return pickle.load(f)
#         except:
#             pass # Nếu lỗi file thì build lại
            
#     # Nếu chưa có file hoặc file lỗi, tiến hành build
#     tfidf_mat, knn, svd_mat = build_models(_df_rec)
    
#     # Lưu lại để lần sau load cho nhanh
#     with open(model_path, 'wb') as f:
#         pickle.dump((tfidf_mat, knn, svd_mat), f)
        
#     return tfidf_mat, knn, svd_mat

# # --- CÁC HÀM GỢI Ý (Giữ nguyên logic của bạn) ---
# def recommend_cosine(df_rec, tfidf_matrix, product_name, top_n=5):
#     try:
#         idx = df_rec[df_rec['name'].str.contains(product_name, case=False, na=False)].index[0]
#         product_vector = tfidf_matrix[idx]
#         cosine_similarities = cosine_similarity(product_vector, tfidf_matrix).flatten()
#         candidate_indices = np.argsort(cosine_similarities)[::-1][1:top_n+1]
        
#         result = df_rec.iloc[candidate_indices].copy()
#         result['similarity_score'] = cosine_similarities[candidate_indices]
#         return result, idx
#     except: return pd.DataFrame(), None

# def recommend_knn(df_rec, knn_model, tfidf_matrix, product_name, top_n=5):
#     try:
#         idx = df_rec[df_rec['name'].str.contains(product_name, case=False, na=False)].index[0]
#         distances, indices = knn_model.kneighbors(tfidf_matrix[idx], n_neighbors=top_n+1)
#         res_idx = indices.flatten()[1:]
#         result = df_rec.iloc[res_idx].copy()
#         result['similarity_score'] = 1 - distances.flatten()[1:]
#         return result, idx
#     except: return pd.DataFrame(), None

# def recommend_svd(df_rec, svd_matrix, product_name, top_n=5):
#     try:
#         idx = df_rec[df_rec['name'].str.contains(product_name, case=False, na=False)].index[0]
#         product_vector = svd_matrix[idx].reshape(1, -1)
#         svd_similarities = cosine_similarity(product_vector, svd_matrix).flatten()
#         candidate_indices = np.argsort(svd_similarities)[::-1][1:top_n+1]
        
#         result = df_rec.iloc[candidate_indices].copy()
#         result['similarity_score'] = svd_similarities[candidate_indices]
#         return result, idx
#     except: return pd.DataFrame(), None

# # --- GIAO DIỆN CHÍNH ---
# def display_product_card(product):
#     col1, col2 = st.columns([1, 3])
#     with col1:
#         st.image(product['image'] if pd.notnull(product['image']) else "https://via.placeholder.com/150", use_container_width=True)
#     with col2:
#         st.write(f"**{product['name']}**")
#         st.write(f"⭐ {product['ratings']} | 💰 ₹{product.get('discount_price_num', 0):,.0f}")
#         if 'similarity_score' in product:
#             st.info(f"Độ tương đồng: {product['similarity_score']*100:.1f}%")

# def main():
#     st.markdown('<div class="main-header">🛍️ Product Recommendation System</div>', unsafe_allow_html=True)
    
#     # Hiển thị UI trước để user thấy ngay
#     st.sidebar.header("⚙️ Cấu hình")
#     algorithm = st.sidebar.selectbox("🔍 Thuật toán", ["Cosine Similarity", "k-Nearest Neighbors (kNN)", "SVD (Matrix Factorization)"])
#     top_n = st.sidebar.slider("📊 Số lượng gợi ý", 3, 10, 5)
    
#     st.sidebar.markdown("---")
#     st.sidebar.info("""
#     **📖 Hướng dẫn:**
#     1. Nhập tên sản phẩm vào ô tìm kiếm
#     2. Chọn thuật toán gợi ý
#     3. Xem kết quả gợi ý tương tự
#     """)
    
#     # Load data và models
#     status = st.empty()
#     status.info("⏳ Đang khởi tạo hệ thống... (chỉ mất vài giây)")
    
#     df_rec = load_data()
#     if df_rec is None: 
#         status.error("❌ Không thể load dữ liệu!")
#         return
    
#     tfidf_matrix, knn_model, svd_matrix = load_trained_model(df_rec)
#     status.success("✅ Hệ thống đã sẵn sàng!")
#     time.sleep(1)
#     status.empty()

#     # Tìm kiếm
#     search_query = st.text_input("🔎 Tìm sản phẩm Amazon:", placeholder="Ví dụ: iPhone, Laptop, Headphone...")
    
#     if search_query:
#         if algorithm == "Cosine Similarity":
#             res, _ = recommend_cosine(df_rec, tfidf_matrix, search_query, top_n)
#         elif algorithm == "k-Nearest Neighbors (kNN)":
#             res, _ = recommend_knn(df_rec, knn_model, tfidf_matrix, search_query, top_n)
#         else:
#             res, _ = recommend_svd(df_rec, svd_matrix, search_query, top_n)
            
#         if not res.empty:
#             st.subheader(f"Sản phẩm gợi ý bằng {algorithm}:")
#             for _, row in res.iterrows():
#                 with st.container():
#                     display_product_card(row)
#                     st.divider()
#         else:
#             st.warning("Không tìm thấy sản phẩm phù hợp.")

# if __name__ == "__main__":
#     main()