"""Import CSV data from TES folders to lazada_etl database."""
import glob
import os
from pathlib import Path
import pandas as pd
from sqlalchemy import create_engine, text
import uuid
from datetime import datetime

# Connection to lazada_etl database
DB_URL = os.environ.get("LAZADA_DB_URL", "postgresql+psycopg2://lazada_user:lazada_pass@localhost:5432/lazada_etl")

BASE_DIR = Path(__file__).resolve().parent

TABLE_NAME = "products"


def generate_id():
    """Generate unique ID."""
    return str(uuid.uuid4())


def extract_lazada_id(url):
    """Extract Lazada product ID from URL."""
    if not url or pd.isna(url):
        return None
    try:
        # URL format: https://www.lazada.vn/products/pdp-i2038581815.html
        if 'pdp-i' in url:
            lazada_id = url.split('pdp-i')[1].split('.')[0]
            return lazada_id
    except:
        pass
    return None


def map_csv_to_schema(df, category_name):
    """Map CSV columns to database schema."""
    mapped_data = []
    
    for _, row in df.iterrows():
        url = row.get('url_san_pham', '')
        lazada_id = extract_lazada_id(url)
        
        product = {
            'id': generate_id(),
            'lazada_id': lazada_id,
            'name': row.get('ten_san_pham', ''),
            'price': float(row.get('gia_sale', 0)) if pd.notna(row.get('gia_sale')) else None,
            'original_price': float(row.get('gia_goc', 0)) if pd.notna(row.get('gia_goc')) else None,
            'discount': None,  # Calculate if needed
            'url': url,
            'image_url': row.get('link_anh', ''),
            'category': category_name,
            'brand': None,  # Not in CSV
            'sold_count': None,  # Not in CSV
            'rating_score': float(row.get('rating', 0)) if pd.notna(row.get('rating')) else None,
            'rating_count': int(row.get('so_review', 0)) if pd.notna(row.get('so_review')) else 0,
            'is_verified': False,
            'is_duplicate': False,
            'extracted_at': datetime.now(),
            'transformed_at': None,
            'loaded_at': datetime.now(),
            'created_at': datetime.now(),
            'updated_at': datetime.now()
        }
        
        # Calculate discount percentage
        if product['original_price'] and product['price']:
            product['discount'] = round((1 - product['price'] / product['original_price']) * 100, 2)
        
        mapped_data.append(product)
    
    return pd.DataFrame(mapped_data)


def load_csvs():
    """Load all CSV files from category folders."""
    engine = create_engine(DB_URL, echo=False)

    # Get all category folders
    category_dirs = [p for p in BASE_DIR.iterdir() if p.is_dir()]
    if not category_dirs:
        print(f"No category folders found under {BASE_DIR}")
        return

    total_rows = 0
    skipped_rows = 0
    
    for cat_dir in category_dirs:
        category = cat_dir.name
        csv_files = glob.glob(str(cat_dir / "lazada_products_*.csv"))
        if not csv_files:
            continue

        for csv_path in csv_files:
            print(f"Loading {csv_path} ...")
            try:
                # Read CSV
                df = pd.read_csv(csv_path, encoding="utf-8-sig", dtype=str)
                
                # Map to new schema
                mapped_df = map_csv_to_schema(df, category)
                
                # Insert to database (use upsert to avoid duplicates)
                with engine.begin() as conn:
                    for _, row in mapped_df.iterrows():
                        # Check if lazada_id already exists
                        if row['lazada_id']:
                            result = conn.execute(
                                text("SELECT id FROM products WHERE lazada_id = :lazada_id"),
                                {"lazada_id": row['lazada_id']}
                            )
                            if result.fetchone():
                                skipped_rows += 1
                                continue
                        
                        # Insert new product
                        conn.execute(
                            text("""
                                INSERT INTO products (
                                    id, lazada_id, name, price, original_price, discount,
                                    url, image_url, category, brand, sold_count,
                                    rating_score, rating_count, is_verified, is_duplicate,
                                    extracted_at, loaded_at, created_at, updated_at
                                ) VALUES (
                                    :id, :lazada_id, :name, :price, :original_price, :discount,
                                    :url, :image_url, :category, :brand, :sold_count,
                                    :rating_score, :rating_count, :is_verified, :is_duplicate,
                                    :extracted_at, :loaded_at, :created_at, :updated_at
                                )
                            """),
                            row.to_dict()
                        )
                        total_rows += 1
                
                print(f"Inserted {len(mapped_df)} rows from {category}")
                
            except Exception as e:
                print(f"Error processing {csv_path}: {str(e)}")
                continue

    print(f"\nDone!")
    print(f"Total inserted: {total_rows}")
    print(f"Total skipped (duplicates): {skipped_rows}")


if __name__ == "__main__":
    load_csvs()
