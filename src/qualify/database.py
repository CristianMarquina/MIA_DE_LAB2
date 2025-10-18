from sqlalchemy import create_engine

# Database configuration
DB_USER = "root"
DB_PASSWORD = "admin" 
DB_HOST = "localhost"
DB_PORT = "3306"
DB_NAME = "f1_db"  

DATABASE_URI = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

try:
    engine = create_engine(DATABASE_URI)
    print("Database engine created successfully.")
except Exception as e:
    print(f"Failed to create database engine: {e}")