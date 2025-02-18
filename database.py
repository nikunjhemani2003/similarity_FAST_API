import psycopg2
from psycopg2.extras import RealDictCursor

# PostgreSQL connection details
DB_CONFIG = {
    "dbname": "nikunj",
    "user": "postgres",
    "password": "12345678",
    "host": "localhost",
    "port": "5432"
}

def get_db_connection():
    """Create and return a database connection"""
    try:
        return psycopg2.connect(**DB_CONFIG, cursor_factory=RealDictCursor)
    except Exception as e:
        print(f"Database connection error: {e}")
        raise 