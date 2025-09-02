import sqlite3
from datetime import datetime
import pandas as pd
DB_FILE = "prices.db"
def init_db():
    connection = sqlite3.connect(DB_FILE)
    connection.execute("""
        CREATE TABLE IF NOT EXISTS prices (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT NOT NULL DEFAULT (datetime('now')),
            brand TEXT,
            model TEXT,
            fuel TEXT,
            transmission TEXT,
            variant TEXT,
            price INTEGER,
            source TEXT DEFAULT 'scraped'
        )
    """)
    connection.execute("CREATE INDEX IF NOT EXISTS idx_timestamp ON prices(timestamp)")
    connection.commit()
    connection.close()

def store_prices(prices):
    if not prices:
        return
    conn = sqlite3.connect(DB_FILE)
    now = datetime.now().isoformat()
    conn.executemany("""
        INSERT INTO prices (timestamp, brand, model, fuel, transmission, variant, price)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, [
        (now, r["Brand"], r["Model"], r["Fuel"], r["Transmission"], r["Variant"], r["Price"])
        for r in prices
    ])
    conn.commit()
    conn.close()

def get_latest_prices():
    conn = sqlite3.connect(DB_FILE)
    q = """
        SELECT brand, model, fuel, transmission, variant, price, source, timestamp
        FROM prices
        WHERE source='manual'
        OR timestamp = (
            SELECT MAX(timestamp) FROM prices WHERE source='scraped'
        )
    """
    df = pd.read_sql_query(q, conn)
    conn.close()
    return df

def add_price(brand, model, variant, price, fuel, transmission,timestamp):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("""
        INSERT INTO prices (brand, model, variant, price, fuel, transmission, timestamp, source)
        VALUES (?, ?, ?, ?, ?, ?, ?, 'manual')
    """, (brand, model, variant, price, fuel, transmission, timestamp))
    conn.commit()
    conn.close()

def delete_price(record_id):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("DELETE FROM prices WHERE id = ? AND source='manual'", (record_id,))
    conn.commit()
    conn.close()