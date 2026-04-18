import sqlite3
from pathlib import Path

DB_PATH = Path('tracker.db')


def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    with sqlite3.connect(DB_PATH) as conn:
        conn.executescript("""
        CREATE TABLE IF NOT EXISTS events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            send_id TEXT NOT NULL,
            campaign_id TEXT DEFAULT '',
            recipient_email TEXT DEFAULT '',
            event_type TEXT NOT NULL,
            url TEXT DEFAULT '',
            user_agent TEXT DEFAULT '',
            ip_address TEXT DEFAULT '',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """)
