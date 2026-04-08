import os
import sqlite3
import yaml

APP_CONFIG_FILE = os.environ.get("APP_CONFIG_FILE", "/config/health_config.yml")

with open(APP_CONFIG_FILE, "r") as f:
    app_config = yaml.safe_load(f)

DB_FILE = app_config["datastore"]["filename"]

conn = sqlite3.connect(DB_FILE)
cur = conn.cursor()

cur.execute(
    """
    CREATE TABLE IF NOT EXISTS service_status (
        service_name TEXT PRIMARY KEY,
        status TEXT NOT NULL,
        last_update TEXT NOT NULL
    )
    """
)

conn.commit()
conn.close()

print("Health service database tables created successfully.")