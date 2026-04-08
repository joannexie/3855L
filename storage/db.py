import yaml
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

# load db config
with open("/config/storage_config.yaml", "r") as f:
    app_config = yaml.safe_load(f.read())

ds = app_config["datastore"]

user = ds["user"]
password = ds["password"]
hostname = ds["hostname"]
port = ds["port"]
db = ds["db"]
driver = ds.get("driver", "pymysql")

DB_URL = f"mysql+{driver}://{user}:{password}@{hostname}:{port}/{db}"

# L11: updated engine settings to reduce lost/stale DB connection issues
ENGINE = create_engine(
    DB_URL,
    echo=False,
    future=True,

    # L11: control pool size
    pool_size=5,

    # L11: allow extra temporary connections if needed
    max_overflow=10,

    # L11: recycle older connections before they go stale
    pool_recycle=1800,

    # L11: test connection before using it
    pool_pre_ping=True,

    # L11: connection timeout
    connect_args={"connect_timeout": 10}
)

SessionLocal = sessionmaker(
    bind=ENGINE,
    autoflush=False,
    autocommit=False,
    future=True
)

def make_session():
    return SessionLocal()