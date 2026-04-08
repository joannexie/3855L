import yaml
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

# load db config from yaml
with open("/config/storage_config.yaml", "r") as f:
    app_config = yaml.safe_load(f.read())

ds = app_config["datastore"]

user = ds["user"]
password = ds["password"]
hostname = ds["hostname"]
port = ds["port"]
db = ds["db"]
driver = ds.get("driver", "pymysql")  

# MySQL SQLAlchemy URL
DB_URL = f"mysql+{driver}://{user}:{password}@{hostname}:{port}/{db}"

ENGINE = create_engine(
    DB_URL,
    echo=False,
    future=True,
    pool_pre_ping=True,
    connect_args={"connect_timeout": 10}
)


SessionLocal = sessionmaker(bind=ENGINE, autoflush=False, autocommit=False, future=True)

def make_session():
    return SessionLocal()
