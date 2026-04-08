from db import ENGINE
from models import Base

if __name__ == "__main__":
    Base.metadata.create_all(ENGINE)
    print("Tables created.")
