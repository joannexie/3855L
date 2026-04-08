from db import ENGINE
from models import Base

if __name__ == "__main__":
    Base.metadata.drop_all(ENGINE)
    print(" Tables dropped.")
