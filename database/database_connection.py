from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker,declarative_base
from sqlalchemy.engine import URL
from config.settings import(
    DB_USERNAME,
    DB_HOST,
    DB_PASSWORD,
    DB_PORT,
    DB_NAME
)

database_url=URL.create(
    drivername="mysql+pymysql",
    username=DB_USERNAME,
    host=DB_HOST,
    password=DB_PASSWORD,
    port=DB_PORT,
    database=DB_NAME
)
engine=create_engine(database_url)
sessionLocal=sessionmaker(bind=engine)
Base=declarative_base()
