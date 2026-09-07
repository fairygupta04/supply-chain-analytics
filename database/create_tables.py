from database.database_connection import(engine,Base)
from models.customers import *
from models.inventory import *
from models.products import *
from models.sales import *
from models.stores import *

def create_database_tables():
    Base.metadata.create_all(bind=engine)
    print("Tables created successfully")

if __name__=="__main__":
    create_database_tables()
