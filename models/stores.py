from sqlalchemy import(Column,Integer,Float,String,Date,ForeignKey)
from database.database_connection import Base

class Store(Base):
    __tablename__="stores"
    store_id=Column(String(20),primary_key=True)
    store_name=Column(String(100),nullable=False)
    city=Column(String(100),nullable=False)
    state=Column(String(100),nullable=False)
    store_manager=Column(String(100),nullable=False)
    opening_date=Column(Date,nullable=False)
    status=Column(String(100),nullable=False)





