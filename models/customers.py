from sqlalchemy import(Column,Integer,Float,String,Date,ForeignKey)
from database.database_connection import Base

class Customer(Base):
    __tablename__="customers"
    customer_id=Column(String(20),primary_key=True)
    customer_name=Column(String(100),nullable=False)
    email=Column(String(100),nullable=False)
    mobile=Column(String(10),nullable=False)
    city=Column(String(100),nullable=False)
    registration_date=Column(Date,nullable=False)
    customer_type=Column(String(100),nullable=False)
