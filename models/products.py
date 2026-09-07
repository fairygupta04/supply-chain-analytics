from sqlalchemy import(Column,Integer,Float,String,Date,ForeignKey)
from database.database_connection import Base

class Product(Base):
    __tablename__="products"
    product_id=Column(String(20),primary_key=True)
    product_name=Column(String(100),nullable=False)
    category=Column(String(100),nullable=False)
    brand=Column(String(100),nullable=False)
    unit_price=Column(Float,nullable=False)
    supplier_id=Column(String(20),nullable=False)
    status=Column(String(100),nullable=False)

