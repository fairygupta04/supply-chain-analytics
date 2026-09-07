from sqlalchemy import(Column,Integer,Float,String,Date,ForeignKey)
from database.database_connection import Base

class Sale(Base):
    __tablename__="sales"
    sale_id=Column(String(20),primary_key=True)
    product_id=Column(String(20),ForeignKey("products.product_id"),nullable=False)
    store_id=Column(String(20),ForeignKey("stores.store_id"),nullable=False)
    customer_id=Column(String(20),ForeignKey("customers.customer_id"),nullable=False)
    quantity=Column(Integer,nullable=False)
    sale_date=Column(Date,nullable=False)
    discount_percentage=Column(Integer,nullable=False)
    payment_mode=Column(String(50),nullable=False)