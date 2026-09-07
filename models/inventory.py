from sqlalchemy import(Column,Integer,Float,String,Date,ForeignKey)
from database.database_connection import Base

class Inventory(Base):
    __tablename__="inventories"
    inventory_id=Column(String(20),primary_key=True)
    product_id=Column(String(20),ForeignKey("products.product_id"),nullable=False)
    store_id=Column(String(20),ForeignKey("stores.store_id"),nullable=False)
    available_quantity=Column(Integer,nullable=False)
    reorder_level=Column(Integer,nullable=False)
    last_updated=Column(Date,nullable=False)