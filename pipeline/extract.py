from config.settings import (
    PRODUCT_FILE,
    STORE_FILE,
    SALE_FILE,
    INVENTORY_FILE,
    CUSTOMER_FILE
)
from pyspark.sql import SparkSession
from logger_config import logger


spark = (
    SparkSession.builder.appName("Supply_Chain").master("local[*]").getOrCreate()
)

def extract_customers():
    logger.info("Reading customers csv")
    customers_df=spark.read.csv("CUSTOMER_FILE")
    return customers_df

def extract_inventory():
    logger.info("Reading inventory csv")
    inventory_df=spark.read.csv("INVENTORY_FILE")
    return inventory_df

def extract_products():
    logger.info("Reading products csv")
    products_df=spark.read.csv("PRODUCT_FILE")
    return products_df

def extract_sales():
    logger.info("Reading sales csv")
    sales_df=spark.read.csv("SALE_FILE")
    return sales_df

def extract_stores():
    logger.info("Reading stores csv")
    stores_df=spark.read.csv("STORE_FILE")
    return stores_df
