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
    SparkSession.builder
    .appName("Supply_Chain")
    .master("local[*]")
    .getOrCreate()
)


def extract_products():

    logger.info("Reading products CSV")

    products_df = spark.read.csv(
        PRODUCT_FILE,
        header=True,
        inferSchema=True
    )

    return products_df


def extract_customers():

    logger.info("Reading customers CSV")

    customers_df = spark.read.csv(
        CUSTOMER_FILE,
        header=True,
        inferSchema=True
    )

    return customers_df


def extract_inventory():

    logger.info("Reading inventory CSV")

    inventory_df = spark.read.csv(
        INVENTORY_FILE,
        header=True,
        inferSchema=True
    )

    return inventory_df



def extract_sales():

    logger.info("Reading sales CSV")

    sales_df = spark.read.csv(
        SALE_FILE,
        header=True,
        inferSchema=True
    )

    return sales_df


def extract_stores():

    logger.info("Reading stores CSV")

    stores_df = spark.read.csv(
        STORE_FILE,
        header=True,
        inferSchema=True
    )

    return stores_df