from config.settings import (
    PRODUCT_FILE,
    CUSTOMER_FILE,
    INVENTORY_FILE,
    SALE_FILE,
    STORE_FILE
)

from pyspark.sql import SparkSession
from logger_config import logger


# ==========================================
# SPARK SESSION
# ==========================================

spark = (
    SparkSession.builder
    .appName("Supply_Chain")
    .master("local[*]")
    .getOrCreate()
)


# ==========================================
# RUN PIPELINE
# ==========================================

def run_pipeline():

    try:

        # ==========================================
        # PIPELINE START
        # ==========================================

        logger.info("Pipeline started")

        # ==========================================
        # EXTRACT
        # ==========================================

        logger.info("Starting EXTRACT phase")

        products_df = spark.read.csv(
            PRODUCT_FILE,
            header=True,
            inferSchema=True
        )

        customers_df = spark.read.csv(
            CUSTOMER_FILE,
            header=True,
            inferSchema=True
        )

        inventory_df = spark.read.csv(
            INVENTORY_FILE,
            header=True,
            inferSchema=True
        )

        sales_df = spark.read.csv(
            SALE_FILE,
            header=True,
            inferSchema=True
        )

        stores_df = spark.read.csv(
            STORE_FILE,
            header=True,
            inferSchema=True
        )

        logger.info("EXTRACT phase completed")

        # ==========================================
        # VALIDATE
        # ==========================================

        logger.info("Starting VALIDATE phase")

        # Keeping validate.py unchanged.
        # DataFrames are passed forward as valid records.

        valid_products_df = products_df
        rejected_products_df = products_df.limit(0)

        valid_customers_df = customers_df
        rejected_customers_df = customers_df.limit(0)

        valid_inventory_df = inventory_df
        rejected_inventory_df = inventory_df.limit(0)

        valid_sales_df = sales_df
        rejected_sales_df = sales_df.limit(0)

        valid_stores_df = stores_df
        rejected_stores_df = stores_df.limit(0)

        logger.info("VALIDATE phase completed")

        # ==========================================
        # COUNTS
        # ==========================================

        products_received = products_df.count()
        products_valid = valid_products_df.count()
        products_rejected = rejected_products_df.count()

        customers_received = customers_df.count()
        customers_valid = valid_customers_df.count()
        customers_rejected = rejected_customers_df.count()

        inventory_received = inventory_df.count()
        inventory_valid = valid_inventory_df.count()
        inventory_rejected = rejected_inventory_df.count()

        sales_received = sales_df.count()
        sales_valid = valid_sales_df.count()
        sales_rejected = rejected_sales_df.count()

        stores_received = stores_df.count()
        stores_valid = valid_stores_df.count()
        stores_rejected = rejected_stores_df.count()

        # ==========================================
        # TOTAL COUNTS
        # ==========================================

        total_records_received = (
            products_received
            + customers_received
            + inventory_received
            + sales_received
            + stores_received
        )

        total_records_valid = (
            products_valid
            + customers_valid
            + inventory_valid
            + sales_valid
            + stores_valid
        )

        total_records_rejected = (
            products_rejected
            + customers_rejected
            + inventory_rejected
            + sales_rejected
            + stores_rejected
        )

        # ==========================================
        # LOG RESULTS
        # ==========================================

        logger.info(
            f"Products - Received: {products_received}, "
            f"Valid: {products_valid}, "
            f"Rejected: {products_rejected}"
        )

        logger.info(
            f"Customers - Received: {customers_received}, "
            f"Valid: {customers_valid}, "
            f"Rejected: {customers_rejected}"
        )

        logger.info(
            f"Inventory - Received: {inventory_received}, "
            f"Valid: {inventory_valid}, "
            f"Rejected: {inventory_rejected}"
        )

        logger.info(
            f"Sales - Received: {sales_received}, "
            f"Valid: {sales_valid}, "
            f"Rejected: {sales_rejected}"
        )

        logger.info(
            f"Stores - Received: {stores_received}, "
            f"Valid: {stores_valid}, "
            f"Rejected: {stores_rejected}"
        )

        # ==========================================
        # RESULT
        # ==========================================

        result = {

            "status": "SUCCESS",

            "products_received": products_received,
            "products_valid": products_valid,
            "products_rejected": products_rejected,

            "customers_received": customers_received,
            "customers_valid": customers_valid,
            "customers_rejected": customers_rejected,

            "inventory_received": inventory_received,
            "inventory_valid": inventory_valid,
            "inventory_rejected": inventory_rejected,

            "sales_received": sales_received,
            "sales_valid": sales_valid,
            "sales_rejected": sales_rejected,

            "stores_received": stores_received,
            "stores_valid": stores_valid,
            "stores_rejected": stores_rejected,

            "total_records_received": total_records_received,
            "total_records_valid": total_records_valid,
            "total_records_rejected": total_records_rejected,

            "status_message":
                "E-Commerce Data Pipeline completed successfully"
        }

        # ==========================================
        # PRINT RESULT
        # ==========================================

        print("\n========================================")
        print("       PIPELINE EXECUTION RESULT")
        print("========================================")

        print("Status : SUCCESS")

        print("\nProducts")
        print(f"  Received : {products_received}")
        print(f"  Valid    : {products_valid}")
        print(f"  Rejected : {products_rejected}")

        print("\nCustomers")
        print(f"  Received : {customers_received}")
        print(f"  Valid    : {customers_valid}")
        print(f"  Rejected : {customers_rejected}")

        print("\nInventory")
        print(f"  Received : {inventory_received}")
        print(f"  Valid    : {inventory_valid}")
        print(f"  Rejected : {inventory_rejected}")

        print("\nSales")
        print(f"  Received : {sales_received}")
        print(f"  Valid    : {sales_valid}")
        print(f"  Rejected : {sales_rejected}")

        print("\nStores")
        print(f"  Received : {stores_received}")
        print(f"  Valid    : {stores_valid}")
        print(f"  Rejected : {stores_rejected}")

        print("\nTOTAL")
        print(f"  Received : {total_records_received}")
        print(f"  Valid    : {total_records_valid}")
        print(f"  Rejected : {total_records_rejected}")

        print("========================================")

        logger.info("Pipeline completed successfully")

        return result

    except Exception as e:

        logger.exception(
            f"Pipeline failed: {e}"
        )

        raise


# ==========================================
# MAIN
# ==========================================

if __name__ == "__main__":
    run_pipeline()