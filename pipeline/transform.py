from logger_config import logger

from pyspark.sql.functions import (
    col,
    trim,
    upper,
    lower,
    initcap,
    regexp_replace,
    try_to_date
)


def transform_products(product_df):

    logger.info("Products Transformation Started")

    try:

        df = (
            product_df
            .withColumn(
                "product_id",
                trim(col("product_id"))
            )
            .withColumn(
                "product_name",
                initcap(trim(col("product_name")))
            )
            .withColumn(
                "category",
                initcap(trim(col("category")))
            )
            .withColumn(
                "brand",
                initcap(trim(col("brand")))
            )
            .withColumn(
                "unit_price",
                col("unit_price").cast("double")
            )
            .withColumn(
                "supplier_id",
                trim(col("supplier_id"))
            )
            .withColumn(
                "status",
                upper(trim(col("status")))
            )
        )

        logger.info("Products Transformation Completed")

        return df

    except Exception as e:

        logger.exception(
            f"Error transforming Products: {str(e)}"
        )

        raise


def transform_customers(customer_df):

    logger.info("Customers Transformation Started")

    try:

        df = (
            customer_df
            .withColumn(
                "customer_id",
                trim(col("customer_id"))
            )
            .withColumn(
                "customer_name",
                initcap(trim(col("customer_name")))
            )
            .withColumn(
                "email",
                lower(trim(col("email")))
            )
            .withColumn(
                "mobile",
                regexp_replace(
                    trim(col("mobile").cast("string")),
                    r"[^0-9]",
                    ""
                )
            )
            .withColumn(
                "city",
                initcap(trim(col("city")))
            )
            .withColumn(
                "registration_date",
                try_to_date(
                    trim(
                        col("registration_date")
                        .cast("string")
                    ),
                    "yyyy-MM-dd"
                )
            )
            .withColumn(
                "customer_type",
                upper(trim(col("customer_type")))
            )
        )

        logger.info("Customers Transformation Completed")

        return df

    except Exception as e:

        logger.exception(
            f"Error transforming Customers: {str(e)}"
        )

        raise



def transform_stores(store_df):

    logger.info("Stores Transformation Started")

    try:

        df = (
            store_df
            .withColumn(
                "store_id",
                trim(col("store_id"))
            )
            .withColumn(
                "store_name",
                initcap(trim(col("store_name")))
            )
            .withColumn(
                "city",
                initcap(trim(col("city")))
            )
            .withColumn(
                "state",
                initcap(trim(col("state")))
            )
            .withColumn(
                "store_manager",
                initcap(trim(col("store_manager")))
            )
            .withColumn(
                "opening_date",
                try_to_date(
                    trim(
                        col("opening_date")
                        .cast("string")
                    ),
                    "yyyy-MM-dd"
                )
            )
            .withColumn(
                "status",
                upper(trim(col("status")))
            )
        )

        logger.info("Stores Transformation Completed")

        return df

    except Exception as e:

        logger.exception(
            f"Error transforming Stores: {str(e)}"
        )

        raise


def transform_inventory(inventory_df):

    logger.info("Inventory Transformation Started")

    try:

        df = (
            inventory_df
            .withColumn(
                "inventory_id",
                trim(col("inventory_id"))
            )
            .withColumn(
                "product_id",
                trim(col("product_id"))
            )
            .withColumn(
                "store_id",
                trim(col("store_id"))
            )
            .withColumn(
                "available_quantity",
                col("available_quantity").cast("int")
            )
            .withColumn(
                "reorder_level",
                col("reorder_level").cast("int")
            )
            .withColumn(
                "last_updated",
                try_to_date(
                    trim(
                        col("last_updated")
                        .cast("string")
                    ),
                    "yyyy-MM-dd"
                )
            )
        )

        logger.info("Inventory Transformation Completed")

        return df

    except Exception as e:

        logger.exception(
            f"Error transforming Inventory: {str(e)}"
        )

        raise


def transform_sales(sales_df):

    logger.info("Sales Transformation Started")

    try:

        df = (
            sales_df
            .withColumn(
                "sale_id",
                trim(col("sale_id"))
            )
            .withColumn(
                "product_id",
                trim(col("product_id"))
            )
            .withColumn(
                "store_id",
                trim(col("store_id"))
            )
            .withColumn(
                "customer_id",
                trim(col("customer_id"))
            )
            .withColumn(
                "quantity",
                col("quantity").cast("int")
            )
            .withColumn(
                "sale_date",
                try_to_date(
                    trim(
                        col("sale_date")
                        .cast("string")
                    ),
                    "yyyy-MM-dd"
                )
            )
            .withColumn(
                "discount_percentage",
                col("discount_percentage").cast("int")
            )
            .withColumn(
                "payment_mode",
                upper(trim(col("payment_mode")))
            )
        )

        logger.info("Sales Transformation Completed")

        return df

    except Exception as e:

        logger.exception(
            f"Error transforming Sales: {str(e)}"
        )

        raise