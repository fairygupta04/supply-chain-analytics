from logger_config import logger

from pyspark.sql.functions import (
    col,
    length,
    trim,
    when,
    lit,
    try_to_date
)
def validate_products(products_df):

    logger.info("Products Validation Started")

    required_columns = [
        "product_id",
        "product_name",
        "category",
        "brand",
        "unit_price",
        "supplier_id",
        "status"
    ]

    try:

        missing_columns = [
            column
            for column in required_columns
            if column not in products_df.columns
        ]

        if missing_columns:
            raise ValueError(
                f"Missing Columns in Products DataFrame: {missing_columns}"
            )

        validation_reason = (

            when(
                col("product_id").isNull(),
                lit("Product ID is Null")
            )

            .when(
                col("product_name").isNull()
                | (length(trim(col("product_name"))) == 0),
                lit("Product Name is Null/Empty")
            )

            .when(
                col("category").isNull()
                | (length(trim(col("category"))) == 0),
                lit("Category is Null/Empty")
            )

            .when(
                col("brand").isNull()
                | (length(trim(col("brand"))) == 0),
                lit("Brand is Null/Empty")
            )

            .when(
                col("supplier_id").isNull()
                | (length(trim(col("supplier_id"))) == 0),
                lit("Supplier ID is Null/Empty")
            )

            .when(
                col("status").isNull()
                | (length(trim(col("status"))) == 0),
                lit("Status is Null/Empty")
            )

            .when(
                col("unit_price").isNull(),
                lit("Unit Price is Null")
            )

            .otherwise(lit(None))
        )

        df = products_df.withColumn(
            "validation_reason",
            validation_reason
        )

        invalid_df = df.filter(
            col("validation_reason").isNotNull()
        )

        valid_df = (
            df.filter(col("validation_reason").isNull())
            .drop("validation_reason")
        )

        logger.info(
            f"Products validation completed. "
            f"Accepted: {valid_df.count()}, "
            f"Rejected: {invalid_df.count()}"
        )

        return valid_df, invalid_df

    except Exception as e:

        logger.exception(
            f"Error validating Products: {str(e)}"
        )

        raise
def validate_customers(customers_df):

    logger.info("Customers Validation Started")

    required_columns = [
        "customer_id",
        "customer_name",
        "email",
        "mobile",
        "city",
        "registration_date",
        "customer_type"
    ]

    try:

        missing_columns = [
            column
            for column in required_columns
            if column not in customers_df.columns
        ]

        if missing_columns:
            raise ValueError(
                f"Missing Required Columns: {missing_columns}"
            )


        df = customers_df.withColumn(
            "_registration_date",
            try_to_date(
                trim(col("registration_date").cast("string")),
                "yyyy-MM-dd"
            )
        )

        validation_reason = (

            when(
                col("customer_id").isNull()
                | (length(trim(col("customer_id"))) == 0),
                lit("Customer ID is Null/Empty")
            )

            .when(
                col("customer_name").isNull()
                | (length(trim(col("customer_name"))) == 0),
                lit("Customer Name is Null/Empty")
            )

            .when(
                col("email").isNull()
                | (length(trim(col("email"))) == 0),
                lit("Email is Null/Empty")
            )

            .when(
                ~trim(col("email")).rlike(
                    r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$"
                ),
                lit("Email is Invalid")
            )

            .when(
                col("mobile").isNull()
                | (~trim(col("mobile")).rlike(r"^[0-9]{10}$")),
                lit("Mobile is Invalid")
            )

            .when(
                col("city").isNull()
                | (length(trim(col("city"))) == 0),
                lit("City is Null/Empty")
            )

            # IMPORTANT
            .when(
                col("_registration_date").isNull(),
                lit("Registration Date is Invalid")
            )

            .when(
                col("customer_type").isNull()
                | (length(trim(col("customer_type"))) == 0),
                lit("Customer Type is Null/Empty")
            )

            .otherwise(lit(None))
        )

        df = df.withColumn(
            "validation_reason",
            validation_reason
        )

        invalid_customer_df = (
            df
            .filter(col("validation_reason").isNotNull())
            .drop("_registration_date")
        )

        valid_customer_df = (
            df
            .filter(col("validation_reason").isNull())
            .drop("_registration_date")
            .drop("validation_reason")
        )

        logger.info(
            f"Customers validation completed. "
            f"Accepted: {valid_customer_df.count()}, "
            f"Rejected: {invalid_customer_df.count()}"
        )

        return valid_customer_df, invalid_customer_df

    except Exception as e:

        logger.exception(
            f"Error validating Customers: {str(e)}"
        )

        raise
def validate_stores(stores_df):

    logger.info("Stores Validation Started")

    required_columns = [
        "store_id",
        "store_name",
        "city",
        "state",
        "store_manager",
        "opening_date",
        "status"
    ]

    try:


        missing_columns = [
            column
            for column in required_columns
            if column not in stores_df.columns
        ]

        if missing_columns:
            raise ValueError(
                f"Missing Required Columns: {missing_columns}"
            )


        df = stores_df.withColumn(
            "_opening_date",
            try_to_date(
                trim(col("opening_date").cast("string")),
                "yyyy-MM-dd"
            )
        )

      

        validation_reason = (

            when(
                col("store_id").isNull()
                | (length(trim(col("store_id"))) == 0),
                lit("Store ID is Null/Empty")
            )

            .when(
                col("store_name").isNull()
                | (length(trim(col("store_name"))) == 0),
                lit("Store Name is Null/Empty")
            )

            .when(
                col("city").isNull()
                | (length(trim(col("city"))) == 0),
                lit("City is Null/Empty")
            )

            .when(
                col("state").isNull()
                | (length(trim(col("state"))) == 0),
                lit("State is Invalid")
            )

            .when(
                col("store_manager").isNull()
                | (length(trim(col("store_manager"))) == 0),
                lit("Store Manager is Null/Empty")
            )

            .when(
                col("_opening_date").isNull(),
                lit("Opening Date is Invalid")
            )

            .when(
                col("status").isNull()
                | (length(trim(col("status"))) == 0),
                lit("Status is Invalid")
            )

            .otherwise(lit(None))
        )

        df = df.withColumn(
            "validation_reason",
            validation_reason
        )


        invalid_store_df = (
            df
            .filter(col("validation_reason").isNotNull())
            .drop("_opening_date")
        )


        valid_store_df = (
            df
            .filter(col("validation_reason").isNull())
            .drop("_opening_date")
            .drop("validation_reason")
        )

        logger.info(
            f"Stores validation completed. "
            f"Accepted: {valid_store_df.count()}, "
            f"Rejected: {invalid_store_df.count()}"
        )

        return valid_store_df, invalid_store_df

    except Exception as e:

        logger.exception(
            f"Error validating Stores: {str(e)}"
        )

        raise
def validate_inventory(
    inventory_df,
    product_df,
    store_df
):

    logger.info("Inventory Validation Started")

    required_columns = [
        "inventory_id",
        "product_id",
        "store_id",
        "available_quantity",
        "reorder_level",
        "last_updated"
    ]

    try:


        missing_columns = [
            column
            for column in required_columns
            if column not in inventory_df.columns
        ]

        if missing_columns:
            raise ValueError(
                f"Missing Required Columns: {missing_columns}"
            )


        valid_product_ids = [
            row["product_id"]
            for row in product_df
            .select("product_id")
            .distinct()
            .collect()
        ]

        valid_store_ids = [
            row["store_id"]
            for row in store_df
            .select("store_id")
            .distinct()
            .collect()
        ]


        df = inventory_df.withColumn(
            "_last_updated",
            try_to_date(
                trim(
                    col("last_updated")
                    .cast("string")
                ),
                "yyyy-MM-dd"
            )
        )

  

        validation_reason = (

            when(
                col("inventory_id").isNull()
                | (length(trim(col("inventory_id"))) == 0),
                lit("Inventory ID is Null/Empty")
            )

            .when(
                col("product_id").isNull()
                | (length(trim(col("product_id"))) == 0),
                lit("Product ID is Null/Empty")
            )

            .when(
                ~trim(col("product_id")).isin(
                    valid_product_ids
                ),
                lit("Product ID does not exist")
            )

            .when(
                col("store_id").isNull()
                | (length(trim(col("store_id"))) == 0),
                lit("Store ID is Null/Empty")
            )

            .when(
                ~trim(col("store_id")).isin(
                    valid_store_ids
                ),
                lit("Store ID does not exist")
            )

            .when(
                col("available_quantity").isNull()
                | (col("available_quantity") < 0),
                lit("Available Quantity is Invalid")
            )

            .when(
                col("reorder_level").isNull()
                | (col("reorder_level") <= 0),
                lit("Reorder Level is Invalid")
            )

       
            .when(
                col("_last_updated").isNull(),
                lit("Last Updated Date is Invalid")
            )

            .otherwise(lit(None))
        )

        df = df.withColumn(
            "validation_reason",
            validation_reason
        )

    

        invalid_inventory_df = (
            df
            .filter(
                col("validation_reason").isNotNull()
            )
            .drop("_last_updated")
        )


        valid_inventory_df = (
            df
            .filter(
                col("validation_reason").isNull()
            )
            .drop("_last_updated")
            .drop("validation_reason")
        )

        logger.info(
            f"Inventory validation completed. "
            f"Accepted: {valid_inventory_df.count()}, "
            f"Rejected: {invalid_inventory_df.count()}"
        )

        return (
            valid_inventory_df,
            invalid_inventory_df
        )

    except Exception as e:

        logger.exception(
            f"Error validating Inventory: {str(e)}"
        )

        raise

def validate_sales(
    sales_df,
    product_df,
    store_df,
    customer_df
):

    logger.info("Sales Validation Started")

    required_columns = [
        "sale_id",
        "product_id",
        "store_id",
        "customer_id",
        "quantity",
        "sale_date",
        "discount_percentage",
        "payment_mode"
    ]

    try:


        missing_columns = [
            column
            for column in required_columns
            if column not in sales_df.columns
        ]

        if missing_columns:
            raise ValueError(
                f"Missing Required Columns: {missing_columns}"
            )

        valid_product_ids = [
            row["product_id"]
            for row in product_df
            .select("product_id")
            .distinct()
            .collect()
        ]

        valid_store_ids = [
            row["store_id"]
            for row in store_df
            .select("store_id")
            .distinct()
            .collect()
        ]

        valid_customer_ids = [
            row["customer_id"]
            for row in customer_df
            .select("customer_id")
            .distinct()
            .collect()
        ]



        df = sales_df.withColumn(
            "_sale_date",
            try_to_date(
                trim(
                    col("sale_date")
                    .cast("string")
                ),
                "yyyy-MM-dd"
            )
        )


        validation_reason = (

            when(
                col("sale_id").isNull()
                | (length(trim(col("sale_id"))) == 0),
                lit("Sale ID is Null/Empty")
            )

            .when(
                col("product_id").isNull()
                | (length(trim(col("product_id"))) == 0),
                lit("Product ID is Null/Empty")
            )

            .when(
                ~trim(col("product_id")).isin(
                    valid_product_ids
                ),
                lit("Product ID does not exist")
            )

            .when(
                col("store_id").isNull()
                | (length(trim(col("store_id"))) == 0),
                lit("Store ID is Null/Empty")
            )

            .when(
                ~trim(col("store_id")).isin(
                    valid_store_ids
                ),
                lit("Store ID does not exist")
            )

            .when(
                col("customer_id").isNull()
                | (length(trim(col("customer_id"))) == 0),
                lit("Customer ID is Null/Empty")
            )

            .when(
                ~trim(col("customer_id")).isin(
                    valid_customer_ids
                ),
                lit("Customer ID does not exist")
            )

            .when(
                col("quantity").isNull()
                | (col("quantity") <= 0),
                lit("Quantity is Invalid")
            )

            # IMPORTANT
            .when(
                col("_sale_date").isNull(),
                lit("Sale Date is Invalid")
            )

            .when(
                col("discount_percentage").isNull()
                | (col("discount_percentage") < 0)
                | (col("discount_percentage") > 100),
                lit("Discount Percentage is Invalid")
            )

            .when(
                col("payment_mode").isNull()
                | (length(trim(col("payment_mode"))) == 0),
                lit("Payment Mode is Null/Empty")
            )

            .otherwise(lit(None))
        )

        df = df.withColumn(
            "validation_reason",
            validation_reason
        )



        invalid_sale_df = (
            df
            .filter(
                col("validation_reason").isNotNull()
            )
            .drop("_sale_date")
        )



        valid_sale_df = (
            df
            .filter(
                col("validation_reason").isNull()
            )
            .drop("_sale_date")
            .drop("validation_reason")
        )

        logger.info(
            f"Sales validation completed. "
            f"Accepted: {valid_sale_df.count()}, "
            f"Rejected: {invalid_sale_df.count()}"
        )

        return (
            valid_sale_df,
            invalid_sale_df
        )

    except Exception as e:

        logger.exception(
            f"Error validating Sales: {str(e)}"
        )

        raise