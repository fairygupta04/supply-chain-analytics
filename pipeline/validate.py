from logger_config import logger
from pyspark.sql.functions import col,length,trim,when,lit,to_date,to_number,count,filter
def validate_products(products_df):
    logger.info("Products Validation Started")
    required_columns=["product_id","product_name","category","brand","unit_price","supplier_id","status"]
    try:
        missing_columns=[column for column in required_columns
                         if column not in products_df]
        if missing_columns:
            raise ValueError("Missing Columns in Products DataFrame:",missing_columns)
        df=products_df
        str_columns=["product_id","product_name","category","brand","supplier_id","status"]
        for column in str_columns:
            df=df.withColumn(column,trim(col(column)))
        df=df.withColumn(column,to_number(col("unit_price")))
        validation_reason = (when( col("product_id").isNull(), lit("Product ID is Null") ).when( col("product_name").isNull() | (length(col("product_name")) == 0), lit("Product Name is Null/Empty") ).when( col("category").isNull() | (length(col("category")) == 0), lit("Category is Null/Empty") ).when( col("brand").isNull(), lit("Brand is Invalid") ).when( col("supplier_id").isNull(), lit("Supplier ID is Null/Empty") ).when( col("status").isNull(), lit("Status is Null") ).when( col("unit_price").isNull() | (col("unit_price")<=0), lit("Invalid Unit Price") ).otherwise( lit(None) ) )      
        df=df.withColumn("validation_reason",validation_reason)
        invalid_products_df=df.filter(col("validation_reason").isNotNull())
        valid_products_df=df.filter(col("validation_reason").isNull()).drop("validation_reason")
        logger.info("Validation of Products completed")
        logger.info(f"Accepted Records:{valid_products_df.count()}")
        logger.info(f"Rejected Records:{invalid_products_df.count()}")
        return valid_products_df,invalid_products_df
    except Exception as e:
        logger.exception(f"Error Validating Products:{str(e)}")
        raise
def validate_customers(customers_df):
    logger.info("Validate Customers Started")
    required_columns=["customer_id","customer_name","email","mobile","city","registration_date","customer_type"]
    try:
        missing_columns=[column for column in required_columns
                         if column not in customers_df]
        if missing_columns:
            raise ValueError(f"Missing Required Columns:{missing_columns}")

        df=customers_df
        string_cols=["customer_id","customer_name","email","mobile","city","customer_type"]
        for column in string_cols:
            df = df.withColumn(column,trim(col(column)))
        df = df.withColumn("registration_date",to_date(col("registration_date"), "yyyy-MM-dd"))
        validation_reason = (when( col("customer_id").isNull(), lit("Customer ID is Null") ).when( col("customer_name").isNull() | (length(col("customer_name")) == 0), lit("Customer Name is Null/Empty") ).when( col("email").isNull() | (length(col("email")) == 0), lit("Email is Null/Empty") ).when( ~col("email").rlike( r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$" ), lit("Email is Invalid") ).when( col("mobile").isNull() | (length(col("mobile")) != 10) | (~col("mobile").rlike(r"^[0-9]{10}$")), lit("Mobile is Invalid") ).when( col("city").isNull() | (length(col("city")) == 0), lit("City is Null/Empty") ).when( col("registration_date").isNull(), lit("Registration Date is Null") ).when( col("customer_type").isNull() | (length(col("customer_type")) == 0), lit("Customer Type is Null/Empty") ).otherwise( lit(None) ) )      
        df = df.withColumn( "validation_reason", validation_reason )
        valid_customer_df = ( df .filter(col("validation_reason").isNull()) .drop("validation_reason") ) 
        invalid_customer_df = ( df .filter(col("validation_reason").isNotNull()) )
        accepted_count = valid_customer_df.count() 
        rejected_count = invalid_customer_df.count() 
        logger.info( f"Customer validation completed. " f"Accepted Records: {accepted_count}, " f"Rejected Records: {rejected_count}" ) 
        return valid_customer_df, invalid_customer_df
    except Exception as e:
        logger.exception("Error in validating Customers:",str(e))
        raise e
def validate_inventory(inventory_df):
    logger.info("Inventroy Validation Started")
    required_columns=["inventory_id","product_id","store_id","available_quantity","reorder_level","last_updated"]
    try:
        missing_columns=[column for column in required_columns
                         if column not in inventory_df]
        if missing_columns:
            raise ValueError(f"Missing Required Columns :{missing_columns}")
        string_columns=["inventory_id","product_id","store_id"]
        numeric_columns=["available_quantity","reorder_level"]
        df=inventory_df
        for column in string_columns:
            df=df.withColumn(column,trim(col(column)))
        for column in numeric_columns:
            df=df.withColumn(column,to_number(col(column)))
        df=df.withColumn("last_updated",to_date(col("last_updated"),"yyyy-MM-dd"))
        validation_reason=(when(col("inventory_id").isNull(),lit("Inventory ID is Null")).when(col("product_id").isNull(),lit("Product ID is Null")).when(col("store_id").isNull(),lit("Store ID is Null")).when(col("available_quantity").isNull() or col("available_quantity")<0,lit("Available Quantity is Invalid")).when(col("last_updated").isNull(),lit("Last Updated Date is Null")).when(col("reorder_level").isNull() or col("reorder_level")<=0,lit("Reorder Level is Invalid")))
        df = df.withColumn( "validation_reason", validation_reason )
        invalid_inventory_df=df.filter(col("validation_reason").isNotNull())
        valid_inventory_df=df.filter(col("validation_reason").isNull()).drop("validation_reason")
        logger.info("Validation of Inventory Completed")
        logger.info(f"Accepted Records:{valid_inventory_df.count()}")
        logger.info(f"Rejected Records:{invalid_inventory_df.count()}")

    except Exception as e:
        logger.exception(f"Error in validating inventory:{str(e)}")
def validate_sales(sales_df):
    logger.info("Sales Validation Started")
    required_columns=["sale_id","product_id","store_id","customer_id","quantity","sale_date","discount_percentage","payment_mode"]
    try:
        missing_columns=[column for column in required_columns
                         if column not in sales_df]
        if missing_columns:
            raise ValueError(f"Missing Required Columns :{missing_columns}")
        string_columns=["sale_id","product_id","store_id","customer_id","payment_mode"]
        numeric_columns=["quantity","discount_percentage"]
        df=sales_df
        for column in string_columns:
            df=df.withColumn(column,trim(col(column)))
        for column in numeric_columns:
            df=df.withColumn(column,to_number(col(column)))
        df=df.withColumn("sale_date",to_date(col("last_updated"),"yyyy-MM-dd"))
        validation_reason=(when(col("sale_id").isNull(),lit("Sale ID is Null")).when(col("product_id").isNull(),lit("Product ID is Null")).when(col("store_id").isNull(),lit("Store ID is Null")).when(col("quantity").isNull() or col("quantity")<0,lit("Quantity is Invalid")).when(col("sale_date").isNull(),lit("Sale Date is Null")).when(col("customer_id").isNull(),lit("Customer ID is Invalid")).when(col("discount_percentage").isNull() | col("discount_percentage")<0,lit("Discount Percentage is Invalid")).when(col("payment_mode").isNull(),lit("Payment Mode is Null")))
        df = df.withColumn( "validation_reason", validation_reason )
        invalid_sale_df=df.filter(col("validation_reason").isNotNull())
        valid_sale_df=df.filter(col("validation_reason").isNull()).drop("validation_reason")
        logger.info("Validation of Sale Completed")
        logger.info(f"Accepted Records:{valid_sale_df.count()}")
        logger.info(f"Rejected Records:{invalid_sale_df.count()}")

    except Exception as e:
        logger.exception(f"Error in validating Sale:{str(e)}")            
def validate_stores(stores_df):
    logger.info("Stores Validation Started")
    required_columns=["store_id","store_name","city","state","store_manager","opening_date","status"]
    try:
        missing_columns=[column for column in required_columns
                         if column not in stores_df]
        if missing_columns:
            raise ValueError(f"Missing Required Columns :{missing_columns}")
        string_columns=["store_id","store_name","city","state","store_manager","status"]
        df=stores_df
        for column in string_columns:
            df=df.withColumn(column,trim(col(column)))
        df=df.withColumn("opening_date",to_date(col("opening_date"),"yyyy-MM-dd"))
        validation_reason=(when(col("store_id").isNull(),lit("Store ID is Null")).when(col("store_name").isNull(),lit("Store Name is Null")).when(col("city").isNull(),lit("City is Null")).when(col("state").isNull(),lit("State is Invalid")).when(col("store_manager").isNull(),lit("Store Manager is Null")).when(col("opening_date").isNull(),lit("Opening Date is Invalid")).when(col("status").isNull(),lit("State is Invalid")))
        df = df.withColumn( "validation_reason", validation_reason )
        invalid_store_df=df.filter(col("validation_reason").isNotNull())
        valid_store_df=df.filter(col("validation_reason").isNull()).drop("validation_reason")
        logger.info("Validation of Store Completed")
        logger.info(f"Accepted Records:{valid_store_df.count()}")
        logger.info(f"Rejected Records:{invalid_store_df.count()}")

    except Exception as e:
        logger.exception(f"Error in validating Store:{str(e)}")      
            
            