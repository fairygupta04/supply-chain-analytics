from pipeline.extract import (
    extract_products,
    extract_customers,
    extract_inventory,
    extract_sales,
    extract_stores
)

from pipeline.validate import (
    validate_products,
    validate_customers,
    validate_inventory,
    validate_sales,
    validate_stores
)

from pipeline.transform import (
    transform_products,
    transform_customers,
    transform_inventory,
    transform_sales,
    transform_stores
)

from pipeline.load import (
    load_product,
    load_customer,
    load_inventory,
    load_sales,
    load_store
)

from logger_config import logger


def run_pipeline():

    logger.info("Pipeline Started")

    try:


        logger.info("Starting EXTRACT phase")

        product_df = extract_products()
        customer_df = extract_customers()
        inventory_df = extract_inventory()
        sales_df = extract_sales()
        store_df = extract_stores()

        logger.info("Data extraction completed")

        logger.info("Starting VALIDATE phase")

        valid_product_df, rejected_product_df = validate_products(
            product_df
        )

        valid_customer_df, rejected_customer_df = validate_customers(
            customer_df
        )

        valid_store_df, rejected_store_df = validate_stores(
            store_df
        )

        valid_inventory_df, rejected_inventory_df = validate_inventory(
            inventory_df,
            valid_product_df,
            valid_store_df
        )

        # IMPORTANT:
        # Sales validation needs Product, Store and Customer
        # DataFrames for foreign-key validation.

        valid_sales_df, rejected_sales_df = validate_sales(
            sales_df,
            valid_product_df,
            valid_store_df,
            valid_customer_df
        )

        logger.info("Data validation completed")


        logger.info("Starting TRANSFORM phase")

        transformed_product_df = transform_products(
            valid_product_df
        )

        transformed_customer_df = transform_customers(
            valid_customer_df
        )

        transformed_store_df = transform_stores(
            valid_store_df
        )

        transformed_inventory_df = transform_inventory(
            valid_inventory_df
        )

        transformed_sales_df = transform_sales(
            valid_sales_df
        )

        logger.info("Data transformation completed")

        logger.info("Starting LOAD phase")

        load_product(
            transformed_product_df
        )

        load_customer(
            transformed_customer_df
        )

        load_store(
            transformed_store_df
        )

        load_inventory(
            transformed_inventory_df
        )

        load_sales(
            transformed_sales_df
        )

        logger.info("Data loading completed")

        return {
            "status": "SUCCESS",
            "message": "Pipeline completed successfully"
        }


    except Exception as e:

        logger.exception(
            f"Pipeline failed: {str(e)}"
        )

        return {
            "status": "FAILED",
            "message": str(e)
        }
if __name__ == "__main__":
    result = run_pipeline()
    print("\nPipeline Result:")
    print(result)    