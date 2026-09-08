import os
RAW_FOLDER="data/raw"
REJECTED_FOLDER="data/rejected"

os.makedirs(
    REJECTED_FOLDER,
    exist_ok=True
)

DB_HOST="localhost"
DB_USERNAME="root"
DB_PASSWORD="root"
DB_PORT=3306
DB_NAME="supply_chain_analytics"

CUSTOMER_FILE="data/raw/customers.csv"
SALE_FILE="data/raw/sales.csv"
INVENTORY_FILE="data/raw/inventory.csv"
PRODUCT_FILE="data/raw/products.csv"
STORE_FILE="data/raw/stores.csv"


REJECTED_CUSTOMERS_FILE=("data/rejected/rejected_customers.csv")
REJECTED_SALES_FILE=("data/rejected/rejected_sales.csv")
REJECTED_PRODUCTS_FILE=("data/rejected/rejected_products.csv")
REJECTED_STORES_FILE=("data/rejected/rejected_stores.csv")
REJECTED_INVENTORY_FILE=("data/rejected/rejected_inventory.csv")