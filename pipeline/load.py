from database.database_connection import sessionLocal

from models.customers import Customer
from models.inventory import Inventory
from models.products import Product
from models.sales import Sale
from models.stores import Store

from logger_config import logger


def load_product(product_df):

    logger.info("Loading products")

    session = sessionLocal()

    try:

        for row in product_df.toLocalIterator():

            product_id = str(row["product_id"])

            existing = session.get(
                Product,
                product_id
            )

            if existing is None:

                product = Product(
                    product_id=product_id,
                    product_name=row["product_name"],
                    category=row["category"],
                    brand=str(row["brand"]),
                    unit_price=float(row["unit_price"]),
                    supplier_id=str(row["supplier_id"]),
                    status=str(row["status"])
                )

                session.add(product)

        session.commit()

        logger.info("Products loaded successfully")

    except Exception as e:

        session.rollback()

        logger.error(
            f"Product loading failed: {e}"
        )

        raise

    finally:

        session.close()


def load_customer(customer_df):

    logger.info("Loading customers")

    session = sessionLocal()

    try:

        for row in customer_df.toLocalIterator():

            customer_id = str(row["customer_id"])

            existing = session.get(
                Customer,
                customer_id
            )

            if existing is None:

                customer = Customer(
                    customer_id=customer_id,
                    customer_name=row["customer_name"],
                    email=str(row["email"]),
                    mobile=str(row["mobile"]),
                    city=str(row["city"]),
                    registration_date=row["registration_date"],
                    customer_type=str(row["customer_type"])
                )

                session.add(customer)

        session.commit()

        logger.info("Customers loaded successfully")

    except Exception as e:

        session.rollback()

        logger.error(
            f"Customer loading failed: {e}"
        )

        raise

    finally:

        session.close()

def load_store(store_df):

    logger.info("Loading stores")

    session = sessionLocal()

    try:

        for row in store_df.toLocalIterator():

            store_id = str(row["store_id"])

            existing = session.get(
                Store,
                store_id
            )

            if existing is None:

                store = Store(
                    store_id=store_id,
                    store_name=row["store_name"],
                    city=str(row["city"]),
                    state=str(row["state"]),
                    store_manager=str(row["store_manager"]),
                    opening_date=row["opening_date"],
                    status=str(row["status"])
                )

                session.add(store)

        session.commit()

        logger.info("Stores loaded successfully")

    except Exception as e:

        session.rollback()

        logger.error(
            f"Store loading failed: {e}"
        )

        raise

    finally:

        session.close()


def load_inventory(inventory_df):

    logger.info("Loading inventory")

    session = sessionLocal()

    try:

        for row in inventory_df.toLocalIterator():

            inventory_id = str(row["inventory_id"])

            existing = session.get(
                Inventory,
                inventory_id
            )

            if existing is None:

                inventory = Inventory(
                    inventory_id=inventory_id,
                    product_id=str(row["product_id"]),
                    store_id=str(row["store_id"]),
                    available_quantity=int(row["available_quantity"]),
                    reorder_level=int(row["reorder_level"]),
                    last_updated=row["last_updated"]
                )

                session.add(inventory)

        session.commit()

        logger.info("Inventory loaded successfully")

    except Exception as e:

        session.rollback()

        logger.error(
            f"Inventory loading failed: {e}"
        )

        raise

    finally:

        session.close()


def load_sales(sales_df):

    logger.info("Loading sales")

    session = sessionLocal()

    try:

        for row in sales_df.toLocalIterator():

            sale_id = str(row["sale_id"])

            existing = session.get(
                Sale,
                sale_id
            )

            if existing is None:

                sale = Sale(
                    sale_id=sale_id,
                    product_id=str(row["product_id"]),
                    store_id=str(row["store_id"]),
                    customer_id=str(row["customer_id"]),
                    quantity=int(row["quantity"]),
                    sale_date=row["sale_date"],
                    discount_percentage=int(
                        row["discount_percentage"]
                    ),
                    payment_mode=str(row["payment_mode"])
                )

                session.add(sale)

        session.commit()

        logger.info("Sales loaded successfully")

    except Exception as e:

        session.rollback()

        logger.error(
            f"Sales loading failed: {e}"
        )

        raise

    finally:

        session.close()