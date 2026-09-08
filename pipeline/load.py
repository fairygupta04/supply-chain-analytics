from database.database_connection import sessionLocal

from models.product import Product
from models.customer import Customer
from models.warehouse import Warehouse
from models.order import Order
from models.shipment import Shipment

from logging_config import pipeline_logger, database_error_logger

from pyspark.sql import DataFrame


def load_products(products_df: DataFrame):
    session = sessionLocal()

    try:
        for row in products_df.toLocalIterator():

            product_id = str(row["product_id"])

            existing_product = session.get(Product, product_id)

            if existing_product is None:

                product = Product(
                    product_id=product_id,
                    product_name=row["product_name"],
                    category=row["category"],
                    brand=str(row["brand"]),
                    unit_price=float(row["unit_price"]),
                    supplier_name=str(row["supplier_name"]),
                    reorder_level=int(row["reorder_level"]),
                    status=str(row["status"])
                )

                session.add(product)

        session.commit()

        pipeline_logger.info("Products loaded successfully")

    except Exception as e:
        session.rollback()

        database_error_logger.error(
            f"Product Loading Error: {e}"
        )

        pipeline_logger.exception(
            f"Product Loading Error: {e}"
        )

        raise

    finally:
        session.close()


def load_customers(customers_df: DataFrame):
    session = sessionLocal()

    try:
        for row in customers_df.toLocalIterator():

            customer_id = str(row["customer_id"])

            existing_customer = session.get(
                Customer,
                customer_id
            )

            if existing_customer is None:

                customer = Customer(
                    customer_id=customer_id,
                    customer_name=row["customer_name"],
                    email=str(row["email"]),
                    mobile=str(row["mobile"]),
                    city=str(row["city"]),
                    state=str(row["state"]),
                    registration_date=row["registration_date"],
                    customer_type=str(row["customer_type"])
                )

                session.add(customer)

        session.commit()

        pipeline_logger.info(
            "Customers loaded successfully"
        )

    except Exception as e:
        session.rollback()

        database_error_logger.error(
            f"Customer Loading Error: {e}"
        )

        pipeline_logger.exception(
            f"Customer Loading Error: {e}"
        )

        raise

    finally:
        session.close()


def load_warehouses(warehouses_df: DataFrame):
    session = sessionLocal()

    try:
        for row in warehouses_df.toLocalIterator():

            warehouse_id = str(row["warehouse_id"])

            existing_warehouse = session.get(
                Warehouse,
                warehouse_id
            )

            if existing_warehouse is None:

                warehouse = Warehouse(
                    warehouse_id=warehouse_id,
                    warehouse_name=row["warehouse_name"],
                    city=str(row["city"]),
                    state=str(row["state"]),
                    manager_name=str(row["manager_name"]),
                    capacity=int(row["capacity"]),
                    available_capacity=int(
                        row["available_capacity"]
                    ),
                    status=str(row["status"])
                )

                session.add(warehouse)

        session.commit()

        pipeline_logger.info(
            "Warehouses loaded successfully"
        )

    except Exception as e:
        session.rollback()

        database_error_logger.error(
            f"Warehouse Loading Error: {e}"
        )

        pipeline_logger.exception(
            f"Warehouse Loading Error: {e}"
        )

        raise

    finally:
        session.close()


def load_orders(orders_df: DataFrame):
    session = sessionLocal()

    try:
        for row in orders_df.toLocalIterator():

            order_id = str(row["order_id"])

            existing_order = session.get(
                Order,
                order_id
            )

            if existing_order is None:

                order = Order(
                    order_id=order_id,
                    customer_id=str(row["customer_id"]),
                    product_id=str(row["product_id"]),
                    warehouse_id=str(row["warehouse_id"]),
                    quantity=int(row["quantity"]),
                    unit_price=float(row["unit_price"]),
                    discount_percentage=float(
                        row["discount_percentage"]
                    ),
                    order_date=row["order_date"],
                    payment_mode=str(row["payment_mode"]),
                    order_status=str(row["order_status"]),
                    gross_amount=float(row["gross_amount"]),
                    discount_amount=float(
                        row["discount_amount"]
                    ),
                    net_amount=float(row["net_amount"]),
                    order_value_category=str(
                        row["order_value_category"]
                    )
                )

                session.add(order)

        session.commit()

        pipeline_logger.info(
            "Orders loaded successfully"
        )

    except Exception as e:
        session.rollback()

        database_error_logger.error(
            f"Order Loading Error: {e}"
        )

        pipeline_logger.exception(
            f"Order Loading Error: {e}"
        )

        raise

    finally:
        session.close()


def load_shipments(shipments_df: DataFrame):
    session = sessionLocal()

    try:
        for row in shipments_df.toLocalIterator():

            shipment_id = str(row["shipment_id"])

            existing_shipment = session.get(
                Shipment,
                shipment_id
            )

            if existing_shipment is None:

                actual_date = row["actual_delivery_date"]

                delivery_days_val = row["delivery_days"]

                shipment = Shipment(
                    shipment_id=shipment_id,
                    order_id=str(row["order_id"]),
                    logistics_partner=str(
                        row["logistics_partner"]
                    ),
                    shipment_date=row["shipment_date"],
                    expected_delivery_date=row[
                        "expected_delivery_date"
                    ],
                    actual_delivery_date=actual_date,
                    delivery_status=str(
                        row["delivery_status"]
                    ),
                    shipping_cost=float(
                        row["shipping_cost"]
                    ),
                    destination_city=str(
                        row["destination_city"]
                    ),
                    delivery_days=(
                        int(delivery_days_val)
                        if delivery_days_val is not None
                        else None
                    ),
                    delivery_performance=str(
                        row["delivery_performance"]
                    )
                )

                session.add(shipment)

        session.commit()

        pipeline_logger.info(
            "Shipments loaded successfully"
        )

    except Exception as e:
        session.rollback()

        database_error_logger.error(
            f"Shipment Loading Error: {e}"
        )

        pipeline_logger.exception(
            f"Shipment Loading Error: {e}"
        )

        raise

    finally:
        session.close()