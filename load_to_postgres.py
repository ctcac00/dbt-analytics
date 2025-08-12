#!/usr/bin/env python3
import argparse
import os
import sys
import psycopg2

DDL = [
    "CREATE SCHEMA IF NOT EXISTS jaffle_shop",
    "CREATE SCHEMA IF NOT EXISTS stripe",
    """
    CREATE TABLE IF NOT EXISTS jaffle_shop.customers (
        id INTEGER,
        first_name TEXT,
        last_name TEXT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS jaffle_shop.orders (
        id INTEGER,
        user_id INTEGER,
        order_date DATE,
        status TEXT,
        _etl_loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS stripe.payment (
        id INTEGER,
        orderid INTEGER,
        paymentmethod TEXT,
        status TEXT,
        amount INTEGER,
        created DATE,
        _batched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """,
]

COPY_SQL = {
    "jaffle_shop.customers": """
        COPY jaffle_shop.customers (id, first_name, last_name)
        FROM STDIN WITH (FORMAT csv, HEADER true)
    """,
    "jaffle_shop.orders": """
        COPY jaffle_shop.orders (id, user_id, order_date, status)
        FROM STDIN WITH (FORMAT csv, HEADER true)
    """,
    "stripe.payment": """
        COPY stripe.payment (id, orderid, paymentmethod, status, amount, created)
        FROM STDIN WITH (FORMAT csv, HEADER true)
    """,
}


def parse_args():
    p = argparse.ArgumentParser(description="Load Jaffle Shop CSVs into PostgreSQL.")
    p.add_argument(
        "--conn",
        required=True,
        help="Postgres connection string (postgresql://user:pass@host:port/dbname[?params])",
    )
    p.add_argument(
        "--customers", default="jaffle_shop_customers.csv", help="Path to customers CSV"
    )
    p.add_argument(
        "--orders", default="jaffle_shop_orders.csv", help="Path to orders CSV"
    )
    p.add_argument(
        "--payments", default="stripe_payments.csv", help="Path to payments CSV"
    )
    p.add_argument(
        "--no-truncate", action="store_true", help="Do not truncate before load"
    )
    return p.parse_args()


def ensure_files(paths):
    missing = [p for p in paths if not os.path.isfile(p)]
    if missing:
        sys.exit(f"Missing CSV file(s): {', '.join(missing)}")


def main():
    args = parse_args()
    ensure_files([args.customers, args.orders, args.payments])

    try:
        conn = psycopg2.connect(args.conn)
        conn.autocommit = False
    except Exception as e:
        sys.exit(f"Failed to connect to Postgres: {e}")

    try:
        with conn.cursor() as cur:
            # Create schemas and tables
            for stmt in DDL:
                cur.execute(stmt)

            # Optional truncate
            if not args.no_truncate:
                cur.execute("TRUNCATE jaffle_shop.customers")
                cur.execute("TRUNCATE jaffle_shop.orders")
                cur.execute("TRUNCATE stripe.payment")

            # Load customers
            with open(args.customers, "r", encoding="utf-8") as f:
                cur.copy_expert(COPY_SQL["jaffle_shop.customers"], f)

            # Load orders
            with open(args.orders, "r", encoding="utf-8") as f:
                cur.copy_expert(COPY_SQL["jaffle_shop.orders"], f)

            # Load payments
            with open(args.payments, "r", encoding="utf-8") as f:
                cur.copy_expert(COPY_SQL["stripe.payment"], f)

            # Commit
            conn.commit()

            # Row counts
            cur.execute("SELECT COUNT(*) FROM jaffle_shop.customers")
            customers_count = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM jaffle_shop.orders")
            orders_count = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM stripe.payment")
            payments_count = cur.fetchone()[0]

            print("Load complete:")
            print(f"- jaffle_shop.customers: {customers_count} rows")
            print(f"- jaffle_shop.orders:    {orders_count} rows")
            print(f"- stripe.payment:        {payments_count} rows")

    except Exception as e:
        conn.rollback()
        sys.exit(f"Load failed, rolled back: {e}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
