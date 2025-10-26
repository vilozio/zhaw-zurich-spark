"""
Script to insert test data into Postgres to test the streaming pipeline.
This should be run inside the postgres container or connected to postgres.
"""

import psycopg2
from datetime import datetime, timedelta
import random
import time

# Connection parameters
conn_params = {
    'host': 'postgres',
    'port': 5432,
    'database': 'postgres',
    'user': 'postgres',
    'password': 'postgres'
}

# Sample data
names = ['Alice Johnson', 'Bob Smith', 'Charlie Brown', 'Diana Prince', 'Eve Davis']
membership_levels = ['free', 'pro', 'elite']
products = ['Laptop', 'Mouse', 'Keyboard', 'Monitor', 'Headphones']
addresses = [
    '123 Main St, Springfield, IL',
    '456 Oak Ave, Portland, OR',
    '789 Pine Rd, Austin, TX',
    '321 Elm St, Seattle, WA',
    '654 Maple Dr, Boston, MA'
]


def insert_customers(conn, num_customers=5):
    """Insert sample customers"""
    cursor = conn.cursor()
    customer_ids = []
    
    for i in range(num_customers):
        name = random.choice(names) + f" {i}"
        birthday = datetime.now().date() - timedelta(days=random.randint(7000, 25000))
        membership = random.choice(membership_levels)
        address = random.choice(addresses)
        
        cursor.execute("""
            INSERT INTO customers (name, birthday, membership_level, shipping_address)
            VALUES (%s, %s, %s, %s)
            RETURNING customer_id
        """, (name, birthday, membership, address))
        
        customer_id = cursor.fetchone()[0]
        customer_ids.append(customer_id)
        print(f"Inserted customer: {customer_id} - {name}")
    
    conn.commit()
    cursor.close()
    return customer_ids


def insert_orders(conn, customer_ids, num_orders=10):
    """Insert sample orders for the given customers"""
    cursor = conn.cursor()
    
    for _ in range(num_orders):
        customer_id = random.choice(customer_ids)
        product = random.choice(products)
        cost = round(random.uniform(10.0, 500.0), 2)
        description = f"Order for {product}"
        credit_card = f"{random.randint(1000, 9999)}{random.randint(1000, 9999)}{random.randint(1000, 9999)}{random.randint(1000, 9999)}"
        
        cursor.execute("""
            INSERT INTO orders (customer_id, product, cost, description, credit_card_number)
            VALUES (%s, %s, %s, %s, %s)
            RETURNING order_id
        """, (customer_id, product, cost, description, credit_card))
        
        order_id = cursor.fetchone()[0]
        print(f"Inserted order: {order_id} - Customer {customer_id} ordered {product} for ${cost}")
    
    conn.commit()
    cursor.close()


def continuous_insert(conn, customer_ids, interval=5):
    """Continuously insert orders at a given interval (in seconds)"""
    print(f"\nStarting continuous order insertion (every {interval} seconds)...")
    print("Press Ctrl+C to stop\n")
    
    try:
        while True:
            insert_orders(conn, customer_ids, num_orders=random.randint(1, 3))
            time.sleep(interval)
    except KeyboardInterrupt:
        print("\n\nStopped continuous insertion")


def main():
    print("Connecting to Postgres...")
    
    try:
        conn = psycopg2.connect(**conn_params)
        print("Connected successfully!\n")
        
        # Insert initial customers
        print("=== Inserting Customers ===")
        customer_ids = insert_customers(conn, num_customers=5)
        time.sleep(2)  # Give Debezium time to process
        
        # Insert initial batch of orders
        print("\n=== Inserting Initial Orders ===")
        insert_orders(conn, customer_ids, num_orders=15)
        time.sleep(2)
        
        # Ask if user wants continuous insertion
        print("\n" + "="*60)
        response = input("Do you want to continuously insert orders? (y/n): ")
        if response.lower() == 'y':
            continuous_insert(conn, customer_ids, interval=5)
        
        conn.close()
        print("\nConnection closed.")
        
    except (psycopg2.Error, KeyboardInterrupt) as e:
        print(f"Error: {e}")


if __name__ == "__main__":
    main()
