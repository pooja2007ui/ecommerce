"""
Synthetic E-Commerce Data Generator
Generates realistic multi-table relational data as well as a consolidated transaction dataset.
"""

import os
import random
from datetime import datetime, timedelta
import pandas as pd
import numpy as np

def generate_ecommerce_data(output_dir="data", num_customers=1000, num_products=150, num_orders=5000):
    os.makedirs(output_dir, exist_ok=True)
    random.seed(42)
    np.random.seed(42)

    print(f"Generating synthetic e-commerce data in '{output_dir}'...")

    # 1. Product Categories & Products
    categories = {
        "Electronics": ["Smartphone", "Laptop", "Wireless Earbuds", "Smartwatch", "4K Monitor", "Gaming Mouse", "Bluetooth Speaker"],
        "Clothing": ["Denim Jacket", "Graphic T-Shirt", "Running Shoes", "Hoodie", "Slim Fit Chinos", "Yoga Pants", "Formal Shirt"],
        "Home & Kitchen": ["Air Fryer", "Espresso Machine", "Ceramic Cookware", "Blender", "Robotic Vacuum", "Stainless Steel Kettle"],
        "Books": ["Sci-Fi Novel", "Data Engineering Handbook", "Psychology of Money", "Cookbook", "Design Systems Guide"],
        "Beauty & Personal Care": ["Facial Cleanser", "Sunscreen SPF 50", "Hair Dryer", "Moisturizing Cream", "Organic Shampoo"],
        "Sports & Outdoors": ["Yoga Mat", "Dumbbell Set", "Camping Tent", "Trekking Backpack", "Hydration Water Bottle"]
    }

    products = []
    pid = 101
    for cat, items in categories.items():
        for item in items:
            cost = round(random.uniform(5.0, 300.0), 2)
            margin = random.uniform(1.2, 2.0)
            price = round(cost * margin, 2)
            products.append({
                "product_id": f"PROD_{pid}",
                "product_name": item,
                "category": cat,
                "cost_price": cost,
                "unit_price": price
            })
            pid += 1

    df_products = pd.DataFrame(products)
    df_products.to_csv(os.path.join(output_dir, "products.csv"), index=False)
    print(f"- Saved {len(df_products)} products to products.csv")

    # 2. Customers
    cities_countries = [
        ("New York", "USA"), ("San Francisco", "USA"), ("Chicago", "USA"), ("Austin", "USA"),
        ("London", "UK"), ("Manchester", "UK"),
        ("Toronto", "Canada"), ("Vancouver", "Canada"),
        ("Berlin", "Germany"), ("Munich", "Germany"),
        ("Paris", "France"),
        ("Sydney", "Australia"), ("Melbourne", "Australia"),
        ("Bangalore", "India"), ("Mumbai", "India")
    ]
    
    first_names = ["James", "Emma", "Liam", "Olivia", "Noah", "Ava", "Lucas", "Mia", "Oliver", "Sophia",
                   "Alexander", "Charlotte", "Ethan", "Amelia", "Daniel", "Harper", "Matthew", "Evelyn", "Aarav", "Priya"]
    last_names = ["Smith", "Johnson", "Brown", "Taylor", "Miller", "Wilson", "Davis", "Martinez", "Anderson", "Thomas",
                  "Jackson", "White", "Harris", "Martin", "Patel", "Sharma", "Clark", "Lewis", "Lee", "Walker"]

    start_date = datetime(2025, 1, 1)
    end_date = datetime(2026, 10, 31)
    date_range_days = (end_date - start_date).days

    customers = []
    for cid in range(1, num_customers + 1):
        fn = random.choice(first_names)
        ln = random.choice(last_names)
        city, country = random.choice(cities_countries)
        reg_days = random.randint(0, date_range_days - 60)
        signup_dt = start_date + timedelta(days=reg_days)
        tier = np.random.choice(["Bronze", "Silver", "Gold", "Platinum"], p=[0.50, 0.30, 0.15, 0.05])
        
        customers.append({
            "customer_id": f"CUST_{cid:05d}",
            "customer_name": f"{fn} {ln}",
            "email": f"{fn.lower()}.{ln.lower()}{cid}@example.com",
            "city": city,
            "country": country,
            "signup_date": signup_dt.strftime("%Y-%m-%d"),
            "customer_tier": tier
        })

    df_customers = pd.DataFrame(customers)
    df_customers.to_csv(os.path.join(output_dir, "customers.csv"), index=False)
    print(f"- Saved {len(df_customers)} customers to customers.csv")

    # 3. Orders and Order Items
    order_statuses = ["Completed", "Shipped", "Processing", "Cancelled", "Refunded"]
    payment_methods = ["Credit Card", "Debit Card", "PayPal", "Apple Pay", "Net Banking"]

    orders = []
    order_items = []
    transactions_denormalized = []

    order_item_id = 1
    for oid in range(1, num_orders + 1):
        cust = random.choice(customers)
        cust_signup = datetime.strptime(cust["signup_date"], "%Y-%m-%d")
        
        # Order date must be on or after signup
        days_after_signup = random.randint(0, max(1, (end_date - cust_signup).days))
        order_dt = cust_signup + timedelta(days=days_after_signup, hours=random.randint(0, 23), minutes=random.randint(0, 59))
        
        status = np.random.choice(order_statuses, p=[0.75, 0.12, 0.05, 0.05, 0.03])
        pay_method = random.choice(payment_methods)
        shipping_cost = round(random.choice([0.0, 4.99, 9.99, 14.99]), 2)

        # Number of line items per order (1 to 5)
        num_items = np.random.choice([1, 2, 3, 4, 5], p=[0.45, 0.30, 0.15, 0.07, 0.03])
        chosen_prods = random.sample(products, num_items)

        order_subtotal = 0.0

        for prod in chosen_prods:
            qty = np.random.choice([1, 2, 3, 4, 5], p=[0.60, 0.22, 0.10, 0.05, 0.03])
            # Occasional discount
            discount = round(np.random.choice([0.0, 0.05, 0.10, 0.15, 0.20], p=[0.65, 0.15, 0.10, 0.05, 0.05]), 2)
            item_price = prod["unit_price"]
            line_total = round(qty * item_price * (1 - discount), 2)
            order_subtotal += line_total

            order_items.append({
                "order_item_id": f"ITEM_{order_item_id:06d}",
                "order_id": f"ORD_{oid:06d}",
                "product_id": prod["product_id"],
                "quantity": qty,
                "unit_price": item_price,
                "discount_percent": discount,
                "line_total": line_total
            })

            # Single denormalized row
            transactions_denormalized.append({
                "order_id": f"ORD_{oid:06d}",
                "customer_id": cust["customer_id"],
                "customer_name": cust["customer_name"],
                "country": cust["country"],
                "city": cust["city"],
                "customer_tier": cust["customer_tier"],
                "order_date": order_dt.strftime("%Y-%m-%d %H:%M:%S"),
                "product_id": prod["product_id"],
                "product_name": prod["product_name"],
                "category": prod["category"],
                "quantity": qty,
                "unit_price": item_price,
                "discount_percent": discount,
                "line_total": line_total,
                "order_status": status,
                "payment_method": pay_method,
                "shipping_cost": shipping_cost
            })
            order_item_id += 1

        orders.append({
            "order_id": f"ORD_{oid:06d}",
            "customer_id": cust["customer_id"],
            "order_date": order_dt.strftime("%Y-%m-%d %H:%M:%S"),
            "order_status": status,
            "payment_method": pay_method,
            "shipping_cost": shipping_cost,
            "subtotal": round(order_subtotal, 2),
            "total_amount": round(order_subtotal + shipping_cost, 2)
        })

    df_orders = pd.DataFrame(orders)
    df_orders.to_csv(os.path.join(output_dir, "orders.csv"), index=False)
    print(f"- Saved {len(df_orders)} orders to orders.csv")

    df_order_items = pd.DataFrame(order_items)
    df_order_items.to_csv(os.path.join(output_dir, "order_items.csv"), index=False)
    print(f"- Saved {len(df_order_items)} line items to order_items.csv")

    df_trans = pd.DataFrame(transactions_denormalized)
    df_trans.to_csv(os.path.join(output_dir, "ecommerce_transactions.csv"), index=False)
    print(f"- Saved {len(df_trans)} records to ecommerce_transactions.csv (denormalized)")

    print("\nDataset generation completed successfully!")

if __name__ == "__main__":
    generate_ecommerce_data(
        output_dir="C:/Users/Poojitha/.gemini/antigravity/scratch/ecommerce_sales_analysis_pyspark/data",
        num_customers=1500,
        num_products=120,
        num_orders=8000
    )
