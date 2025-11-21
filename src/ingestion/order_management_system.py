import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
import string
import os


# Set seed for reproducibility
np.random.seed(42)
random.seed(42)

today_str=datetime.today().strftime("%Y-%m-%d")
ORDER_PATH=f"./data/landing/orders/"
CUSTOMER_PATH=f"./data/landing/customers/"
ORDER_ITEMS_PATH=f"./data/landing/orders_item/"


os.makedirs(ORDER_PATH, exist_ok=True)
os.makedirs(CUSTOMER_PATH, exist_ok=True)
os.makedirs(ORDER_ITEMS_PATH, exist_ok=True)

# Generate realistic messy orders data
def generate_orders_data(num_records=5000):
    """
    Simulates OLTP database dumps with realistic messiness:
    - Inconsistent date formats
    - Missing values
    - Duplicate records
    - Data quality issues
    """
    
    start_date = datetime(2023, 1, 1)
    end_date = datetime(2024, 11, 15)
    
    orders = []
    
    for i in range(num_records):
        order_id = f"ORD{str(i+1000).zfill(8)}"
        
        # Introduce 2% duplicate orders (real-world issue)
        if random.random() < 0.02 and i > 0:
            order_id = orders[-1]['order_id']
        
        # Random timestamp
        order_date = start_date + timedelta(
            seconds=random.randint(0, int((end_date - start_date).total_seconds()))
        )
        
        # Inconsistent date formats (common in real systems)
        date_formats = [
            order_date.strftime('%Y-%m-%d %H:%M:%S'),
            order_date.strftime('%m/%d/%Y %H:%M'),
            order_date.strftime('%d-%m-%Y %H:%M:%S'),
            order_date.isoformat(),
        ]
        order_date_str = random.choice(date_formats)
        
        customer_id = f"CUST{random.randint(1, 2000):06d}"
        
        # Introduce missing customer IDs (3%)
        if random.random() < 0.03:
            customer_id = None
        
        # Order status with typos and inconsistencies
        statuses = ['completed', 'Completed', 'COMPLETED', 'pending', 'Pending', 
                   'cancelled', 'Cancelled', 'shipped', 'returned', 'refunded', 
                   'processing', 'failed', '']
        order_status = random.choice(statuses)
        
        # Random amounts
        subtotal = round(random.uniform(10, 2000), 2)
        tax = round(subtotal * random.uniform(0.05, 0.15), 2)
        shipping = round(random.uniform(0, 25), 2) if random.random() > 0.1 else 0
        discount = round(random.uniform(0, subtotal * 0.3), 2) if random.random() > 0.7 else 0
        
        total_amount = subtotal + tax + shipping - discount
        
        # Introduce negative amounts (data quality issue)
        if random.random() < 0.01:
            total_amount = -abs(total_amount)
        
        # Payment method with inconsistencies
        payment_methods = ['credit_card', 'Credit Card', 'CREDIT_CARD', 'debit_card',
                          'paypal', 'PayPal', 'PAYPAL', 'gift_card', 'cod', 'Cash on Delivery', '']
        payment_method = random.choice(payment_methods)
        
        # Shipping addresses with inconsistencies
        states = ['CA', 'NY', 'TX', 'FL', 'WA', 'California', 'New York', '', None]
        countries = ['USA', 'US', 'United States', 'usa', 'U.S.A', '', None]
        
        orders.append({
            'order_id': order_id,
            'customer_id': customer_id,
            'order_date': order_date_str,
            'subtotal': subtotal,
            'tax_amount': tax,
            'discount_amount': discount,
            'total_amount': total_amount,
            'payment_method': payment_method,
            'created_at': order_date_str
        })
    
    return pd.DataFrame(orders)


# Generate order items (line items)
def generate_order_items(orders_df, avg_items_per_order=2.5):
    """
    Generate order line items with product details
    """
    
    order_items = []
    item_id = 1
    
    for _, order in orders_df.iterrows():
        num_items = max(1, int(np.random.poisson(avg_items_per_order)))
        
        for _ in range(num_items):
            product_id = f"PROD{random.randint(1, 1000):06d}"
            quantity = random.randint(1, 5)
            
            # Introduce negative quantities (data issue)
            if random.random() < 0.005:
                quantity = -quantity
            
            unit_price = round(random.uniform(5, 500), 2)
            
            # Inconsistent product names
            product_name = f"Product_{product_id}"
            if random.random() < 0.1:
                product_name = product_name.lower()
            
            # Category with inconsistencies
            categories = ['Electronics', 'electronics', 'ELECTRONICS', 'Clothing', 
                         'Home & Garden', 'Books', 'Sports', 'Toys', '', None]
            
            order_items.append({
                'item_id': f"ITM{str(item_id).zfill(10)}",
                'order_id': order['order_id'],
                'product_id': product_id,
                'product_name': product_name,
                'category': random.choice(categories),
                'quantity': quantity,
                'unit_price': unit_price,
                'line_total': round(quantity * unit_price, 2),
                'sku': f"SKU-{random.randint(10000, 99999)}" if random.random() > 0.05 else None
            })
            item_id += 1
    
    return pd.DataFrame(order_items)


# Generate customer data
def generate_customers(num_customers=2000):
    """
    Generate customer master data with quality issues
    """
    
    customers = []
    
    for i in range(1, num_customers + 1):
        customer_id = f"CUST{i:06d}"
        
        # Email with inconsistencies
        email_formats = [
            f"customer{i}@email.com",
            f"Customer{i}@Email.com",
            f"customer.{i}@email.com",
            f"customer{i}@gmail.com",
            "",
            None
        ]
        email = random.choice(email_formats)
        
        # Phone with different formats
        phone_formats = [
            f"555-{random.randint(100, 999)}-{random.randint(1000, 9999)}",
            f"(555) {random.randint(100, 999)}-{random.randint(1000, 9999)}",
            f"555{random.randint(100, 999)}{random.randint(1000, 9999)}",
            f"+1-555-{random.randint(100, 999)}-{random.randint(1000, 9999)}",
            "",
            None
        ]
        
        registration_date = datetime(2020, 1, 1) + timedelta(days=random.randint(0, 1400))
        
        customers.append({
            'customer_id': customer_id,
            'first_name': f"FirstName{i}",
            'last_name': f"LastName{i}",
            'email': email,
            'phone': random.choice(phone_formats),
            'registration_date': registration_date.strftime('%Y-%m-%d'),
            'customer_segment': random.choice(['Premium', 'Standard', 'Basic', 'VIP', '', None]),
            'is_active': random.choice([True, False, 'true', 'false', 1, 0, 'Y', 'N']),
            'lifetime_value': round(random.uniform(0, 10000), 2)
        })
    
    return pd.DataFrame(customers)


# Generate all datasets
print("Generating orders data...")
orders_df = generate_orders_data(5000)
orders_df.to_csv(f'{ORDER_PATH}/orders.csv', index=False)
print(f"✓ Generated orders.csv with {len(orders_df)} records")

print("\nGenerating order items data...")
order_items_df = generate_order_items(orders_df)
order_items_df.to_csv(f'{ORDER_ITEMS_PATH}/order_items.csv', index=False)
print(f"✓ Generated order_items.csv with {len(order_items_df)} records")

print("\nGenerating customers data...")
customers_df = generate_customers(2000)
customers_df.to_csv(f'{CUSTOMER_PATH}/customers.csv', index=False)
print(f"✓ Generated customers.csv with {len(customers_df)} records")

print("\n" + "="*60)
print("DATA QUALITY ISSUES INTRODUCED:")
print("="*60)
print("✓ Inconsistent date formats across records")
print("✓ Missing values (NULLs) in critical fields")
print("✓ Duplicate order records (~2%)")
print("✓ Inconsistent capitalization and naming")
print("✓ Invalid data (negative amounts/quantities)")
print("✓ Multiple phone/email formats")
print("✓ Boolean values in different formats")
print("✓ Empty strings vs NULL values")
print("\n" + "="*60)
print("SAMPLE DATA PREVIEW:")
print("="*60)
print("\nOrders Sample:")
print(orders_df.head(3))
print("\nOrder Items Sample:")
print(order_items_df.head(3))
print("\nCustomers Sample:")
print(customers_df.head(3))





# import pandas as pd
# import numpy as np
# from datetime import datetime, timedelta
# import random
# import string
# from  datetime import datetime
# from faker import Faker
# import os
# fake=Faker()

# # Set seed for reproducibility
# np.random.seed(42)
# random.seed(42)
# today_str=datetime.today().strftime("%Y-%m-%d")

# ORDER_PATH=f"./data/landing/orders/{today_str}/"
# CUSTOMER_PATH=f"./data/landing/customers/{today_str}/"
# ORDER_ITEMS_PATH=f"./data/landing/orders_item/{today_str}/"


# os.makedirs(ORDER_PATH, exist_ok=True)
# os.makedirs(CUSTOMER_PATH, exist_ok=True)
# os.makedirs(ORDER_ITEMS_PATH, exist_ok=True)

# # Generate realistic messy orders data
# def generate_orders_data(num_records=5000):
#     """
#     Simulates OLTP database dumps with realistic messiness:
#     - Inconsistent date formats
#     - Missing values
#     - Duplicate records
#     - Data quality issues
#     """
    
#     start_date = datetime(2023, 1, 1)
#     end_date = datetime(2024, 11, 15)
    
#     orders = []
    
#     for i in range(num_records):
#         order_id = f"ORD{str(i+1000).zfill(8)}"
        
#         # Introduce 2% duplicate orders (real-world issue)
#         if random.random() < 0.02 and i > 0:
#             order_id = orders[-1]['order_id']
        
#         # Random timestamp
#         order_date = start_date + timedelta(
#             seconds=random.randint(0, int((end_date - start_date).total_seconds()))
#         )
        
#         # Inconsistent date formats (common in real systems)
#         date_formats = [
#             order_date.strftime('%Y-%m-%d %H:%M:%S'),
#             order_date.strftime('%m/%d/%Y %H:%M'),
#             order_date.strftime('%d-%m-%Y %H:%M:%S'),
#             order_date.isoformat(),
#         ]
#         order_date_str = random.choice(date_formats)
        
#         customer_id = f"CUST{random.randint(1, 2000):06d}"
        
#         # Introduce missing customer IDs (3%)
#         if random.random() < 0.03:
#             customer_id = None
        
#         # Order status with typos and inconsistencies
#         statuses = ['completed', 'Completed', 'COMPLETED', 'pending', 'Pending', 
#                    'cancelled', 'Cancelled', 'shipped', 'returned', 'refunded', 
#                    'processing', 'failed', '']
#         order_status = random.choice(statuses)
        
#         # Random amounts
#         subtotal = round(random.uniform(10, 2000), 2)
#         tax = round(subtotal * random.uniform(0.05, 0.15), 2)
#         shipping = round(random.uniform(0, 25), 2) if random.random() > 0.1 else 0
#         discount = round(random.uniform(0, subtotal * 0.3), 2) if random.random() > 0.7 else 0
        
#         total_amount = subtotal + tax + shipping - discount
        
#         # Introduce negative amounts (data quality issue)
#         if random.random() < 0.01:
#             total_amount = -abs(total_amount)
        
#         # Payment method with inconsistencies
#         payment_methods = ['credit_card', 'Credit Card', 'CREDIT_CARD', 'debit_card',
#                           'paypal', 'PayPal', 'PAYPAL', 'gift_card', 'cod', 'Cash on Delivery', '']
#         payment_method = random.choice(payment_methods)
        
#         # Shipping addresses with inconsistencies
#         states = ['CA', 'NY', 'TX', 'FL', 'WA', 'California', 'New York', '', None]
#         countries = ['USA', 'US', 'United States', 'usa', 'U.S.A', '', None]
        
#         orders.append({
#             'order_id': order_id,
#             'customer_id': customer_id,
#             'order_date': order_date_str,
#             'order_status': order_status,
#             'subtotal': subtotal,
#             'tax_amount': tax,
#             'shipping_cost': shipping,
#             'discount_amount': discount,
#             'total_amount': total_amount,
#             'payment_method': payment_method,
#             'shipping_state': random.choice(states),
#             'shipping_country': random.choice(countries),
#             'warehouse_id': f"WH{random.randint(1, 10):03d}",
#             'created_at': order_date_str,
#             'updated_at': (order_date + timedelta(hours=random.randint(0, 48))).strftime('%Y-%m-%d %H:%M:%S')
#         })
    
#     return pd.DataFrame(orders)


# # Generate order items (line items)
# def generate_order_items(orders_df, avg_items_per_order=2.5):
#     """
#     Generate order line items with product details
#     """
    
#     order_items = []
#     item_id = 1
    
#     for _, order in orders_df.iterrows():
#         num_items = max(1, int(np.random.poisson(avg_items_per_order)))
        
#         for _ in range(num_items):
#             product_id = f"PROD{random.randint(1, 1000):06d}"
#             quantity = random.randint(1, 5)
            
#             # Introduce negative quantities (data issue)
#             if random.random() < 0.005:
#                 quantity = -quantity
            
#             unit_price = round(random.uniform(5, 500), 2)
            
#             # Inconsistent product names
#             product_name = f"Product_{product_id}"
#             if random.random() < 0.1:
#                 product_name = product_name.lower()
            
#             # Category with inconsistencies
#             categories = ['Electronics', 'electronics', 'ELECTRONICS', 'Clothing', 
#                          'Home & Garden', 'Books', 'Sports', 'Toys', '', None]
            
#             order_items.append({
#                 'item_id': f"ITM{str(item_id).zfill(10)}",
#                 'order_id': order['order_id'],
#                 'product_id': product_id,
#                 'product_name': product_name,
#                 'category': random.choice(categories),
#                 'quantity': quantity,
#                 'unit_price': unit_price,
#                 'line_total': round(quantity * unit_price, 2),
#                 'sku': f"SKU-{random.randint(10000, 99999)}" if random.random() > 0.05 else None
#             })
#             item_id += 1
    
#     return pd.DataFrame(order_items)


# # Generate customer data
# def generate_customers(num_customers=2000):
#     """
#     Generate customer master data with quality issues
#     """
    
#     customers = []
    
#     for i in range(1, num_customers + 1):
#         customer_id = f"CUST{i:06d}"
#         name=fake.name().split()
        
#         # Email with inconsistencies
#         email_formats = [
#             f"{name[0]+name[1]}{i}@email.com",
#             f"{name[0]+name[1]}{i}@Email.com",
#             f"{name[0]+name[1]}.{i}@email.com",
#             f"{name[0]+name[1]}{i}@gmail.com",
#             "",
#             None
#         ]
#         email = random.choice(email_formats)
        
#         # Phone with different formats
#         phone_formats = [
#             f"555-{random.randint(100, 999)}-{random.randint(1000, 9999)}",
#             f"(555) {random.randint(100, 999)}-{random.randint(1000, 9999)}",
#             f"555{random.randint(100, 999)}{random.randint(1000, 9999)}",
#             f"+1-555-{random.randint(100, 999)}-{random.randint(1000, 9999)}",
#             "",
#             None
#         ]
        
#         registration_date = datetime(2020, 1, 1) + timedelta(days=random.randint(0, 1400))
        
#         customers.append({
#             'customer_id': customer_id,
#             'first_name': name[0],
#             'last_name': name[1],
#             'email': email,
#             'phone': random.choice(phone_formats),
#             'registration_date': registration_date.strftime('%Y-%m-%d'),
#             'customer_segment': random.choice(['Premium', 'Standard', 'Basic', 'VIP', '', None]),
#             'is_active': random.choice([True, False, 'true', 'false', 1, 0, 'Y', 'N']),
#             'lifetime_value': round(random.uniform(0, 10000), 2)
#         })
    
#     return pd.DataFrame(customers)


# # Generate all datasets
# print("Generating orders data...")
# orders_df = generate_orders_data(5000)
# orders_df.to_csv(f'{ORDER_PATH}/orders.csv', index=False)
# print(f"✓ Generated orders.csv with {len(orders_df)} records")

# print("\nGenerating order items data...")
# order_items_df = generate_order_items(orders_df)
# order_items_df.to_csv(f'{ORDER_ITEMS_PATH}/order_items.csv', index=False)
# print(f"✓ Generated order_items.csv with {len(order_items_df)} records")

# print("\nGenerating customers data...")
# customers_df = generate_customers(2000)
# customers_df.to_csv(f'{CUSTOMER_PATH}/customers.csv', index=False)
# print(f"✓ Generated customers.csv with {len(customers_df)} records")

# print("\n" + "="*60)
# print("DATA QUALITY ISSUES INTRODUCED:")
# print("="*60)
# print("✓ Inconsistent date formats across records")
# print("✓ Missing values (NULLs) in critical fields")
# print("✓ Duplicate order records (~2%)")
# print("✓ Inconsistent capitalization and naming")
# print("✓ Invalid data (negative amounts/quantities)")
# print("✓ Multiple phone/email formats")
# print("✓ Boolean values in different formats")
# print("✓ Empty strings vs NULL values")
# print("\n" + "="*60)
# print("SAMPLE DATA PREVIEW:")
# print("="*60)
# print("\nOrders Sample:")
# print(orders_df.head(3))
# print("\nOrder Items Sample:")
# print(order_items_df.head(3))
# print("\nCustomers Sample:")
# print(customers_df.head(3))