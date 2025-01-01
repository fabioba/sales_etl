import time
import random

# Sample data pools
products = [
    {'product_id': 1, 'name': 'Laptop', 'category': 'Electronics', 'price': 1200.0},
    {'product_id': 2, 'name': 'Smartphone', 'category': 'Electronics', 'price': 800.0},
    {'product_id': 3, 'name': 'Tablet', 'category': 'Electronics', 'price': 500.0},
    {'product_id': 4, 'name': 'Monitor', 'category': 'Accessories', 'price': 300.0}
]
customers = [
    {'customer_id': 1, 'name': 'Alice', 'region': 'North'},
    {'customer_id': 2, 'name': 'Bob', 'region': 'South'},
    {'customer_id': 3, 'name': 'Charlie', 'region': 'East'},
    {'customer_id': 4, 'name': 'Diana', 'region': 'West'}
]
payment_methods = ['Credit Card', 'Debit Card', 'PayPal', 'Gift Card']

def generate_sales(count_sales : int = 10) -> list:
    """
    This method generates sales data

    Args:
        count_sales (int)

    Returns:
        list_sales (list)
    """
    # Parameters to customize number of sales
    list_sales = []

    for _ in range(count_sales):
        product = random.choice(products)
        customer = random.choice(customers)
        quantity = random.randint(1, 5)
        list_sales.append({
            'sales_id': random.randint(1000, 9999),
            'product_id': product['product_id'],
            'product_name': product['name'],
            'category': product['category'],
            'price': product['price'],
            'quantity': quantity,
            'total_amount': round(product['price'] * quantity, 2),
            'customer_id': customer['customer_id'],
            'customer_name': customer['name'],
            'region': customer['region'],
            'payment_method': random.choice(payment_methods),
            'sale_date': time.strftime('%Y-%m-%d %H:%M:%S')
        })

    return list_sales
