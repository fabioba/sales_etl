import time
import random
from flask import Flask, jsonify, request

app = Flask(__name__)

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

# Endpoint to fetch random sales data
@app.route('/sales', methods=['GET'])
def generate_sales():
    # Parameters to customize number of sales
    count = int(request.args.get('count', 10))  # Default to 10 sales records
    sales_data = []

    for _ in range(count):
        product = random.choice(products)
        customer = random.choice(customers)
        quantity = random.randint(1, 5)
        sales_data.append({
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
            'transaction_date': time.strftime('%Y-%m-%d %H:%M:%S')
        })

    return jsonify(sales_data)

if __name__ == '__main__':
    print("Enhanced sales data generator server is running...")
    app.run(debug=True, host='0.0.0.0', port=5000)
