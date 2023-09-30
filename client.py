from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

# Connect to your Astra DB
cloud_config = {
    'secure_connect_bundle': './secure-connect-furnituredb.zip'
}
auth_provider = PlainTextAuthProvider(CLIENT_ID="vfMxWptNmqPhTPZApJQDFgDJ",
  CLIENT_SECRET= "7aEMzXGdSjTs04ZuIUoXkyZWoAwwp8T0wFEA8bY3qlr+YWR01SsX.A3aZgkQeww0lZF1iqhC5BtxZT1yJGA5_zHlimOaJ_rw3We2L6oEn5v-kYP6CRuCcPJ4j.7I+DLk",CLIENT_TOKEN= "AstraCS:vfMxWptNmqPhTPZApJQDFgDJ:bad72138e8af1402e8554953545911f4cac3b1c8196d83c4cdd59ff3203a01dc")

session = cluster.connect('furniture_keyspace')

def add_customer(customer_id, name, email):
    query = """
    INSERT INTO customers (customer_id, name, email, created_at)
    VALUES (%s, %s, %s, toTimeStamp(now()))
    """
    session.execute(query, (customer_id, name, email))

def add_product(product_id, name, description, price, stock):
    query = """
    INSERT INTO products (product_id, name, description, price, stock)
    VALUES (%s, %s, %s, %s, %s)
    """
    session.execute(query, (product_id, name, description, price, stock))

def add_order(order_id, customer_id, customer_name, product_id, product_name, quantity):
    query = """
    INSERT INTO orders (order_id, customer_id, customer_name, product_id, product_name, quantity, order_date)
    VALUES (%s, %s, %s, %s, %s, %s, toTimeStamp(now()))
    """
    session.execute(query, (order_id, customer_id, customer_name, product_id, product_name, quantity))

def fetch_all(table_name):
    query = f"SELECT * FROM {table_name}"
    rows = session.execute(query)
    for row in rows:
        print(row)

# Example usage:
# add_customer('new-customer-uuid', 'New Customer', 'new.customer@example.com')
# fetch_all('customers')

# Don't forget to close the connection when you're done
cluster.shutdown()
