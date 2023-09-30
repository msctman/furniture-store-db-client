from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

# Connect to your Astra DB
cloud_config = {
    'secure_connect_bundle': '/workspace/furniture-store-db-client/python_project/secure-connect-furnituredb.zip'
}
auth_provider = PlainTextAuthProvider(client_id=FEZquCKMGPBKLOhluYLdEYWp, client_secret:uY_slev,e_zhEZG,U+q3QBTAADnEXbyjuqjEHRRhtcTs+PveLldNDIeo.FveH2DqXcUNlxO+uBdbjzQUopIgeohwSC88Q2.C964xLYxo6dT18nDG3NLrwyryFIxjelKE)

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
