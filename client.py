# Importing necessary libraries
import json
import uuid  # Import the uuid module
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

# Global session variable to use across functions
session = None

def initialize_connection():
    # This function initializes the connection to the Cassandra cluster and sets the global session variable.
    global session  # Declare session as global so it can be accessed and modified within this function
    cloud_config = {
        'secure_connect_bundle': './secure-connect-furnituredb.zip'
    }

    # Load credentials from JSON file
    with open("./msctmansoor@gmail.com-token.json") as f:
        secrets = json.load(f)

    CLIENT_ID = secrets["clientId"]
    CLIENT_SECRET = secrets["secret"]

    # Setting up the authentication provider and Cassandra cluster connection
    auth_provider = PlainTextAuthProvider(CLIENT_ID, CLIENT_SECRET)
    cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
    session = cluster.connect('furniture_keyspace')  # Connect to your keyspace
    return cluster  # Return the cluster connection object for later use

def add_customer():
    try:
        # Automatically generate a random UUID for the new customer
        customer_id = uuid.uuid4()  
        name = input("Enter Name: ")
        email = input("Enter Email: ")
        query = "INSERT INTO customers (customer_id, name, email, created_at) VALUES (%s, %s, %s, toTimeStamp(now()))"
        session.execute(query, (customer_id, name, email))
        print(f"Customer added successfully with ID: {customer_id}")
    except Exception as e:
        print(f"An error occurred: {e}")

def add_product():
    try:
        # Automatically generate a random UUID for the new product
        product_id = uuid.uuid4()
        name = input("Enter Name: ")
        description = input("Enter Description: ")
        price = float(input("Enter Price: "))
        stock = int(input("Enter Stock: "))
        query = "INSERT INTO products (product_id, name, description, price, stock) VALUES (%s, %s, %s, %s, %s)"
        session.execute(query, (product_id, name, description, price, stock))
        print(f"Product added successfully with ID: {product_id}")
    except Exception as e:
        print(f"An error occurred: {e}")

def add_order():
    try:
        # Automatically generate a random UUID for the new order
        order_id = uuid.uuid4()
        customer_id = uuid.UUID(input("Enter Customer ID: "))  # Convert input string to UUID
        customer_name = input("Enter Customer Name: ")
        product_id = uuid.UUID(input("Enter Product ID: "))  # Convert input string to UUID
        product_name = input("Enter Product Name: ")
        quantity = int(input("Enter Quantity: "))
        query = "INSERT INTO orders (order_id, customer_id, customer_name, product_id, product_name, quantity, order_date) VALUES (%s, %s, %s, %s, %s, %s, toTimeStamp(now()))"
        session.execute(query, (order_id, customer_id, customer_name, product_id, product_name, quantity))
        print(f"Order added successfully with ID: {order_id}")
    except Exception as e:
        print(f"An error occurred: {e}")

def fetch_all():
    # This function prompts the user for a table name, then fetches and displays all records from that table.
    try:
        table_name = input("Enter table name: ")  # Get user input for table name
        query = f"SELECT * FROM {table_name}"
        rows = session.execute(query)  # Execute the CQL query
        for row in rows:
            print(row)  # Print each row of the result
    except Exception as e:
        print(f"An error occurred: {e}")  # Display any errors

def main():
    # The main function initializes the database connection, then enters an interactive loop.
    cluster = initialize_connection()  # Initialize the database connection
    while True:  # Enter an interactive loop
        # Display the menu options
        print("1. Add Customer")
        print("2. Add Product")
        print("3. Add Order")
        print("4. Fetch All")
        print("5. Exit")
        choice = input("Enter your choice: ")  # Get user choice
        if choice == '1':
            add_customer()  # Call the add_customer function if choice is 1
        elif choice == '2':
            add_product()  # Call the add_product function if choice is 2
        elif choice == '3':
            add_order()  # Call the add_order function if choice is 3
        elif choice == '4':
            fetch_all()  # Call the fetch_all function if choice is 4
        elif choice == '5':
            # Exit the interactive loop and shut down the connection if choice is 5
            cluster.shutdown()
            break
        else:
            print("Invalid choice. Please try again.")

if __name__ == "__main__":
    main()  # Call the main function if this script is run as the main module

