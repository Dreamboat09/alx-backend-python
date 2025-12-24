import mysql.connector
from mysql.connector import Error
import csv
import uuid
import os


def connect_db():

    # Connects to the MySQL database server (not a specific database).
    
    try:
        # Attempt to establish connection to MySQL server
        connection = mysql.connector.connect(
            host='localhost',        
            user='root',             
            password='your_password' 
        )
        
        # Check if connection was successful
        if connection.is_connected():
            print(" Successfully connected to MySQL server")
            return connection
            
    except Error as e:
        # Error handling: catches MySQL-specific errors
        print(f" Error connecting to MySQL: {e}")
        return None


def create_database(connection):

    # Creates the ALX_prodev database if it doesn't exist.
    
    try:
        cursor = connection.cursor()
        create_db_query = "CREATE DATABASE IF NOT EXISTS ALX_prodev"
        cursor.execute(create_db_query)
        print(" Database 'ALX_prodev' created successfully (or already exists)")
        cursor.close()
        
    except Error as e:
        print(f" Error creating database: {e}")


def connect_to_prodev():
    
   # Connects to the ALX_prodev database specifically.
    try:
        connection = mysql.connector.connect(
            host='localhost',
            user='root',
            password='your_password',
            database='ALX_prodev'  
        )
        
        if connection.is_connected():
            print(" Successfully connected to ALX_prodev database")
            return connection
            
    except Error as e:
        print(f" Error connecting to ALX_prodev: {e}")
        return None


def create_table(connection):
    
    # Creates the user_data table with required schema.
    try:
        cursor = connection.cursor()
        create_table_query = """
        CREATE TABLE IF NOT EXISTS user_data (
            user_id CHAR(36) PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            email VARCHAR(255) NOT NULL,
            age DECIMAL(3, 0) NOT NULL,
        )
        """
        
        # Execute the query
        cursor.execute(create_table_query)
        print("Table 'user_data' created successfully (or already exists)")
        
        cursor.close()
        
    except Error as e:
        print(f" Error creating table: {e}")


def insert_data(connection, data):
 
    # Inserts user data into the database, avoiding duplicates.
    try:
        cursor = connection.cursor()
        
        # SQL that ignores the insert if the Primary Key already exists
        sql = "INSERT IGNORE INTO user_data (user_id, name, email, age) VALUES (%s, %s, %s, %s)"
        
        # Convert the generator/data into a list of tuples for bulk insertion
        records = list(data) 
        
        # executemany is MUCH faster than a for-loop with execute()
        cursor.executemany(sql, records)
        
        connection.commit()
        print(f"Successfully processed {cursor.rowcount} new records.")
        cursor.close()
        
    except Error as e:
        # If error occurs, rollback changes
        connection.rollback()
        print(f" Error inserting data: {e}")


def load_csv_data(csv_file_path):
    
    # Reads user data from CSV file and yeild
    
    try:
        # Check if file exists
        if not os.path.exists(csv_file_path):
            print(f" CSV file not found: {csv_file_path}")
            return None
        
        # Open CSV file for reading
        with open(csv_file_path, 'r', encoding='utf-8') as csv_file:
            # Create CSV reader object
            csv_reader = csv.DictReader(csv_file)
            
            # Read each row as a dictionary
            for row in csv_reader:
                # Generate UUID for user_id if not in CSV
                # str(uuid.uuid4()) converts UUID object to string
                user_id = str(uuid.uuid4())
                # Extract data from row
                name = row.get('name', '').strip()  # .strip() removes whitespace
                email = row.get('email', '').strip()
                age = row.get('age', '0').strip()
                
                yield (user_id, name, email, age)
        
    except Exception as e:
        print(f" Error reading CSV file: {e}")
        return None