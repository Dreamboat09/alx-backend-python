import mysql.connector


def paginate_users(page_size, offset):
    
    # Fetches a single page of users from the database.
    connection = mysql.connector.connect(
        host="localhost",
        user="root",
        password="your_password",
        database="ALX_prodev"
    )

    cursor = connection.cursor(dictionary=True)
    cursor.execute(
        "SELECT * FROM user_data LIMIT %s OFFSET %s",
        (page_size, offset)
    )

    rows = cursor.fetchall()

    cursor.close()
    connection.close()
    return rows


def lazy_paginate(page_size):
    
    # Generator that lazily fetches paginated user data.

    offset = 0

    while True:                   
        page = paginate_users(page_size, offset)

        if not page:
            return                   

        yield page                   
        offset += page_size