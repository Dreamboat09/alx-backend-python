import mysql.connector

def stream_user_ages():
    # Generator that yields user ages from 1 to 100.
   
    connection = mysql.connector.connect(
        host="localhost",
        user="root",
        password="your_password",
        database="ALX_prodev"
    )
   
    cursor = connection.cursor(dictionary=True)
    cursor.execute("SELECT age FROM user_data")
   
    for age in cursor:
      yield age['age']

    cursor.close()
    connection.close()

def average_age():
   
    total = 0
    count = 0
    for num in stream_user_ages():
        total += num
        count += 1
    return total / count if count > 0 else None
print(f"Average age of users: {average_age()}")