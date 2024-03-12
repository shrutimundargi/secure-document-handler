import mysql.connector
 
# Set up your database connection details
config = {
    'user': '',
    'password': '',
    'host': '',  
    'unix_socket': '',
}
 
# Connect to the database
connection = mysql.connector.connect(**config)
 
try:
    # Create a cursor
    cursor = connection.cursor()
 
    # Execute SQL queries
    cursor.execute("SELECT * FROM your_table_name")
 
    # Fetch and print results
    results = cursor.fetchall()
    for row in results:
        print(row)
 
finally:
    # Close the cursor and connection
    cursor.close()
    connection.close()