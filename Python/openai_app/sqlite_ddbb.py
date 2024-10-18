import sqlite3

# Connect to SQLite database
# If the database does not exist, it will be created
conn = sqlite3.connect('example.db')

# Create a cursor object to interact with the database
cursor = conn.cursor()

# Create a table
cursor.execute('''
CREATE TABLE IF NOT EXISTS hs_kb (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    title TEXT NOT NULL,
    category TEXT NOT NULL,
    object TEXT NOT NULL,
    text TEXT NOT NULL
)
''')

# # # Insert some data into the table
# # cursor.execute('''
# # INSERT INTO users (name, age, email)
# # VALUES ('Alice', 30, 'alice@example.com'),
# #        ('Bob', 24, 'bob@example.com'),
# #        ('Charlie', 29, 'charlie@example.com')
# # ''')

# # Commit the transaction
# conn.commit()

# Query the database and fetch all results
cursor.execute('SELECT * FROM hs_kb')
rows = cursor.fetchall()

# Print the results
for row in rows:
    print(row)

# Query to get the list of all tables in the database
cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")

# Fetch all table names
tables = cursor.fetchall()

# Print the list of tables
print("Tables in the database:")
for table in tables:
    print(table[0])

# Close the connection
conn.close()