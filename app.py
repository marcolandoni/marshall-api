import mysql.connector

print ( "Hello World! " )

mydb = mysql.connector.connect( host="127.0.0.1",
  user="marshall",
  password="mar5ha11",
  database="marshall"
)

print(mydb)


cursor = mydb.cursor(buffered=True, dictionary=True)
cursor.execute("SELECT * from webapp_users;")
results = cursor.fetchall()
print(results)
for r in results:
    print(r)