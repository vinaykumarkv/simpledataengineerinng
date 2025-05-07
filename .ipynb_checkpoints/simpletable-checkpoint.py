import psycopg2
try:
    conn = psycopg2.connect("host=127.0.0.1 dbname=postgres user=postgres password=1234")
except psycopg2.Error as e:
    print("Error: could not make connection to postgres database")
    print(e)

try:
    cur = conn.cursor()
except psycopg2.Error as e:
    print("Error:could not get cursor to the database")

conn.set_session(autocommit=True)

try:
    cur.execute("create database myfirstdb")
except psycopg2.Error as e:
    print("error could not create database")

try:
    conn.close()
except psycopg2.Error as e:
    print("error disconnecting")

try:
    conn = psycopg2.connect("host=127.0.0.1 dbname=myfirstdb user=postgres password=1234")
except psycopg2.Error as e:
    print("Error: could not make connection to postgres myfirstdb database")
    print(e)

try:
    cur = conn.cursor()
except psycopg2.Error as e:
    print("Error:could not get cursor to the database")
    print(e)

conn.set_session(autocommit=True)

try:
    cur.execute("CREATE TABLE IF NOT EXISTS students (student_id int, name varchar, age int, gender varchar, subject varchar, marks int);")
except psycopg2.Error as e:
    print("Unable to create table")
    print(e)

try:
    cur.execute("INSERT INTO students (student_id, name, age, gender, subject, marks) VALUES (%s, %s, %s, %s, %s, %s)",(1,"Raj", 23, "Male", "Python", 85))
except psycopg2.Error as e:
    print("Unable to insert rows")
    print(e)

try:
    cur.execute("INSERT INTO students (student_id, name, age, gender, subject, marks) VALUES (%s, %s, %s, %s, %s, %s)",(2,"priya", 22, "Female", "Python", 86))
except psycopg2.Error as e:
    print("Unable to insert rows")
    print(e)

try:
    cur.execute("SELECT * FROM students")

except psycopg2.Error as e:
    print("unable to select from table")

row = cur.fetchone()
while row:
    print(row)
    row=cur.fetchone()

cur.close()
conn.close()