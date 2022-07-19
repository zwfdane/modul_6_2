""" Korzystając ze zdobytej właśnie wiedzy, stwórz swoją bazę danych. Przejdź w niej przez wszystkie kroki opisane w tym submodule i upewnij się, że je rozumiesz"""
import sqlite3
from sqlite3 import Error, Connection

def create_connection(db_books):
    """ create a database connection to a SQLite database """
    conn = None
    try:
        conn = sqlite3.connect(db_books)
        print(f"Connected to db_file, sqlite version: sqlite3.version")
    except Error as e:
        print(e)
    return conn

create_books_sql = """
    -- books table
    CREATE TABLE IF NOT EXISTS books (
    id integer PRIMARY KEY,
    author_last_name text NOT NULL,
    title text NOT NULL,
    year integer,
    status text
    );
    """

def execute_sql(conn, sql):
    """ Execute sql
        :param conn: Connection object
        :param sql: string - a SQL script
        :return:
    """
    try:
         c = conn.cursor()
         c.execute(sql)
         print("SQL code executed!")
    except Error as e:
         print(e)


def add_book(conn, book):
   """
   Create a new book into the books table
   :param conn:
   :param book:
   :return: book_id
   """
   sql = '''INSERT INTO books(author_last_name, title, year, status)
             VALUES(?,?,?,?)'''
   cur = conn.cursor()
   cur.execute(sql, book)
   conn.commit()
   return cur.lastrowid

def select_book_by_status(conn, status):
   """
   Query tasks by priority
   :param conn: the Connection object
   :param status:
   :return:
   """
   cur = conn.cursor()
   cur.execute("SELECT * FROM books WHERE status=?", (status,))
   rows = cur.fetchall()
   return rows

def select_all(conn, table):
   """
   Query all rows in the table
   :param conn: the Connection object
   :return:
   """
   cur = conn.cursor()
   cur.execute(f"SELECT * FROM {table}")
   rows = cur.fetchall()
   return rows

def select_where(conn, table, **query):
   """
   Query tasks from table with data from **query dict
   :param conn: the Connection object
   :param table: table name
   :param query: dict of attributes and values
   :return:
   """
   cur = conn.cursor()
   qs = []
   values = ()
   for k, v in query.items():
       qs.append(f"{k}=?")
       values += (v,)
   q = " AND ".join(qs)
   cur.execute(f"SELECT * FROM {table} WHERE {q}", values)
   rows = cur.fetchall()
   return rows
def update(conn, table, id, **kwargs):
   """
   update status, begin_date, and end date of a task
   :param conn:
   :param table: table name
   :param id: row id
   :return:
   """
   parameters = [f"{k} = ?" for k in kwargs]
   parameters = ", ".join(parameters)
   values = tuple(v for v in kwargs.values())
   values += (id, )

   sql = f''' UPDATE {table}
             SET {parameters}
             WHERE id = ?'''
   try:
       cur = conn.cursor()
       cur.execute(sql, values)
       conn.commit()
       print("OK")
   except sqlite3.OperationalError as e:
       print(e)

def delete_where(conn, table, **kwargs):
   """
   Delete from table where attributes from
   :param conn:  Connection to the SQLite database
   :param table: table name
   :param kwargs: dict of attributes and values
   :return:
   """
   qs = []
   values = tuple()
   for k, v in kwargs.items():
       qs.append(f"{k}=?")
       values += (v,)
   q = " AND ".join(qs)

   sql = f'DELETE FROM {table} WHERE {q}'
   cur = conn.cursor()
   cur.execute(sql, values)
   conn.commit()
   print("Deleted")

def delete_all(conn, table):
   """
   Delete all rows from table
   :param conn: Connection to the SQLite database
   :param table: table name
   :return:
   """
   sql = f'DELETE FROM {table}'
   cur = conn.cursor()
   cur.execute(sql)
   conn.commit()
   print("Deleted all")

if __name__ == '__main__':
    # Połączenie do bazy
    conn = create_connection("db_books.db")
    # Tworzenie tabelki
    execute_sql(conn, create_books_sql)
    # Wprowadzamy dane
    book_data_0 = ("Holiday","Umysł niewzruszony", "2019", "w trakcie czytania")
    add_book(conn,book_data_0)
    book_data_1 = ("Navlani","Python i praca z danymi", "2022", "nieprzeczytane")
    add_book(conn,book_data_1)
    book_data_2 = ("Nestor","Oddech", "2021", "nieprzeczytane")
    add_book(conn,book_data_2)
    # Wybieramy nieprzeczytane - I sposób
    print(select_book_by_status(conn,"nieprzeczytane"))
    # Wybieramy nieprzeczytane - II sposób
    print(select_where(conn, "books", status="nieprzeczytane"))
    # Wybieramy wszystkie
    print(select_all(conn, "books"))
    # Uakyualniamy status
    update(conn, "books", 2, status="przeczytane")
    # Usuwamy rekord z id=5
    delete_where(conn, "books", id=5)
    # delete_all(conn, "books")
   
