import psycopg2
from psycopg2 import Error

class Connection:
    def __init__(self, host, port, database, user, password):
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        self.connection = None
        self.cursor = None

    def connect(self):
        try:
            self.connection = psycopg2.connect(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password
            )
            self.cursor = self.connection.cursor()
        except Error as e:
            print(f"Error while connecting to PostgreSQL: {e}")

    def close(self):
        if self.connection:
            self.cursor.close()
            self.connection.close()

    def execute(self, query):
        try:
            self.cursor.execute(query)
            self.connection.commit()
        except Error as e:
            print(f"Error while executing query: {e}")