import psycopg2

# DB_CREDENTIALS = "host=127.0.0.1 dbname=sparkifydb user=student password=student"
DB_CREDENTIALS = "host=127.0.0.1 dbname=sparkifydb user=postgres"


class DbConnection:
    def __init__(self):
        self.conn = None
        self.cur = None

    def open_connection(self):
        conn = psycopg2.connect(DB_CREDENTIALS)
        self.conn = conn
        self.cur = conn.cursor()

    def close_connection(self):
        self.cur.close()
        self.conn.close()

    def execute_insert_query(self, df, insert_query):
        self.cur.execute(insert_query, df)
        self.conn.commit()

    def execute_select_query(self, select_query, cols):
        """ This only returns one row """
        self.cur.execute(select_query, cols)
        return self.cur.fetchone()
