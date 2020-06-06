import psycopg2

# DB_CREDENTIALS = "host=127.0.0.1 dbname=sparkifydb user=student password=student"
DB_CREDENTIALS = "host=127.0.0.1 dbname=sparkifydb user=postgres"


class DbConnection:
    def __init__(self):
        conn = psycopg2.connect(DB_CREDENTIALS)

        self.conn = conn
        self.cur = conn.cursor()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.cur.close()
        self.conn.close()

    def execute_insert_query(self, insert_query, df):
        """ Inserts the dataframe data into a table according to the insert query """
        self.cur.execute(insert_query, df)
        self.conn.commit()

    def execute_copy_from(self, file, table_name, cols):
        """ Inserts the file's data into a specified table columns """
        self.cur.copy_from(file, table_name, columns=cols, null='Unknown')
        self.conn.commit()

    def execute_select_query(self, select_query, cols):
        """
        Selects the given columns from the table according to the select query

        Returns the first match.
        """
        self.cur.execute(select_query, cols)
        return self.cur.fetchone()
