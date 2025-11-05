import subprocess
from errno import errorcode
from mysql.connector import connect, Error
from initial_setup import generate_history_records


class DBConnection:
    def __init__(self, db_name, user_name, password):
        self.db_name = db_name
        self.user_name = user_name
        self.password = password
        # run this only at the beginning of the script once
        # generate_history_records()


    def get_connection(self):
        return self.make_connection()


    def make_connection(self):
        try:
            connection = connect(
                host="localhost",
                port=3306,
                user=self.user_name,
                password=self.password,
                database=self.db_name,
                allow_local_infile=True        # needed to load data from local files
            )
        except Error as e:
            match e.errno:
                case errorcode.ER_ACCESS_DENIED_ERROR:
                    print("Something is wrong with your user name or password")
                case errorcode.ER_BAD_DB_ERROR:
                    print("Database does not exist")
                case _:
                    print(e)
                    return False
        else:
            print(f"Connection established successfully.")
            return connection


    def close_connection(self):
        self.get_connection().close()
