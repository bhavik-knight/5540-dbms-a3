import subprocess, sys
from errno import errorcode
from mysql.connector import CMySQLConnection, connect, Error
from mysql.connector.cursor_cext import CMySQLCursor
from record_time import record_time
from time import process_time_ns


class DBConnection:
    def __init__(self, db_name, user_name, password):
        self.db_name = db_name
        self.user_name = user_name
        self.password = password
        # run this only at the beginning of the experiment
        # generate_history_records()


    def get_connection(self) -> CMySQLConnection | None:
        try:
            connection = self.make_connection()
        except Error as e:
            sys.exit(f"Error connecting to database: {e}")
        else:
            print("Database connection successful.")
        return connection


    def make_connection(self) -> CMySQLConnection | None:
        try:
            connection = connect(
                host="localhost",
                port=3306,
                user=self.user_name,
                password=self.password,
                database=self.db_name,
                # needed to load data from local files
                allow_local_infile=True,
                # to carry out multiple operations in a single transaction
                buffered=True
            )
        except Error as e:
            match e.errno:
                case errorcode.ER_ACCESS_DENIED_ERROR:
                    print("Something is wrong with your user name or password")
                case errorcode.ER_BAD_DB_ERROR:
                    print("Database does not exist")
                case _:
                    sys.exit(e)
        else:
            print(f"Connection established successfully.")
            connection.autocommit = False
            return connection


    def get_cursor(self) -> CMySQLCursor | None:
        connection = self.get_connection()
        if connection:
            cursor = connection.cursor()
            print(f"Cursor obtained successfully. {type(cursor)}")
            return cursor
        else:
            sys.exit("Connection to database failed.")


    def close_connection(self, connection: CMySQLConnection) -> None:
        if connection.is_connected():
            connection.close()
            print("Database connection closed.")


    def create_tables(self, cnx: CMySQLConnection) -> None:
        """
        Creates table and loads the necessary data needed for the assignment
        :param cursor: connection to database
        :param cnx: connection to database
        """

        if not cnx:
            sys.exit("Connection to database failed.")

        try:
            with open("as3_results.csv", 'w') as outfile:
                outfile.write("Operation Name, Index, Number of Records, Time (ms), Operation Description\n")
        except Exception as e:
            print(e)
        finally:
            outfile.close()

        try:
            cursor = cnx.cursor()
            with open("make_person_table.sql", 'r') as file:
                sql_commands = file.read()
            for command in sql_commands.split(';'):
                if command.strip():
                    start_time = process_time_ns()
                    cursor.execute(command)
                    cnx.commit()
                    end_time = process_time_ns()
        except Exception as e:
            sys.exit(e)
        else:
            # record time taken in milliseconds to create person table
            record_time(start_time, end_time, op_name="CREATE_AND_LOAD_NEW", op_description="Create Person Table and load data", num_records=20_000)
        finally:
            file.close()

        self.generate_history_records(cnx=cnx, num_records=300_000)
        print("\nTables created and loaded with given data successfully.")
        # to check number of records in each table
        result = subprocess.run(args="wc -l < persons.tsv", capture_output=True, text=True, shell=True)
        records = int(result.stdout.strip())
        print(f"Number of records in person table: {records:_}")

        result = subprocess.run(args="wc -l < phistory.tsv", capture_output=True, text=True, shell=True)
        records = int(result.stdout.strip())
        print(f"Number of records in history table: {records:_}")



    def generate_history_records(self, cnx: CMySQLConnection, num_records:int=300_000) -> None:
        """
        Generates N history records by calling make_person_history.py
        :param num_records: string - number of records to generate
        """
        if not cnx:
            sys.exit("Connection to database failed.")

        try:
            subprocess.run(
                ["python3", "make_person_history.py"],
                input=str(num_records),
                text=True,
                capture_output=True,
            )
        except Exception as e:
            sys.exit(f"An error occurred while generating history records: {e}")
        else:
            # count number of lines in phistory.tsv
            result = subprocess.run(args="wc -l < phistory.tsv", shell=True, text=True, capture_output=True)
            records = int(result.stdout.strip())
            print(f"Generated {records:_} history records.")


        try:
            cursor = cnx.cursor()
            with open("make_history_table.sql", 'r') as file:
                sql_commands = file.read()
            for command in sql_commands.split(';'):
                if command.strip():
                    start_time = process_time_ns()
                    cursor.execute(command)
                    cnx.commit()
                    end_time = process_time_ns()
        except Exception as e:
            sys.exit(e)
        else:
            # record time taken in milliseconds to create person table
            record_time(
                start_time,
                end_time,
                op_name="CREATE_AND_LOAD_NEW",
                op_description="Create history table and load data into it from phistory.tsv",
                index=False,
                num_records=records
            )
