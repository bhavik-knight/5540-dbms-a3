import subprocess
import sys
from errno import errorcode
from time import process_time_ns
from mysql.connector import connect, Error


def main():
    print("Welcome to Assignment 3 regarding Indexing:= ")


    # check for exactly 3 command line args
    if len(sys.argv) != 4:
        print("Usage: python main.py <db_name> <user_name> <password>")
        sys.exit(1)

    db_name = sys.argv[1]
    user_name = sys.argv[2]
    password = sys.argv[3]

    if cnx :=connect_to_db(db_name, user_name, password):
        print("Connection to database was successful!")
    else:
        print("Connection to database failed.")

    # execute the as3 related tasks
    print("Executing Assignment 3 tasks...")

    # output results to a csv file
    outfile = "as3_results.csv"

    # create tables and load data
    create_tables(cnx)

    # generate history records
    user_input = str.strip(input("How many records would you like to generate? "))
    generate_N_history_records(user_input)

    # experiment with indexes


def generate_N_history_records(N:str) -> None:
    """
    Generates N history records by calling make_person_history.py
    :param N: string - number of records to generate
    """

    try:
        subprocess.run(
            ["python3", "make_person_history.py"],
            input=N,
            text=True,
            capture_output=True,
        )
    except Exception as e:
        print(f"An error occurred while generating history records: {e}")
        return
    else:

        # count number of lines in phistory.tsv
        records: int = 0
        with open("phistory.tsv", "r") as infile:
            records = len(infile.readlines())
        print(f"Generated {records:_} history records.")


def create_tables(cnx: object):
    """
    Creates table and loads the necessary data needed for the assignment
    """
    if not cnx:
        print("No valid connection object provided.")
        return

    # get cursor
    cursor = cnx.cursor()

    # remove content of csv file if anything is there
    try:
        with open("as3_results.csv", 'r+') as outfile:
            outfile.truncate(0)
    except FileNotFoundError:
        pass

    # create csv file and write header
    with open("as3_results.csv", 'w') as outfile:
        outfile.write("Operation, Index, Number of Records, Time (ms)\n")

    person_table = "make_person_table.sql";
    history_table = "make_history_table.sql";

    start_time = process_time_ns()
    with open(person_table, 'r') as file:
        sql_commands = file.read()
        for command in sql_commands.split(';'):
            if command.strip():
                cursor.execute(command)
    end_time = process_time_ns()
    # record time taken in milliseconds to create person table
    record_time(start_time, end_time, "Create Person Table", num_records=20_000)


    start_time = process_time_ns()
    with open(history_table, 'r') as file:
        sql_commands = file.read()
        for command in sql_commands.split(';'):
            if command.strip():
                cursor.execute(command)
    # record time taken to create history table
    end_time = process_time_ns()
    record_time(start_time, end_time, "Create History Table", num_records=300_000)


    # to check number of records in each table
    start_time = process_time_ns()
    cursor.execute("SELECT COUNT(*) FROM person;")
    person_count = cursor.fetchone()[0]
    end_time = process_time_ns()
    print(f"Number of records in person table: {person_count}")
    record_time(start_time, end_time, "Count Person Records")

    start_time = process_time_ns()
    cursor.execute("SELECT COUNT(*) FROM history;")
    history_count = cursor.fetchone()[0]
    end_time = process_time_ns()
    print(f"Number of records in history table: {history_count}")
    record_time(start_time, end_time, "Count History Records")

    print("Tables created and loaded with given data successfully.")


def record_time(start, end, operation, index=False, num_records=300_000, file_name="as3_results.csv") -> float:
    """
    Records time taken in milliseconds
    :param start: int - start time in nanoseconds
    :param end: int - end time in nanoseconds
    :return: float - time taken in milliseconds
    """
    with open(file_name, 'a') as outfile:
        outfile.write(f"{operation}, {index}, {num_records}, {(end - start) / 1_000_000}\n")


def connect_to_db(db_name: str, user_name:str, password: str) -> object:
    """
    Connects to a MySQL database
    :param db_name: string - name of the database
    :param user_name: string - username
    :param password: string - password
    :return: object - cursor if connection is successful, else False
    """
    try:
        connection = connect(
            host="localhost",
            port=3306,
            user=user_name,
            password=password,
            database=db_name,
            allow_local_infile=True # needed to load data from local files
        )

    except Error as e:
        if e.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Something is wrong with your user name or password")
        elif e.errno == errorcode.ER_BAD_DB_ERROR:
            print("Database does not exist")
        else:
            print(e)
        return False

    else:
        print (f"Connection established. {type(connection)}")
        return connection


if __name__ == "__main__":
    main()
