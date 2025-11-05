import sys
import subprocess
from time import process_time_ns

from mysql.connector.cursor_cext import CMySQLCursor

from record_time import record_time


def create_tables(cursor: CMySQLCursor):
    """
    Creates table and loads the necessary data needed for the assignment
    "param cnx: connection to database
    """
    try:
        with open("as3_results.csv", 'w') as outfile:
            outfile.write("Operation, Index, Number of Records, Time (ms)\n")
    except Exception as e:
        print(e)
    finally:
        outfile.close()

    person_table = "make_person_table.sql"
    history_table = "make_history_table.sql"

    try:
        start_time = process_time_ns()
        with open(person_table, 'r') as file:
            sql_commands = file.read()
            for command in sql_commands.split(';'):
                if command.strip():
                    cursor.execute(command)
        end_time = process_time_ns()
    except Exception as e:
        print(e)
    else:
        # record time taken in milliseconds to create person table
        record_time(start_time, end_time, "Create Person Table and load data", num_records=20_000)

    try:
        start_time = process_time_ns()
        with open(history_table, 'r') as file:
            sql_commands = file.read()
            for command in sql_commands.split(';'):
                if command.strip():
                    cursor.execute(command)
        # record time taken to create history table
        end_time = process_time_ns()
    except Exception as e:
        print(e)
    else:
        record_time(start_time, end_time, "Create History Table and load data", num_records=300_000)

    print("\nTables created and loaded with given data successfully.")
    # to check number of records in each table
    result = subprocess.run(args="wc -l < persons.tsv", capture_output=True, text=True, shell=True)
    print(f"Number of records in person table: {result.stdout.strip()}")

    result = subprocess.run(args="wc -l < phistory.tsv", capture_output=True, text=True, shell=True)
    print(f"Number of records in history table: {result.stdout.strip()}")


def default_query(cursor:CMySQLCursor, num_records: int, index: bool = False) -> None:
    """
    execute the default query and measure the performance
    :param cursor: connection to database
    :param num_records: int - number of records to generate
    :param index: boolean - whether to index or not
    :return: None
    """

    query = """
    SELECT lname, fname 
    FROM person, history as H
    WHERE _id=pid and eyear=2000 and H.city="Las Vegas"
    ;
    """

    start_time = process_time_ns()
    cursor.execute(query)
    end_time = process_time_ns()
    record_time(start_time, end_time, "READ Operation: SELECT")


def insert_query(cursor: CMySQLCursor, num_records: int, index: bool = False) -> None:
    """
    execute the insert query and measure the performance
    :param cursor: connection to database
    :param num_records: int - number of records to insert
    :param index: boolean - whether to index or not
    :return: None
    """
    pass


def clean_file(file_name:str) -> None:
    """
    Clean the content of the file without deleting the file
    :param file_name: name of the file to be cleaned
    :return:  None
    """
    subprocess.run(args=f"cat /dev/null > {file_name}", shell=True)
    print(f"\nFile {file_name} was successfully cleaned...")
    return None


def generate_history_records(num_records:int=300_000) -> None:
    """
    Generates N history records by calling make_person_history.py
    :param num_records: string - number of records to generate
    """

    try:
        subprocess.run(
            ["python3", "make_person_history.py"],
            input=str(num_records),
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
