import sys
import subprocess
from db_connection import DBConnection
from initial_setup import clean_file, default_query, delete_query, insert_query, create_index, delete_index

MIN_RECORDS = 300_000
INCREMENT = 25_000
MAX_RECORDS = 625_000


def main():
    print("Welcome to Assignment 3 regarding Indexing:= ")

    # check for exactly 3 command line args
    if len(sys.argv) != 4:
        print("Usage: python main.py <db_name> <user_name> <password>")
        sys.exit(1)

    db_name = sys.argv[1]
    user_name = sys.argv[2]
    password = sys.argv[3]

    # create db object
    db_object = DBConnection(db_name, user_name, password)
    cnx = db_object.get_connection()

    # execute the as3 related tasks
    print("Executing Assignment 3 tasks...")

    # output results to a csv file
    outfile = "as3_results.csv"
    clean_file(outfile)

    # create tables and load data
    db_object.create_tables(cnx)

    for index in [False, True]:
        for num_records in range(MIN_RECORDS, MAX_RECORDS + 1, INCREMENT):
            print("-" * 100)
            db_object.generate_history_records(cnx=cnx, num_records=num_records)
            experiment(cnx, num_records, index=index)

    return None


def experiment(cnx, num_records, index=False):
    if index:
        create_index(cnx, num_records, column_name="city")
        default_query(cnx, num_records, index=True)
        insert_query(cnx, num_records, index=True)
        delete_query(cnx, num_records, index=True)
        print(f"Experiment with {num_records} records done with index.")
    else:
        default_query(cnx, num_records, index=False)
        insert_query(cnx, num_records, index=False)
        delete_query(cnx, num_records, index=False)
        print(f"Experiment with {num_records} records done without index.")


if __name__ == "__main__":
    main()
