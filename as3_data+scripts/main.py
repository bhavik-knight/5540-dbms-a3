import sys
from db_connection import DBConnection
from initial_setup import create_tables, clean_file, default_query


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

    if cnx:
        print("Connection to database was successful!")
    else:
        print("Connection to database failed.")

    # execute the as3 related tasks
    print("Executing Assignment 3 tasks...")

    # output results to a csv file
    outfile = "as3_results.csv"
    clean_file(outfile)

    # create tables and load data
    create_tables(cnx)

    return None


if __name__ == "__main__":
    main()
