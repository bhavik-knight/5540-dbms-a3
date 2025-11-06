import sys
import subprocess
from time import process_time_ns

from mysql.connector.cursor_cext import CMySQLCursor
from mysql.connector import CMySQLConnection

from record_time import record_time


def default_query(cnx:CMySQLConnection, num_records: int, index: bool = False) -> None:
    """
    execute the default query and measure the performance
    :param cnx: connection to database
    :param num_records: int - number of records to generate
    :param index: boolean - whether to index or not
    :return: None
    """

    if not cnx:
        sys.exit("Connection to database failed.")
    cursor = cnx.cursor()

    query = """
        SELECT lname, fname
        FROM person, history as H
        WHERE _id=pid and eyear=2000 and H.city="Las Vegas"
        ;
    """

    start_time = process_time_ns()
    cursor.execute(query)
    end_time = process_time_ns()
    record_time(start_time, end_time, op_name="SELECT", op_description="SELECT query given in the PDF", index=index, num_records=num_records)


def insert_query(cnx: CMySQLConnection, num_records: int, index: bool = False) -> None:
    """
    execute the insert query and measure the performance
    :param cnx: connection to database
    :param num_records: number of records to be inserted
    :param index: boolean - whether the index is on or not
    :return: None
    """
    if not cnx:
        sys.exit("Connection to database failed.")
    cursor = cnx.cursor()

    op_name = "INSERT_1000"
    op_description = "INSERT 1000 records from phistory2.tsv into table history"

    query = f"load data local infile 'phistory2.tsv' into table history;"

    start_time = process_time_ns()
    cursor.execute(query)
    end_time = process_time_ns()
    cnx.rollback()
    record_time(start_time, end_time, op_name, op_description, index=index, num_records=num_records)


def delete_query(cnx: CMySQLConnection, num_records: int= 100_000, index: bool = False) -> None:
    """
    execute the delete query and measure the performance
    :param cursor: connection to database
    :param cnx: connection to database
    :param num_records: number of records to be deleted
    :param index: boolean - whether the index is on or not
    :return: None
    """
    if not cnx:
        sys.exit("Connection to database failed.")
    cursor = cnx.cursor()

    # first find the name of the city that is travelled to most
    # q_select = f"""
    #     SELECT city, COUNT(*) as num_city
    #     FROM history
    #     GROUP BY city
    #     ORDER BY num_city DESC
    #     LIMIT 1
    #     ;
    # """

    city_name = "Oslo"
    country_name = "Norway"

    q_delete_city = "DELETE FROM history WHERE city=%s;"
    start_time = process_time_ns()
    cursor.execute(q_delete_city, (city_name,))
    cnx.rollback()
    end_time = process_time_ns()
    op_name = "DELETE_CITY"
    op_description = f"DELETE all records from history where city='{city_name}'"
    record_time(start_time, end_time, op_name, op_description, index=index, num_records=num_records)

    q_delete_country = "DELETE FROM history WHERE country=%s;"
    start_time = process_time_ns()
    cursor.execute(q_delete_country, (country_name,))
    cnx.rollback()
    end_time = process_time_ns()
    op_name = "DELETE_COUNTRY"
    op_description = f"DELETE all records from history where country='{country_name}'"
    record_time(start_time, end_time, op_name, op_description, index=index, num_records=num_records)

    q_delete_100K = "DELETE FROM history LIMIT 100000;"
    start_time = process_time_ns()
    cursor.execute(q_delete_100K)
    cnx.rollback()
    end_time = process_time_ns()
    op_name = "DELETE_100K"
    op_description = f"DELETE 100,000 records from history"
    record_time(start_time, end_time, op_name, op_description, index=index, num_records=num_records)


def create_index(cnx: CMySQLConnection, num_records: int = 300_000, column_name: str="city") -> None:
    """
    create the index on column_name
    :param cnx: connection to database
    :param num_records: number of records in the table
    :param column_name: string - name of the index to be created
    :return: None
    """
    if not cnx:
        sys.exit("Connection to database failed")
    cursor = cnx.cursor()

    try:
        start_time = process_time_ns()
        query = "CREATE INDEX idx_city ON history (city)"
        cursor.execute(query)
        cnx.commit()
        end_time = process_time_ns()
    except Exception as e:
        print(f"An error occurred while creating index: {e}")
    else:
        record_time(
            start_time,
            end_time,
            op_name="CREATE_INDEX",
            op_description=f"Created index on {column_name} from the table history",
            index=True,
            num_records=num_records
        )
        print(f"Index created on {column_name} successfully.")


def delete_index(cnx: CMySQLConnection, num_records:int = 300_000, column_name: str = "city") -> None:
    """
    delete the index on column_name
    :param cnx: connection to database
    :param num_records: number of records in the table
    :param column_name: string - name of the index to be created
    :return: None
    """
    if not cnx:
        sys.exit("Connection to database failed.")
    cursor = cnx.cursor()

    try:
        start = process_time_ns()
        query = "DROP INDEX idx_city ON history;"
        cursor.execute(query)
        cnx.commit()
        end = process_time_ns()
    except Exception as e:
        print(f"An error occurred while creating index: {e}")
    else:
        record_time(
            start,
            end,
            op_name="DELETE_INDEX",
            op_description=f"Deleted index on {column_name} from the table history",
            index=False,
            num_records=num_records
        )
        print(f"Index deleted successfully.")


def close_connection(cnx: CMySQLConnection) -> None:
    """
    close the connection to the database
    :param cnx: connection to database
    :return:  None
    """
    cnx.close()


def clean_file(file_name:str) -> None:
    """
    Clean the content of the file without deleting the file
    :param file_name: name of the file to be cleaned
    :return:  None
    """
    subprocess.run(args=f"cat /dev/null > {file_name}", shell=True)
    print(f"\nFile {file_name} was successfully cleaned...")
    return None
