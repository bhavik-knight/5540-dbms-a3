from time import process_time

def record_time(
    start,
    end,
    operation,
    index=False,
    num_records=300_000,
    file_name="as3_results.csv"
) -> None:
    """
    Records time taken in milliseconds
    :param start: int - start time in nanoseconds
    :param end: int - end time in nanoseconds
    :param operation: string - operation to perform
    :param index: bool - whether to index is on or not
    :param num_records: int - number of records to generate
    :param file_name: string - name of the file to write
    :return: float - time taken in milliseconds
    """
    try:
        with open(file_name, 'a') as outfile:
            outfile.write(f"{operation}, {index}, {num_records}, {(end - start) / 1_000_000}\n")
    except Exception as e:
        print(e)
    else:
        print(f"Time recorded successfully for operation =>\t{operation}")
    finally:
        outfile.close()

