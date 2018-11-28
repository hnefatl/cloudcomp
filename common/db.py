import pymysql
import contextlib
import warnings
import tabulate

# pymysql connections commit on close but don't actually close themselves: this context manager wraps one to provide
# sane behaviour.
class pymysql_connect:
    def __init__(self, *args, **kwargs):
        self._conn = pymysql.connect(*args, **kwargs)
        self._exitstack = contextlib.ExitStack()

    def __enter__(self):
        self._exitstack.enter_context(self._conn)
        return self._conn

    def __exit__(self, *_):
        self._exitstack.close()
        self._conn.close()


# Delete existing tables, create new tables
def initialise_instance(host, port, db_name, username, password, table_suffix):
    with pymysql_connect(
        host=host, port=port, user=username, password=password, db=db_name
    ) as connection, connection.cursor() as cursor:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            cursor.execute(
                f"DROP TABLE IF EXISTS words_{table_suffix}, letters_{table_suffix}"
            )
        cursor.execute(
            f"CREATE TABLE words_{table_suffix} ( rank INTEGER PRIMARY KEY, word TEXT NOT NULL, category TEXT NOT NULL, frequency INTEGER NOT NULL )"
        )
        cursor.execute(
            f"CREATE TABLE letters_{table_suffix} ( rank INTEGER PRIMARY KEY, letter TEXT NOT NULL, category TEXT NOT NULL, frequency INTEGER NOT NULL )"
        )


def show_db_contents(host, port, db_name, username, password, table_suffix):
    with pymysql_connect(
        host=host, port=port, user=username, password=password, db=db_name
    ) as connection, connection.cursor() as cursor:
        headers = ["Rank", "Word", "Category", "Frequency"]

        output = ""
        cursor.execute(f"SELECT * FROM words_{table_suffix}")
        output += "Words: \n"
        output += tabulate.tabulate(cursor.fetchall(), headers=headers)
        output += "\n\n"

        cursor.execute(f"SELECT * FROM letters_{table_suffix}")
        output += "Letters: \n"
        output += tabulate.tabulate(cursor.fetchall(), headers=headers)
        return output

def print_db_contents(*args, **kwargs):
    print(show_db_contents(*args, **kwargs))