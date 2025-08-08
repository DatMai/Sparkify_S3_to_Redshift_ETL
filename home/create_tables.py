import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries

def drop_tables(cur, conn):
    """
    Drops all tables in the data warehouse using predefined SQL queries.

    Args:
        cur (object): Database cursor to execute queries.
        conn (object): Database connection for committing transactions.

    Returns:
        None
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()

def create_tables(cur, conn):
    """
    Creates all tables in the data warehouse using predefined SQL queries.

    Args:
        cur (object): Database cursor to execute queries.
        conn (object): Database connection for committing transactions.

    Returns:
        None
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    Establishes a connection to the Redshift cluster, executes SQL queries 
    to drop existing tables, and creates new ones for the data warehouse schema.
    After completing the operations, it closes the connection.

    Args:
        None

    Returns:
        None
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()