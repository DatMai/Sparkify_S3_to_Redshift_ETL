import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Loads data from S3 into Redshift staging tables using predefined SQL queries.

    Args:
        cur (object): Database cursor for executing queries.
        conn (object): Database connection for committing transactions.

    Returns:
        None
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Inserts data from staging tables into fact and dimension tables 
    for the analytical schema using predefined SQL queries.

    Args:
        cur (object): Database cursor for executing queries.
        conn (object): Database connection for committing transactions.

    Returns:
        None
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    Establishes a connection to the Redshift cluster and executes queries to:
    1. Load data from S3 into staging tables.
    2. Transform and insert data from staging tables into analytical tables.

    After execution, the data warehouse is prepared for analysis. 
    The connection to the Redshift database is then closed.

    Args:
        None

    Returns:
        None
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()