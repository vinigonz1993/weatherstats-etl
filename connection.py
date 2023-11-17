from psycopg2 import connect
from message import message
from config import (
    SNOW_RANGES, LABELS, RAIN_RANGES,
    CLOUD_LABELS, CLOUD_RANGES
)

# Database connection
HOST = 'HOST'
DATABASE_NAME = 'DATABASE_NAME'
USER = 'USER'
PASSWORD = 'PASSWORD'

conn = connect(
    host=HOST,
    database=DATABASE_NAME,
    user=USER,
    password=PASSWORD
)

queries = ()

def close_connection():
    '''
        Closes the database connection
    '''
    conn.close()
    message('Operation completed', 'green')
    print('')

def run_sql(queries, msg = None):
    '''
        Runs the SQL queries
    '''
    i = 0
    try:
        cursor = conn.cursor()
        if msg:
            print('')
            message(f'--- Query: {msg} ---')

        for command in queries:
            i += 1
            print(f'Completed (%): {round(i/len(queries) * 100, 2)}', end="\r")
            cursor.execute(command)
        print('')

        conn.commit()
        message('--- Query completed ---')
        message('')

    except Exception as error:
        message(error, 'red')
        conn.rollback()

def create_tables():
    '''
        Creates the initial tables.
        The first commands are drops, to restart the database
        to restart to the initial state
    '''
    return (
        """
            DROP TABLE IF EXISTS fact_table
        """,
        """
            DROP TABLE IF EXISTS snow_classification
        """,
        """
            DROP TABLE IF EXISTS city
        """,
        """
            DROP TABLE IF EXISTS rain_classification
        """,
        """
            DROP TABLE IF EXISTS cloud_classification
        """,
        """
            CREATE TABLE IF NOT EXISTS snow_classification (
                snow_classification_id INTEGER PRIMARY KEY,
                snow_classification_name VARCHAR(255),
                min INTEGER,
                max INTEGER
            )
        """,
        """
            CREATE TABLE IF NOT EXISTS city (
                city_id INTEGER PRIMARY KEY,
                city_name VARCHAR(255)
            )
        """,
        """
            CREATE TABLE IF NOT EXISTS rain_classification (
                rain_classification_id INTEGER PRIMARY KEY,
                rain_classification_name VARCHAR(255),
                min INTEGER,
                max INTEGER
            )
        """,
        """
            CREATE TABLE IF NOT EXISTS cloud_classification (
                cloud_classification_id INTEGER PRIMARY KEY,
                cloud_classification_name VARCHAR(255),
                min INTEGER,
                max INTEGER
            )
        """,
        """
            CREATE TABLE IF NOT EXISTS fact_table (
                date DATE,
                city_id INTEGER NOT NULL,
                snow_classification_id INTEGER NOT NULL,
                rain_classification_id INTEGER NOT NULL,
                cloud_classification_id INTEGER NOT NULL,
                avg_temperature DECIMAL,
                min_temperature DECIMAL,
                max_temperature DECIMAL,

                CONSTRAINT fk_city
                FOREIGN KEY(city_id) REFERENCES city(city_id),
                CONSTRAINT fk_snow_classification
                    FOREIGN KEY(snow_classification_id)
                    REFERENCES snow_classification(snow_classification_id),
                CONSTRAINT fk_rain_classification
                    FOREIGN KEY(rain_classification_id)
                    REFERENCES rain_classification(rain_classification_id),
                CONSTRAINT fk_cloud_classification
                    FOREIGN KEY(cloud_classification_id)
                    REFERENCES cloud_classification(cloud_classification_id)
            )
        """,
    )

def load_data():
    '''
        Loads the initial data
    '''
    sql = ()
    for row in SNOW_RANGES:
        index = SNOW_RANGES.index(row)
        sql += (
            f"""
            INSERT INTO snow_classification (
                snow_classification_id,
                snow_classification_name,
                min,
                max
            ) VALUES (
                {index},
                '{LABELS[index]}',
                {row[0]},
                {row[1]}
            )
            """,
        )

    for row in RAIN_RANGES:
        index = RAIN_RANGES.index(row)
        sql += (
            f"""
                INSERT INTO rain_classification (
                    rain_classification_id,
                    rain_classification_name,
                    min,
                    max
                ) VALUES (
                    {index},
                    '{LABELS[index]}',
                    {row[0]},
                    {row[1]}
                )
            """,
        )

    for row in CLOUD_RANGES:
        index = CLOUD_RANGES.index(row)
        sql += (
            f"""
                INSERT INTO cloud_classification (
                    cloud_classification_id,
                    cloud_classification_name,
                    min,
                    max
                ) VALUES (
                    {index},
                    '{CLOUD_LABELS[index]}',
                    {row[0]},
                    {row[1]}
                )
            """,
        )

    return sql

def initialize_db():
    '''
        Initializes the database
    '''
    queries = ()
    queries += create_tables()
    queries += load_data()
    run_sql(queries, 'Starting up the database')

