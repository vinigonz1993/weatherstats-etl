import os
from connection import run_sql, close_connection
from config import LABELS, CLOUD_LABELS
from message import message

class Load():
    '''
        Class to load the data to the database
    '''

    def __init__(self, df):
        self.df = df
        self.insert_cities()
        self.insert_records_fact_table()
        close_connection()

    def insert_cities(self):
        '''
            Runs the SQL query to insert the cities
            names and indexes into the city table
        '''
        queries = ()

        cities = list(self.df['city'].unique())

        for city in cities:
            index = cities.index(city)
            queries += (
                f"""
                    INSERT INTO city (
                        city_id,
                        city_name
                    ) VALUES (
                        {index},
                        '{city}'
                    )
                """,
            )
        run_sql(queries, 'Loading cities')

    def insert_records_fact_table(self):
        '''
            Inserts the transformed data into the fact
            table, using the foreign keys related to the object
            if it is the case
        '''
        queries = ()

        cities = list(self.df['city'].unique())

        message('Hold on, this might take some time!', 'yellow')
        message(f'{self.df.shape[0]} rows of data are being loaded', 'yellow')

        for index, row in self.df.iterrows():
            snow_classification_id = LABELS.index(row['snow_fall_category'])
            rain_classification_id = LABELS.index(row['rain_fall_category'])

            try:
                cloud_classification_id = CLOUD_LABELS.index(row['cloud_classification'])
            except:
                cloud_classification_id = 0
            city = cities.index(row['city'])

            queries += (
                f"""
                    INSERT INTO fact_table(
                        date,
                        city_id,
                        snow_classification_id,
                        rain_classification_id,
                        cloud_classification_id,
                        avg_temperature,
                        min_temperature,
                        max_temperature
                    ) VALUES (
                        '{row['date']}',
                        {city},
                        {snow_classification_id},
                        {rain_classification_id},
                        {cloud_classification_id},
                        {row['avg_temperature']},
                        {row['min_temperature']},
                        {row['max_temperature']}
                    )
                """,
            )

        run_sql(queries, 'Inserting data')

        message(f'{self.df.shape[0]} rows of data were loaded')


        message('The data is ready', 'green')