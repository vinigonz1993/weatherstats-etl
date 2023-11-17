import os
import csv
import requests
import pandas as pd
from message import message

class Extract():
    '''
        Class to manage the following actions:
        - Download data from weatherstats.ca
        - Validate data
        - Transform data
    '''

    FOLDER = 'data_lake'
    BASE_FILE_NAME = 'weatherstats_'

    def __init__(
            self,
            cities = ['Ottawa', 'Toronto', 'Montreal', 'Vancouver'],
            data_type = 'daily',
            row_limit = '500'
        ):
        '''
            Initializes the class with the following optional arguments:
            - city
            - data_type
            - row_limit
        '''
        self.cities = cities
        self.data_type = data_type
        self.row_limit = str(row_limit)

    def download(self):
        '''
            Downloads the data from the data source and saves a csv
            file into the "data_lake" folder
        '''
        form_data = {
            'formdata': 'ok',
            'type': self.data_type,
            'limit': self.row_limit,
            'submit': 'Download'
        }

        for city in self.cities:
            message(f'Extracting data from: {city.upper()}')
            url = f"https://{city.lower()}.weatherstats.ca/download.html"

            response = requests.post(url, data=form_data)

            if response.status_code == 200:
                os.makedirs(self.FOLDER, exist_ok=True)

                filename = f"{self.FOLDER}/{self.BASE_FILE_NAME}{city}.csv"
                with open(filename, "w", newline="") as f:
                    writer = csv.writer(f)
                    for line in response.text.strip().split("\n"):
                        writer.writerow(line.split(","))

            else:
                pass

            message(f'Data from {city.upper()} extracted!', 'green')

        message('Data extracted!', 'green')

    def df(self):
        '''
            Returns a dataframe from the downloaded data
        '''

        dfs = []
        for city in self.cities:
            df = pd.read_csv(f'{self.FOLDER}/{self.BASE_FILE_NAME}{city}.csv')
            df['city'] = city
            dfs.append(df)

        df = pd.concat(dfs, axis = 0)


        return df
