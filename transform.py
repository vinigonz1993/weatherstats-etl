from extract import Extract
from message import message
from config import (
    SNOW_RANGES, RAIN_RANGES, LABELS, CLOUD_LABELS,
    CLOUD_RANGES
)

class Transform():
    '''
        Class to do the data transformation
    '''

    def __init__(self, data):
        '''
            Initializes the dataframe
        '''
        message('Transforming the data')
        self.data = data

    def clean_data(data):
        '''
            Method to clean the data and removes unnecessary columns.
            The remaining columns are:
            - snow
            - rain,
            - date,
            - avg_temperature,
            - min_temperature,
            - max_temperature,
            - avg_cloud_cover_8,
            - city
            This method also created classifications for columns such as
            cloud, snow and rain
        '''

        features_to_keep = [
            'snow',
            'rain',
            'date',
            'avg_temperature',
            'min_temperature',
            'max_temperature',
            'avg_cloud_cover_8',
            'city'
        ]
        df = Extract().df()
        df = df[features_to_keep]

        # Drop columns with more than 60% missing values
        df = df.loc[:, df.isnull().mean(axis=0) < 0.6]

        df.fillna(value = 0, inplace=True)

        snow_ranges = SNOW_RANGES
        rain_ranges = RAIN_RANGES
        cloud_ranges = CLOUD_RANGES
        cloud_labels = CLOUD_LABELS
        labels = LABELS

        def map_snow_fall_to_category(value):
            for i,r in enumerate(snow_ranges):
                if value >= r[0] and value < r[1]:
                    return labels[i]
            return None

        def map_rain_fall_to_category(value):
            for i,r in enumerate(rain_ranges):
                if value >= r[0] and value < r[1]:
                    return labels[i]
            return None

        def map_cloud_cover(value):
            for i,r in enumerate(cloud_ranges):
                if value >= r[0] and value < r[1]:
                    return cloud_labels[i]
            return None

        df['snow_fall_category'] = df['snow'].apply(map_snow_fall_to_category)
        df['rain_fall_category'] = df['rain'].apply(map_rain_fall_to_category)
        df['cloud_classification'] = df['avg_cloud_cover_8'].apply(map_cloud_cover)

        message('The data was transformed', 'green')

        return df
