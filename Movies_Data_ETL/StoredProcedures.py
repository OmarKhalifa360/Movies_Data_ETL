import os
from google.cloud import storage
from os import read
import numpy as np
import pandas as pd


class google_bucket:
    
    # Creating an Environmental Variable for the service key configuration
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'ServiceKey_GoogleCloud.json'

    # Creating a storage client
    storage_client = storage.Client()

    ########################
    # List of class methods
    # print(dir(storage_client))
    ########################

    def __init__(self):
        pass
    
    def create_bucket(self, bucket_name, location = 'europe-west1'):
        '''
        Create a Google Cloud Stroge Bucket.
        Args:
            - bucket_name(str) - Name of the Bucket.
            - location(str, default = US) - Location.
        Returns:
            - True or False
        '''
        # Creating a new bucket
        self.bucket_name = bucket_name
        self.location = location

        # Set bucket's name and location
        bucket = self.storage_client.bucket(bucket_name = bucket_name)
        bucket.location = location
        
        try: 
            # Creating bucket
            bucket = self.storage_client.create_bucket(bucket)
            print('\nBucket Created Sucessfuly')
            print('Printing Bucket Details...\n')
            return print(vars(bucket))
        except Exception as e:
            print(e)

    def process_ratings(self, file_path = 'ml-100k/u.data'):

        '''
        Create Dataframe of movie ratings from the file u.data.
        Args:
            - file_path(str) - Path to u.data.
        Returns:
            - ratings(DataFrame)
        '''
        # Create a DataFrame for Movie Ratings
        ratings = pd.read_csv('ml-100k/u.data', delimiter = '\t', 
                        header = None, 
                        names = ['user_id', 'item_id', 'rating', 'timestamp'])
        return ratings


    def process_data(self, file_path = 'ml-100k/u.item'):
        '''
        Create Dataframe of movie names from the file u.item.
        Args:
            - file_path(str) - Path to u.item.
        Returns:
            - movies_df(DataFrame)
        '''

        self.file_path = file_path

        # Create a DataFrame for Movie Names
        with open('ml-100k/u.item', 'r', encoding = 'ISO-8859-1') as read_file:

            counter = 0
            df_columns = ['item_id', 'movie_name', 'release_timestamp']
            df_movies = pd.DataFrame(columns = df_columns)

            # Iteration through the lines in the file
            for line in read_file:

                # From each line extract the first three values
                fields = line.split('|')
                item_id, movie_name, release_timestamp = fields[0], fields[1], fields[2]
                movie_name[:len(movie_name) - len('striked')]

                # Aggregate line data
                line_data = [int(item_id), str(movie_name), release_timestamp]

                # Create a temp DataFrame , then append it to df_movies
                temp_df = pd.DataFrame(data=[line_data], columns = df_columns)
                df_movies = pd.concat([temp_df, df_movies], ignore_index = True)

                counter += 1

            # Sort values by item_id
            df_movies.sort_values(by = 'item_id', ascending = True, inplace = True)
        
        # Close file
        read_file.close()

        return df_movies

    def export_dataframe_to_csv(self, dataframe, csv_name):

        '''
        Export DataFrame to CSV.
        Args:
            - dataframe(Pandas.DataFrame) = Name of the DataFrame to Export.
            - csv_name(str) = Name of the CSV file
        Returns:
            - None
        '''

        self.dataframe = dataframe
        self.csv_name = csv_name

        try:
            dataframe.to_csv(csv_name + '.csv', index = False)
        except Exception as e:
            print(e)
        
    def load_data(self, blob_path, file_path, bucket_name):

        '''
        Load CSV files to Google Cloud Storage.
        Args:
            - blob_path(str) = Where the csv file should go in the bucket.
            - file_path(str) = Path to the CSV file
            - bucket_name(str) = Bucket to Load data into
        Returns:
            - True/False
        '''

        self.location_in_bucket = blob_path
        self.file_path = file_path
        self.bucket_name = bucket_name

        try:
            # Access bucket
            bucket = self.storage_client.get_bucket(bucket_name)

            # Create a blob from the bucket
            blob = bucket.blob(blob_name=blob_path)

            # Upload file
            blob.upload_from_filename(file_path)
            return True
        except Exception as e:
            print(e)

