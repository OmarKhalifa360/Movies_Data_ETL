# movies_etl.py

from StoredProcedures import google_bucket

# Create an instance of class: google_bucket
google = google_bucket()

# Create a new bucket
google.create_bucket(bucket_name='strike_log_bucketv3')

# Process the ratings and movies files into DataFrames
ratings = google.process_ratings()
movies = google.process_data()

# Export DataFrames to CSV
google.export_dataframe_to_csv(dataframe=ratings, csv_name='ratings')
google.export_dataframe_to_csv(dataframe=movies, csv_name='movies')

# Load CSV files into the new Bucket
google.load_data(blob_path='project-alpha/ratings', file_path='ratings.csv', bucket_name='strike_log_bucketv2')
google.load_data(blob_path='project-alpha/movies', file_path='movies.csv', bucket_name='strike_log_bucketv2')
