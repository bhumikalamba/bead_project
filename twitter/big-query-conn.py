import google.auth
from google.cloud import bigquery
from google.cloud import bigquery_storage


GOOGLE_APPLICATION_CREDENTIALS="C:/Users/Suren/Documents/bead_project/nice-forge-305606-0c1b603cf119.json"
# Explicitly create a credentials object. This allows you to use the same
# credentials for both the BigQuery and BigQuery Storage clients, avoiding
# unnecessary API calls to fetch duplicate authentication tokens.
# credentials, your_project_id = google.auth.default(
#     scopes=["https://www.googleapis.com/auth/cloud-platform"]
# )

cred = google.auth.load_credentials_from_file(GOOGLE_APPLICATION_CREDENTIALS)
your_project_id ='bead-project'

# Make clients.
bqclient = bigquery.Client(credentials=cred[0], project=cred[1],)
bqstorageclient = bigquery_storage.BigQueryReadClient(credentials=cred[0])

# Download query results.
query_string = """
SELECT DISTINCT(twitter_handle_id),twitter_handle_name FROM `nice-forge-305606.twitter_dataset.tweets_data`
"""

dataframe = (
    bqclient.query(query_string)
    .result()
    .to_dataframe(bqstorage_client=bqstorageclient)
)
print(dataframe.head())
dataframe.to_csv("twitter_IDS.csv",index=False)