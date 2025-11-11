from google.cloud import bigquery

# The client will automatically use the credentials from ADC
client = bigquery.Client()

query = "SELECT * FROM `comp6231-project.first_test.stock` LIMIT 10"
query_job = client.query(query)  # API request

for row in query_job.result():
    print(row)