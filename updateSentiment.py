import config
from google.cloud import bigquery
from google.cloud import language
from google.cloud.language import enums
from google.cloud.language import types

#CONFIGURATION
BIGQUERY_DATESET = config.BIGQUERY_DATESET
BIGQUERY_TABLE = config.BIGQUERY_TABLE
BIGQUERY_QUERY_TABLE = config.BIGQUERY_QUERY_TABLE
GCP_SERVICE_ACCOUNT_JSON = config.GCP_SERVICE_ACCOUNT_JSON

#BIGQUERY CONNECTION
client = bigquery.Client.from_service_account_json(GCP_SERVICE_ACCOUNT_JSON)
dataset_id = BIGQUERY_DATESET
table_id = BIGQUERY_TABLE
table_ref = client.dataset(dataset_id).table(table_id)
table = client.get_table(table_ref) 
rows_to_insert = []
last_id_saved = "";

#NLP CONFIGURATION
nlpClient = language.LanguageServiceClient().from_service_account_json(GCP_SERVICE_ACCOUNT_JSON)

listTweets = ( "SELECT id_str, text, sentiment_score, sentiment_magnitude FROM " + BIGQUERY_QUERY_TABLE + " WHERE sentiment_score IS NULL AND sentiment_magnitude IS NULL ORDER BY id_str ASC LIMIT 100" )
query_job = client.query(listTweets)
tweets = query_job.result()
for tweet in tweets:
    #NLP analysis
    document = types.Document(
        content = tweet.text,
        type = enums.Document.Type.PLAIN_TEXT,
        language = 'es')
    sentiment = nlpClient.analyze_sentiment(document=document).document_sentiment;
    
    print(tweet.id_str + " - " + str(sentiment.score) + " / " + str(sentiment.magnitude))
    updateRow = "UPDATE"  + BIGQUERY_QUERY_TABLE + "SET sentiment_score ='" + str(sentiment.score) + "', sentiment_magnitude ='" + str(sentiment.magnitude) + "' WHERE id_str ='" + tweet.id_str + "'"
    query_job_update = client.query(updateRow)
    update = query_job_update.result()