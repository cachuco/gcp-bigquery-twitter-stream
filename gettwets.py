import config
import tweepy
from tweepy.auth import OAuthHandler
from google.cloud import bigquery
from google.cloud import language
from google.cloud.language import enums
from google.cloud.language import types

#CONFIGURATION
APP_KEY = config.APP_KEY
APP_SECRET = config.APP_SECRET
OATH_TOKEN = config.OATH_TOKEN
OATH_TOKEN_SECRET = config.OATH_TOKEN_SECRET
BIGQUERY_DATESET = config.BIGQUERY_DATESET
BIGQUERY_TABLE = config.BIGQUERY_TABLE
BIGQUERY_QUERY_TABLE = config.BIGQUERY_QUERY_TABLE
GCP_SERVICE_ACCOUNT_JSON = config.GCP_SERVICE_ACCOUNT_JSON
HASHTAG = config.HASHTAG

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

#TWITTER AUTHENTICATION
auth = OAuthHandler(APP_KEY, APP_SECRET)
auth.set_access_token(OATH_TOKEN, OATH_TOKEN_SECRET)
api = tweepy.API(auth)

lasttweet = ( "SELECT created_at, id_str FROM " + BIGQUERY_QUERY_TABLE + " ORDER BY created_at DESC LIMIT 1" )
query_job = client.query(lasttweet)
last = query_job.result()
for tw in last:
    print(tw.id_str)
    last_id_saved = tw.id_str

try:
    api.verify_credentials()
    print("Authentication OK")
except:
    print("Error during authentication")
    
tweets = tweepy.Cursor(api.search, q='\"' + HASHTAG + '\"', lang='es', since_id=last_id_saved).items(200)

for tweet in tweets:
    #https://developer.twitter.com/en/docs/tweets/data-dictionary/overview/tweet-object.html

    #NLP analysis
    document = types.Document(
        content = tweet.text,
        type = enums.Document.Type.PLAIN_TEXT)
    sentiment = nlpClient.analyze_sentiment(document=document).document_sentiment;
    
    row = [tweet.id_str,str(tweet.created_at),tweet.text,tweet.source,tweet.user.id_str,tweet.user.name,tweet.user.screen_name,tweet.user.description,str(tweet.user.verified),str(tweet.user.created_at),str(tweet.user.geo_enabled),tweet.user.lang,str(sentiment.score), str(sentiment.magnitude)];
    print(tweet.id_str + " @" + tweet.user.screen_name + "\n" + " / " + str(sentiment.score) + " / " + str(sentiment.magnitude))
    rows_to_insert.append(row)

errors = client.insert_rows(table, rows_to_insert)

assert errors == []



