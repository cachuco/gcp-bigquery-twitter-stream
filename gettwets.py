import tweepy
from tweepy.auth import OAuthHandler
from google.cloud import bigquery

#CONFIGURATION
APP_KEY = ""
APP_SECRET = ""
OATH_TOKEN = "-OI8LWdsWPejepUmLj9e6YGpaCDzMNAQ"
OATH_TOKEN_SECRET = ""
BIGQUERY_DATESET = ""
BIGQUERY_TABLE = ""
BIGQUERY_QUERY_TABLE = "``"
GCP_SERVICE_ACCOUNT_JSON = '';
HASHTAG = ""

#BIGQUERY CONNECTION
client = bigquery.Client.from_service_account_json(GCP_SERVICE_ACCOUNT_JSON)
dataset_id = BIGQUERY_DATESET
table_id = BIGQUERY_TABLE
table_ref = client.dataset(dataset_id).table(table_id)
table = client.get_table(table_ref) 
rows_to_insert = []
last_id_saved = "";

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

    tweets = tweepy.Cursor(api.search, q='\"' + HASHTAG + '\"', lang='es', since_id=last_id_saved).items(200)

    for tweet in tweets:
     #https://developer.twitter.com/en/docs/tweets/data-dictionary/overview/tweet-object.html
     row = [str(tweet.created_at),tweet.id_str,tweet.text,tweet.source,tweet.user.id_str,tweet.user.name,tweet.user.screen_name,tweet.user.description,str(tweet.user.verified),str(tweet.user.created_at),str(tweet.user.geo_enabled),tweet.user.lang];
     print(tweet.id_str + " " + tweet.user.name + "\n")
     rows_to_insert.append(row)

    errors = client.insert_rows(table, rows_to_insert)

    assert errors == []

except:
    print("Error during authentication")

