# gcp-bigquery-twitter-stream
Stream Twitter Data into BigQuery with tweepy


APP de twitter
Crear proyecto
Habilitar billin account
Habilitar BigQuery
- Definir schema
Habilitar NLP API
- Enable the required APIs and modify permissions
Crear instancia ComputeEngine

sudo apt update
sudo apt install python python-dev python3 python3-dev python3-pip

sudo python3 -m pip install tweepy google-api-python-client google-cloud google-cloud-vision google.cloud.bigquery google-cloud-language

sudo chmod +x /home/juanjo_cacho/gettweets.py 
sudo chmod +x /home/juanjo_cacho/updateSentiment.py 

sudo crontab -e

*/5 * * * * cd /home/juanjo_cacho && ./gettweets.py > /tmp/stream.log 2>&1
*/10 * * * * cd /home/juanjo_cacho && ./updateSentiment.py > /tmp/updatesentiment.log 2>&1