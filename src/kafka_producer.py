# https://developer.twitter.com/en/docs/twitter-api/tweets/filtered-stream/integrate/build-a-rule
import tweepy
import logging
from kafka import KafkaProducer
import configparser

# Start the server


# import subprocess

# service_started = subprocess.run(["bash", "/home/bdm/tweet/src/start_kafka_server.sh"])
def get_rule():

    with open('/home/bdm/tweet/data/company_list.txt') as f:
        tmp_list = f.readlines()

    rule ="("
    for  i in range(len(tmp_list)):
        rule = rule+tmp_list[i].strip()+" or "

    rule = rule+tmp_list[len(tmp_list)-1].strip()+")"
    
    return rule


"""API ACCESS KEYS"""

config = configparser.ConfigParser()
config.read('/home/bdm/tweet/data/twitter-app-credentials.txt')
#consumer_key = config['DEFAULT']['consumerKey']
#consumer_secret = config['DEFAULT']['consumerSecret']
#access_key = config['DEFAULT']['accessToken']
#access_secret = config['DEFAULT']['accessTokenSecret']
bearer_token = config['DEFAULT']['bearerToken']


producer = KafkaProducer(bootstrap_servers='localhost:9092')
search_term = ['Bitcoin','Tesla']
topic_name = 'stream-app'



class TweetListener(tweepy.StreamingClient):

    def on_data(self, raw_data):
        logging.info(raw_data)
        producer.send(topic_name, value=raw_data)
        print(raw_data)
        return True

    def on_error(self, status_code):
        print(status_code)

# Press the green button in the gutter to run the script.


if __name__ == '__main__':

   


    twitter_stream = TweetListener(bearer_token)
    twitter_stream.add_rules(tweepy.StreamRule(get_rule()+' lang:en -is:retweet'))
    twitter_stream.filter()
    # twitter_stream Auth object
'''
    auth = tweepy.OAuth2BearerHandler(bearer_token)
    api = tweepy.API(auth)

    # Create stream and bind the listener to it
    stream = tweepy.Stream(auth, listener=TweetListener(api))

    # Custom Filter rules pull all traffic for those filters in real time.
    #stream.filter(track = ['love', 'hate'], languages = ['en'])
    '''



