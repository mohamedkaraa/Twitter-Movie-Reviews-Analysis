import tweepy
from imdb import IMDb
from tweepy import OAuthHandler
from kafka import KafkaProducer
from elasticsearch import Elasticsearch
import json
import re
import argparse

# configure elasticsearch
elastic_index = "twitter"
elastic_host = "localhost"
es = Elasticsearch(hosts = elastic_host)

# configure kafka
kafkaProducer = KafkaProducer(bootstrap_servers = ['localhost:9092'],
                                value_serializer = lambda data:
                                json.dumps(data).encode("utf-8"))

# configure twitter
def initializeAuthenticationHandler(twitter_consumer_key,twitter_consumer_secret,twitter_access_token,twitter_access_token_secret):
    twitter_authentication_handler = OAuthHandler(consumer_key = twitter_consumer_key,
                                                consumer_secret = twitter_consumer_secret)
    twitter_authentication_handler.set_access_token(key = twitter_access_token,
                                                    secret = twitter_access_token_secret)
    return twitter_authentication_handler

# Initialize Twitter instance
def initializeTwitter(twitter_consumer_key,twitter_consumer_secret,twitter_access_token,twitter_access_token_secret):
    twitter = tweepy.API(initializeAuthenticationHandler(twitter_consumer_key,twitter_consumer_secret,twitter_access_token,twitter_access_token_secret),
                        wait_on_rate_limit = True)
    return twitter

# functions to clean tweets
def removeEmojis(text):
    regrex_pattern = re.compile(pattern = "["
        u"\U0001F600-\U0001F64F"  # emoticons
        u"\U0001F300-\U0001F5FF"  # symbols & pictographs
        u"\U0001F680-\U0001F6FF"  # transport & map symbols
        u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
                           "]+", flags = re.UNICODE)
    return regrex_pattern.sub(r'',text)


def cleanTweets(tweet):
    # remove links
    tweet = re.sub(r"http\S+", "", tweet)
    tweet = re.sub(r"https\S+", "", tweet)
    tweet = re.sub(r"www.\S+", "", tweet)
    # remove tags
    tweet = re.sub("/[A-Za-z0-9_]+","", tweet)
    # remove emojis
    tweet = removeEmojis(tweet)
    
    return tweet


# search for imdb movie
def getMovies(imdb, name):
    return imdb.search_movie(name)

def producer(movie_title, twitter_consumer_key, twitter_consumer_secret, twitter_access_token, twitter_access_token_secret):
    imdb = IMDb()
    twitterAPI = initializeTwitter(twitter_consumer_key,twitter_consumer_secret,twitter_access_token,twitter_access_token_secret)
    es.delete_by_query(index= elastic_index, body={"query": {"match_all": {}}})
    movie_title = "Spider-Man: No Way Home" 
    movies = getMovies(imdb,movie_title)
    json_titleField = "Title"
    json_tweetField = "tweet"

    for movie in movies:
        title = movie["title"]
        tweetsQuery = tweepy.Cursor(twitterAPI.search_tweets,\
                                    q = '"' + title + '" -filter:retweets -filter:mentions -filter:hashtags',\
                                    tweet_mode='extended', lang = "en").items()
        for tweet in tweetsQuery:
            tweet_text = tweet.full_text
            tweet_text = cleanTweets(tweet_text)
            if(tweet_text.lower() not in movie_title.lower()):
                print(tweet_text)
                print("********")
                json_output = '{"'+json_titleField+'":"'+title+'","'+json_tweetField+'":"'+tweet_text+'}'
                kafkaProducer.send("tweets", value = json_output)
    print("Twitter search finished")



parser = argparse.ArgumentParser(description="imdb movie twitter reviews")
parser.add_argument("--movie_title", type=str, help="Movie title")
parser.add_argument("--twitter_consumer_key", type=str, help="Twitter consumer key")
parser.add_argument("--twitter_consumer_secret", type=str, help="Twitter consumer secret")
parser.add_argument("--twitter_access_token", type=str, help="Twitter access token")
parser.add_argument("--twitter_access_token_secret", type=str, help="twitter access token secret")

args = parser.parse_args()        

producer(movie_title=args.movie_title, twitter_consumer_key=args.twitter_consumer_key, twitter_consumer_secret=args.twitter_consumer_secret, twitter_access_token=args.twitter_access_token, twitter_access_token_secret=args.twitter_access_token_secret)