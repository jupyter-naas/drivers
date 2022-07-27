try:
    import tweepy
except:
    !pip install tweepy --user
    import tweepy
import pandas as pd
import json
from typing import List
import datetime
import pydash
from numpy import inf

tweet_fields = ["author_id,created_at,source,public_metrics"]
tweet_personal_fields = ["author_id,created_at,source,public_metrics,non_public_metrics,organic_metrics"]


class Twitter:
    
    # Authenticate as an app.
    __bearer_token : str
    
    # Authenticate as a user.
    __consumer_key : str
    __consumer_secret : str
    __access_token : str
    __access_token_secret : str
    
    # Twitter v2 auth
    __app_client : tweepy.Client
    __user_client : tweepy.Client
    
    __me : pd.Series
    
    def connect(self, bearer_token:str, consumer_key:str, consumer_secret:str, access_token:str, access_token_secret:str) -> "Twitter":
        self.__bearer_token = bearer_token
        
        self.__app_client = tweepy.Client(
            bearer_token=self.__bearer_token
        )
        
        self.__consumer_key = consumer_key
        self.__consumer_secret = consumer_secret
        self.__access_token = access_token
        self.__access_token_secret = access_token_secret
        
        
        self.__user_client = tweepy.Client(
            consumer_key=consumer_key,
            consumer_secret=consumer_secret,
            access_token=access_token,
            access_token_secret=access_token_secret
        )
        
        self.__me = self.get_me()
        
        return self
    
    @property
    def app_client(self):
        return self.__app_client
    
    @property
    def user_client(self):
        return self.__user_client
    
    def get_user(self, username:str) -> pd.Series:
        users = self.__app_client.get_users(usernames=[username])
        if users is None:
            return None
        
        return pd.Series(users.data[0].data)
    
    def get_me(self):
        me = self.__user_client.get_me()
        if me is None:
            return None
        return pd.Series(me.data.data)
    
    def get_my_tweets(self, **kwargs):
        return self.get_users_tweets(self.__me.id, **kwargs)


    def get_users_tweets(self,
                         user_id:str,
                         tweet_count=200,
                         tweet_fields :List[str]=tweet_fields,
                         start_time=datetime.datetime.now() - datetime.timedelta(days=30),
                         end_time=datetime.datetime.now())-> pd.DataFrame:
        should_stop = False
        tweets_array = []
        next_token = None
        
        while len(tweets_array) < tweet_count and should_stop is False:
            tweets_left_to_fetch = tweet_count - len(tweets_array)
            
            if tweets_left_to_fetch > 100:
                max_results = 100
            elif tweets_left_to_fetch < 5:
                max_results = 5
            else:
                max_results = tweets_left_to_fetch
            
        
            tweets = self.__app_client.get_users_tweets(
                id=user_id,
                max_results=max_results,
                start_time=start_time,
                end_time=end_time,
                pagination_token=next_token,
            )
            next_token = pydash.get(tweets, 'meta.next_token', None)
            if next_token is None:
                should_stop = True

            is_own_tweets = user_id == self.__me.id
            for tweet in tweets.data:
                tweet_id = tweet.id


                if is_own_tweets is True:
                    rich_tweet_response = self.__user_client.get_tweet(tweet_id, tweet_fields=tweet_personal_fields, user_auth=True)
                    if len(rich_tweet_response.errors):
                        rich_tweet_response = self.__user_client.get_tweet(tweet_id, tweet_fields=tweet_fields, user_auth=True)
                else:
                    rich_tweet_response = self.__app_client.get_tweet(tweet_id, tweet_fields=tweet_fields, user_auth=False)


                rtd = rich_tweet_response.data
                
                tweets_array.append({
                    "TWEET_ID": rtd.id,
                    "TWEET_URL": f'https://twitter.com/{self.__me.username}/status/{rtd.id}',
                    "CREATED_AT": rtd.created_at,
                    "AUTHOR_ID": rtd.author_id,
                    "AUTHOR_NAME": self.__me['name'],
                    "AUTHOR_USERNAME": self.__me.username,
                    "TEXT": rtd.text,
                    "PUBLIC_RETWEETS": pydash.get(rtd, 'public_metrics.retweet_count', 0),
                    "PUBLIC_REPLIES": pydash.get(rtd, 'public_metrics.reply_count', 0),
                    "PUBLIC_LIKES": pydash.get(rtd, 'public_metrics.like_count', 0),
                    "PUBLIC_QUOTES": pydash.get(rtd, 'public_metrics.quote_count', 0),
                    "ORGANIC_RETWEETS": pydash.get(rtd, 'organic_metrics.retweet_count', 0),
                    "ORGANIC_REPLIES": pydash.get(rtd, 'organic_metrics.reply_count', 0),
                    "ORGANIC_LIKES": pydash.get(rtd, 'organic_metrics.like_count', 0),
                    "ORGANIC_QUOTES": pydash.get(rtd, 'organic_metrics.quote_count', 0),
                    "USER_PROFILE_CLICKS": pydash.get(rtd, 'non_public_metrics.user_profile_clicks', 0),
                    "IMPRESSIONS": pydash.get(rtd, 'non_public_metrics.impression_count', 0),
                })
                
        # Create final dataframe
        as_types = {
            "PUBLIC_RETWEETS": int, 
            "PUBLIC_REPLIES": int,
            "PUBLIC_LIKES": int,
            "PUBLIC_QUOTES": int,
            "ORGANIC_RETWEETS": int,
            "ORGANIC_REPLIES": int,
            "ORGANIC_LIKES": int,
            "ORGANIC_QUOTES": int,
            "USER_PROFILE_CLICKS": int,
            "IMPRESSIONS": int
        }
        df = pd.DataFrame(tweets_array).astype(as_types)
        df["ENGAGEMENTS"] = (df["PUBLIC_RETWEETS"] + df["PUBLIC_REPLIES"] + df["PUBLIC_LIKES"] + df["PUBLIC_QUOTES"] + df["USER_PROFILE_CLICKS"])
        df["ENGAGEMENT_RATE"] = df["ENGAGEMENTS"] / df["IMPRESSIONS"]
        df = df.round({'ENGAGEMENT_RATE': 4})
        df = df.fillna({"ENGAGEMENT_RATE": 0})
        df['ENGAGEMENT_RATE'] = df['ENGAGEMENT_RATE'].replace(inf, 0)
        df['ENGAGEMENT_RATE'] = df['ENGAGEMENT_RATE'].apply(lambda x: 0 if x < 0 else x)        
        return df.reset_index(drop=True)
        
    
twitter = Twitter()