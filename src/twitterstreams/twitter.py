from logging import raiseExceptions
from multiprocessing import AuthenticationError
import requests 
import pandas 
import json 
import logging
import datetime
import os
import urllib3
from yaml import parse


log=logging.getLogger(__name__)



class TweetStream:

    def __init__(self,auth,user_agent="v2FilteredStream"):
        self.auth= auth
        self.user_agent=user_agent
        self.session=requests.Session()
        self.headers={}
        self.headers["User-Agent"]=self.user_agent
        self.headers["Authorization"]=f'Bearer {self.auth}'
        self.url='https://api.twitter.com/2/'
    

    def request(self, method, endpoint, query=None, query_required=False, query_method=None, json_payload=None,headers=None):
        
        """
        PARAMETERS:

        method
            Request method (GET, POST)

        endpoint
            endpoint from Twitter docs to call

        query
            Rules to pass into the stream if the query_method is ADD/ADD.VALUE, otherwise rules IDs

        query_required
            default is False, can be set to True for POST requests

        json_payload
            default is None for GET requests, but otherwise overwritten depending on query_method

        headers
            includes bearer token for authorization to be sent with the request object 
        
        """
        
        if headers is None:
            self.headers=headers

        if query is None and query_required:
            raise ValueError("You must pass query parameters to this endpoint")
        else:
            self.query=query

        if query_required and query_method:
            if query_method =='delete':
                json_payload={query_method:{'ids': query}}
            else:
                json_payload={query_method: query}
            print(json_payload)
            
        self.method=method

        if not self.auth:
            raise AuthenticationError("A auth is required to make the call")
       
       
        if headers is None:
            headers={}
            headers["User-Agent"]=self.user_agent
            headers["Authorization"]=f'Bearer {self.auth}'

        url=self.url+f'{endpoint}'

        try:
            response=self.session.request(method=method, url=url, headers=headers, json=json_payload,)

        except Exception as e:
            print(e)

        
        print(response.json())

        
        return response

    def parse_stream(self, tweet):
        tweetID=tweet['data']['id']
        tweetText=tweet['data']['text']
        matchingRuleID=tweet['matching_rules'][0]['id']
        return tweetID, tweetText, matchingRuleID

    def write_tweets_dict(self, dictionary):
        if len(dictionary)>0:    
            fileName='twitterStreaming'+str(datetime.datetime.now().strftime("%Y%m%d%H%M%S"))
            with open(fileName+'.json','w', encoding='utf-8') as f:
                    json.dump(dictionary, f)
                    log.info("Creating a file in the %s called %s", os.system('pwd'),fileName)




    def add_stream_rules(self, rule):
        return self.request(method="POST", endpoint="tweets/search/stream/rules", query=rule, query_required=True, query_method='add', 
        )


    def delete_stream_rules(self, rule):
        return self.request(method="POST", endpoint="tweets/search/stream/rules", query=rule, query_required=True, query_method='delete',)

    
    def get_stream_rules(self):
        return self.request(method="GET", endpoint="tweets/search/stream/rules", 
        )


    def stream_connect(self, method, endpoint, stream=True, timeout=4,max_tweets=100):
        self.open_connection=True
        self.timeout=timeout
        counter=0
        self.max_tweets=max_tweets
       
       
        tweets_dictionary=dict()
        #replace by schema
        tweets_dictionary['matchingRuleID']=[]
        tweets_dictionary['tweetID']=[]
        tweets_dictionary['tweetText']=[]
       

        try:
            try:
                with self.session.request(url=self.url+endpoint,method=method,headers=self.headers, stream=True, timeout=timeout) as resp:
                    if resp.status_code==200:
                        log.info("Stream connection is open")
                        for resp_line in resp.iter_lines():
                            if counter<max_tweets:
                                if resp_line:
                                    tweet=json.loads(resp_line)
                                    tweetID, tweetText, matchingRuleID=self.parse_stream(tweet)
                                    log.info("Processed %d tweets", counter)
                                    counter+=1
                                    tweets_dictionary['tweetID'].append(tweetID)
                                    tweets_dictionary['tweetText'].append(tweetText)
                                    tweets_dictionary['matchingRuleID'].append(matchingRuleID)


                            else:
                                break
                                self.open_connection=False

                        self.write_tweets_dict(tweets_dictionary)

                    else:
                        log.error("Stream encountered %d HTTPS error", resp.status_code)
                    
            
            except Exception as e: 
                       #log.error("Stream connection encounted an error")
                       raise e

        except Exception as e:
            #(requests.ConnectionError, requests.Timeout, ssl.SSLError, urllib3.exceptions.ReadTimeoutError) as exp:
            #log.error("Stream connection encounted an exception and is terminating")
            raise e
        finally:
            self.session.close()
            self.open_connection=False
            log.info("Stream connection closed")


