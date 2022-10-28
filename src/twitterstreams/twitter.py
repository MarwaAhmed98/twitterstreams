from logging import raiseExceptions
from multiprocessing import AuthenticationError
import requests 
import pandas 
import json 
import logging


log=logging.getLogger(__name__)



class TweetStream:

    def __init__(self,auth,user_agent="v2FilteredStream"):

        self.auth= auth
        self.user_agent=user_agent
        self.session=requests.Session()
    

    def request(self, method, endpoint, query=None, query_required=False, query_method=None, json_payload=None,headers=None,stream=False):
        
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


        path=f'/2/{endpoint}'

        url='https://api.twitter.com'+path



        try:
            response=self.session.request(method=method, url=url, headers=headers, json=json_payload,)

        except Exception as e:
            print(e)

        
        print(response.json())

        
        return response


    def add_stream_rules(self, rule):
        return self.request(method="POST", endpoint="tweets/search/stream/rules", query=rule, query_required=True, query_method='add', 
        )


    def delete_stream_rules(self, rule):
        return self.request(method="POST", endpoint="tweets/search/stream/rules", query=rule, query_required=True, query_method='delete',)

    
    def get_stream_rules(self):
        return self.request(method="GET", endpoint="tweets/search/stream/rules", 
        )


    def initiate_stream(self):
       return self.request(method="GET", endpoint="tweets/search/stream",stream=True )
       