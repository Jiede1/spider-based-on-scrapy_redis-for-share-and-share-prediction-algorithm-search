# -*- coding: utf-8 -*-
"""
Created on Mon Mar 26 13:42:52 2018

@author: Administrator
"""
import time
from scrapy_redis.spiders import RedisSpider
import redis
from scrapy import log
from share_code.items import ShareLoader
#from scrapy.selector import Selector
import re
from kazoo.client import KazooClient   
import sys
reload(sys)
sys.setdefaultencoding('gbk')

class SlaveSpider(RedisSpider):
    name = "spider"
    download_delay=2
    #allowed_domains = ["spider.org"]
    redis_key = 'share:download_url'
    
    zk = KazooClient(hosts='127.0.0.1:2181')  
    zk.start()  
    
    # Ensure a path, create if necessary  
    zk.ensure_path("/ip_process")  
      
    # Create a node with data  
    zk.create("/ip_process/192.168.111.129",  
             value=b"ok", ephemeral=True)  
    
    pool=redis.ConnectionPool(host='192.168.111.130', port=6379, decode_responses=True)
      

    def parse(self, response):
	#print('response.body: ',response.body.decode('gbk'))  #str
	print('response.encoding: ',response.encoding)
	#print('body: ',response.body)
        #print('response.request.meta: ',response.request.meta)
        self.log('parse begin!',level=log.INFO)
        try:
            #global pool
            R=redis.Redis(connection_pool=self.pool)
        except Exception as e:
            self.log(e,level=log.ERROR)
        print('response.url: ',response.url)    
        if R.sismember('share:dupefilter_bak',response.url):  #如果url不重复
            self.log('download_url repeat! stop this chance and continue',level=log.DEBUG)   
            #return None  
        else:    
            #打印user-agent
            print('user-agent: ',response.request.headers['User-Agent'])
	    time.sleep(10)
            el=ShareLoader(response=response)
           
            #text=str(response.body,encoding='gbk')
	    text=str(response.body) if type(response.body) != 'str' else response.body
            el.add_value('data',text)
            
            pat=re.compile('.*?code=0(\d+).*?')  #正则表达式，取出code
            download_url=response.url
            code=pat.findall(download_url)
            print('code: ',code)
            
            el.add_value('number',code)
                
            R.sadd('share:dupefilter_bak',download_url) #储存已经spider的网页url，实现去重     
            
            self.log('parse end!',level=log.INFO)
            
            return el.load_item()
