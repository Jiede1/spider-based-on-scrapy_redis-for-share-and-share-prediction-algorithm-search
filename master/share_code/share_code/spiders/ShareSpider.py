# -*- coding: utf-8 -*-

#redis-cli sadd share:start_urls http://quote.eastmoney.com/stocklist.html#sh
#scrapy crawl share
#del share:download_url share:temp_urls share:share_code


from scrapy_redis.spiders import RedisSpider
import redis
import scrapy
from scrapy import log
from share_code.items import ShareLoader
from scrapy.selector import Selector
import re
from kazoo.client import KazooClient
#rom scrapy import log

class ShareSpider(RedisSpider):
    name = "share"
    #allowed_domains = ["share.org"]redis_key = 'share:start_urls'
    start_urls = 'http://quote.eastmoney.com/stocklist.html#sh'
    redis_key = 'share:start_urls'
    
    temp_url_piece = 'http://quotes.money.163.com/trade/lsjysj_'  #中间url的片段
    download_url_piece = 'http://quotes.money.163.com/service/chddata.html?code=0'

    zk = KazooClient(hosts='127.0.0.1:2181')  
    zk.start()  
    
    # Ensure a path, create if necessary  
    zk.ensure_path("/ip_process")  
      
    # Create a node with data  
    zk.create("/ip_process/192.168.111.130",  
              value=b"ok", ephemeral=True)  

    pool=redis.ConnectionPool(host='localhost', port=6379, decode_responses=True)
    
    def parse(self, response):
        self.log('parse begin!',level=log.INFO)
        #r = ShareLoader(response=response)
        #response.encoding='GB2312'
        node_list = Selector(response=response).xpath('//*[@id="quotesearch"]/ul[1]/li/a/text()').extract()
        #print(node_list)
        code_list = []
        count = 1 #zadd score
        totalCount = 0 #total share url 
        
        R = redis.Redis(connection_pool=self.pool)
        
        self.log('Redis connect success!',level=log.INFO)
        for node in node_list:
            #r.add_value('number',node)
            try:
                code = re.match(r'.*?\((\d+)\)', node).group(1)
                print ('code: ',code)
                code_list.append(code)
                p0 = R.zadd('share:share_code',code,count)  #增加code进sorted_set
                if p0==1:  #无重复
                    print('Add share code success')
                    totalCount += 1
                    print('code totalCount +=1')
                    p1 = R.sadd('share:temp_urls',self.temp_url_piece +str(code) + '.html')
                    if p1 ==1: #无重复
                        print('share:temp_urls count: ',R.scard('share:temp_urls'))
                        temp_url = self.temp_url_piece +str(code) + '.html'
                        count += 1
                        yield scrapy.Request(url=temp_url,callback=self.parse2,meta={'code':code})
                        self.log('parse end!',level=log.INFO)
		else:
		    print('Add share code duplicate')
            except Exception as e:
                print(e)
                continue
                        
    def parse2(self,response):
	#print('response body: ',response.body)
        if response.status == 200:
	    #print('url: ',response.url)
	    #print('response.body: ',response.body)
            code = response.meta['code']
            #print('__init__: ',Selector(response=response).xpath('//input[@name="date_start_type"]/@value').extract())
            if Selector(response=response).xpath('//input[@name="date_start_type"]/@value').extract():
                start_date = Selector(response=response).xpath('//input[@name="date_start_type"]/@value').extract()[0].replace('-','')
	    else:
		start_date = Selector(response=response).xpath('//input[@type="text"]/@value').extract()[0].replace('-','')
            end_date = Selector(response=response).xpath('//input[@name="date_end_type"]/@value').extract()[0].replace('-','')
            print('start_date: ',start_date)
            download_url = self.download_url_piece+str(code)+"&start="+str(start_date)+"&end="+str(end_date)+"&fields=TCLOSE;HIGH;LOW;TOPEN;LCLOSE;CHG;PCHG;TURNOVER;VOTURNOVER;VATURNOVER;TCAP;MCAP"
            #yield scrapy.Request(url=download_url,meta={'code',code},callback=self.parse3)
            r = ShareLoader(response=response)
            pool=redis.ConnectionPool(host='localhost', port=6379, decode_responses=True)
            R = redis.Redis(connection_pool=pool)
            
            p2 = R.sadd('share:download_url',download_url)    
                
            if p2:
                print('download_url success')
                print('download_url: ',download_url)
                
                r.add_value('number',code)
                r.add_value('data',download_url)
                self.log('Add download_url one more', level=log.INFO) 
                
            return r.load_item()
        else:
            self.log('Response not 200!! ',level=log.WARNING)
