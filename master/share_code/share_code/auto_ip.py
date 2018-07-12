# -*- coding: utf-8 -*-
"""
Created on Fri Apr 13 21:51:29 2018

@author: Administrator
"""
import urllib
import urllib2
import time
import redis
from scrapy.selector import Selector
import requests

def get_url(url):     # 国内高匿代理的链接
    url_list = []
    for i in range(1,100):
        url_new = url + str(i)
        url_list.append(url_new)
    return url_list
def get_content(url):     # 获取网页内容
    user_agent = 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/49.0.2623.22 Safari/537.36 SE 2.X MetaSr 1.0'
    headers = {'User-Agent': user_agent}
    req = requests.get(url=url, headers=headers)
    
    '''
    req = urllib.request.Request(url=url, headers=headers)
    res = urllib.request.urlopen(req)
    content = res.read()
    return content.decode('utf-8')    
    '''
    return req

def get_info(content):      # 提取网页信息 / ip 端口
    datas_ip = Selector(content).xpath('//table[contains(@id,"ip_list")]/tr/td[2]/text()').extract()
    datas_head = Selector(content).xpath('//table[contains(@id,"ip_list")]/tr/td[6]/text()').extract()
    datas_port =Selector(content).xpath('//table[contains(@id,"ip_list")]/tr/td[3]/text()').extract()
    
    '''
    datas_ip = Selector(text=content).xpath('//table[contains(@id,"ip_list")]/tr/td[2]/text()').extract()
    datas_head = Selector(text=content).xpath('//table[contains(@id,"ip_list")]/tr/td[6]/text()').extract()
    datas_port =Selector(text=content).xpath('//table[contains(@id,"ip_list")]/tr/td[3]/text()').extract()
    '''
    
    #写入redis
    print('head: ',datas_head)
    try:        
        R=redis.Redis(host='localhost',port='6379')
    except Exception as e:
        print(e)
    count=0
    for head,ip,port in zip(datas_head,datas_ip,datas_port):
        p=R.zadd('share:auto_ip_pool',str(head).lower()+'://'+str(ip)+':'+str(port),count)
        if p:
            count+=1
    #print(datas_ip,datas_port)
    return datas_head,datas_ip,datas_port
def verify_ip(head,ip,port):    # 验证ip有效 
    user_agent ='Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/49.0.2623.22 Safari/537.36 SE 2.X MetaSr 1.0' 
    headers = {'User-Agent':user_agent}
    if 'https' in head:
        proxy = {'https':'%s://%s:%s'%(head.lower(),ip,port)}
    else:
        proxy = {'http':'%s://%s:%s'%(head.lower(),ip,port)}
    print(proxy)
    
    #proxy_handler = urllib.request.ProxyHandler(proxy)
    proxy_handler=urllib2.ProxyHandler(proxy)
    #opener = urllib.request.build_opener(proxy_handler)
    opener = urllib2.build_opener(proxy_handler)
    urllib2.install_opener(opener)

    #test_url = "https://www.baidu.com/"
    test_url = "http://quote.eastmoney.com/stocklist.html#sh"
    req = urllib2.Request(url=test_url,headers=headers)
    time.sleep(3)
    
    try:   
        R=redis.Redis(host='localhost',port='6379')
    except Exception as e:
        print(e)
    count=0
    try:
        res = urllib2.urlopen(req,timeout=1)
        #time.sleep(3)
        content = res.read()
        if content:
            print('that is ok')
            R.zadd('share:auto_ip_pool_ok',str(head).lower()+'://'+str(ip)+':'+str(port),count)
            count+=1
        else:
            print('its not ok')
    except urllib2.URLError as e:
        print(e.reason)
    except Exception as e:
	print(e)

if __name__ == '__main__':
    url = 'http://www.xicidaili.com/nn/'
    url_list = get_url(url)
    for i in url_list:
        print(i)
        content = get_content(i)
        time.sleep(3)
        data=get_info(content)
        for head,ip,port in zip(data[0],data[1],data[2]):
            verify_ip(head,ip,port)
