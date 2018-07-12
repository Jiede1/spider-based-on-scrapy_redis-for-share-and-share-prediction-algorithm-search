# -*- coding: utf-8 -*-
"""
Created on Wed Apr 11 00:01:28 2018

@author: Administrator
"""
from scrapy.selector import Selector
import time
import requests
import redis
from multiprocessing import Process, Queue  


def get_url(url):     # 国内高匿代理的链接
    url_list = []
    for i in range(1,100):
        url_new = url + str(i)
        url_list.append(url_new)
    return url_list

def get_content(url):     # 获取网页内容
    user_agent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.84 Safari/537.36'
    headers = {'User-Agent': user_agent}
    req = requests.get(url=url, headers=headers)
    return req

def get_info(content):      # 提取网页信息 / ip 端口
    datas_ip = Selector(content).xpath('//table[contains(@id,"ip_list")]/tr/td[2]/text()').extract()
    datas_head = Selector(content).xpath('//table[contains(@id,"ip_list")]/tr/td[6]/text()').extract()
    datas_port =Selector(content).xpath('//table[contains(@id,"ip_list")]/tr/td[3]/text()').extract()
    
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
   
def verify_ip_one(old_queue,new_queue):    # 验证ip有效性
    while 1:
        data=old_queue.get()
        print(data)
        if data==0:
            break
        head=data[0].lower()
        ip=data[1]
        port=data[2]
        print('head,data,ip')
        
        user_agent ='Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/49.0.2623.22 Safari/537.36 SE 2.X MetaSr 1.0'
        accept_encoding ='gzip, deflate, sdch'  
        accept_language ='zh-CN,zh;q=0.8'  
        headers = {'User-Agent':user_agent,'Accept-Encoding':accept_encoding,'Accept-Language':accept_language}
        if 'https' in head:
            proxy = {'https':'%s://%s:%s'%(head,ip,port)}
        else:
            proxy = {'http':'%s://%s:%s'%(head,ip,port)}
        print(proxy)
    
        test_url = "https://www.baidu.com/"
        
        try:
            req = requests.get(url=test_url,proxies=proxy,headers=headers)
            status_code=req.status_code
            if status_code==200:
                print('that is ok')
                print(str(ip) + u":" + str(port))
                new_queue.put([head,ip,port])
            else:
                print('its not ok')
        except Exception as e:
            print('fall down')

def verif_ip(data):
    old_queue=Queue()
    for head,ip,port in zip(data[0],data[1],data[2]):
        old_queue.put([head,ip,port])               #往没验证过的queue加入数据
    print('verify ip.....')
    print('old_queue: ',old_queue.qsize())
    old_queue.put(0)  #终止条件
    new_queue=Queue()
    works = []  
    for i in range(1):  
        print('process %s'%i)
        works.append(Process(target=verify_ip_one, args=(old_queue,new_queue)))
    for work in works:
        print('process start')
        work.start()     
        work.join()
    '''
    for work in works:
        work.join() 
    '''
    try:   
        R=redis.Redis(host='localhost',port='6379')
    except Exception as e:
        print(e)
    for i in range(new_queue.qsize()):
        head,ip,port=new_queue.get()
        R.sadd('share:auto_ip_pool_ok',str(head).lower()+'://'+str(ip)+':'+str(port))
        print('insert one row')
    
        
if __name__ == '__main__':
    url = 'http://www.xicidaili.com/nn/'
    url_list = get_url(url)
    for i in url_list:
        print(i)
        content = get_content(i)
        time.sleep(3)
        data=get_info(content)
        verif_ip(data)
        '''
        for head,ip,port in zip(data[0],data[1],data[2]):
            verif_ip(head.lower(),ip,port)
        '''
            