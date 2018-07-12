# -*- coding: utf-8 -*-
"""
Created on Fri Apr 20 00:57:45 2018

@author: Administrator
"""

from kazoo.client import KazooClient  
  
import time  
  
import logging  
logging.basicConfig()  
  
zk = KazooClient(hosts='127.0.0.1:2181')  
zk.start()  
  
# Determine if a node exists  
while True:  
    for ip in ['192.168.111.130']:
        if zk.exists("/ip_process/" + ip):  
            print ("%s is alive!"%ip)  
        else:  
            print ("%s is dead!"%ip) 
            break  
    time.sleep(6)  
  
zk.stop()  
