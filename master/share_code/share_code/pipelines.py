# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html
#from hdfs import Client
from scrapy.exceptions import DropItem
import logging
logger=logging.getLogger(__name__)

class ShareCodePipeline(object):
     # 初始化方法
    def __init__(self):
        logger.info("ShareCodePipeline __init__")
	'''
   	try:
            self.client=Client('http://192.168.111.130:50070')
        except Exception as e:
            print(e)
	'''
 
    def process_item(self, item, spider):
        logger.info("ShareCodePipeline process_item")
        
        if item['number']:
            number=item['number']
	    logger.info('number exists')
        else:
           raise DropItem('Missing number in %s'%item)
        if item['data']:
            data=item['data']
	    logger.info('data exists')
            #print('data:',data,'\n\n')
        else:
           raise DropItem('Missing data in %s'%item)
        
        data_str=data.encode('utf-8')   #内含中文，先编码成utf-8
	logger.info('ShareCodePipeline process_item success')
        '''       
        try:
            print('begin write')
            self.client.write('/sdbadmin/hadoop/input/'+str(number)+'.csv',data=data_str)
            print('end write')
        except Exception as e:
            print(e)
        '''
