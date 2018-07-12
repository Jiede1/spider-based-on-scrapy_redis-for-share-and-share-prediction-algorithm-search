# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html
from hdfs import Client
from scrapy.exceptions import DropItem


class ShareCodePipeline(object):
    def __init__(self,client):
        print("ShareCodePipeline __init__")
	self.client=client
	'''
	try:
            client=Client('http://192.168.111.130:50070')
	except Exception as e:
            print(e)
	self.client=client
	'''
    @classmethod
    def from_settings(cls,settings):
	hdfs_master=settings['HDFS_MASTER']
	hdfs_address=settings['HDFS_ADDRESS']
	try:
       	    client=Client('http://'+str(hdfs_master)+':'+str(hdfs_address))	
	except Exception as e:
	    print(e)

	return cls(client)

    def process_item(self, item, spider):
        print("ShareCodePipeline process_item")
        
        if item['number']:
            number=item['number']
        else:
           raise DropItem('Missing number in %s'%item)
        if item['data']:
            data=item['data']
        else:
           raise DropItem('Missing data in %s'%item)
        
        data_str=data   #内含中文，先编码成utf-8
        '''
	try:
            client=Client('http://192.168.111.130:50070')
	except Exception as e:
            print(e)
	'''

        try:
            print('begin write')
	    if not self.client.content('/sdbadmin/hadoop/input/'+str(number)+'.csv',strict=False):
            	self.client.write('/sdbadmin/hadoop/input/'+str(number)+'.csv',data=data_str,encoding='utf-8')
		print('hdfs client close!')
            	print('end write')
	    else:
  		print('dupilicate data!')
        except Exception as e:
            print(e)

        return item 
