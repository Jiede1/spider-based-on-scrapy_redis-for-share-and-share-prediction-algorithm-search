# -*- coding: utf-8 -*-
"""
Created on Fri Apr 20 19:18:54 2018

@author: Administrator
"""
import time
import logging
from scrapy import signals
from scrapy.exceptions import NotConfigured
logger = logging.getLogger(__name__)

class SpiderOpenCloseLogging(object):

    def __init__(self, item_count,idle_number,crawler):
        self.item_count = item_count

        self.items_scraped = 0

	self.idle_count = 0

	self.idle_list = []
	
	self.crawler = crawler

	self.idle_number = idle_number

    @classmethod
    def from_crawler(cls, crawler):
        # first check if the extension should be enabled and raise

        # NotConfigured otherwise

        if not crawler.settings.getbool('MYEXT_ENABLED'):

            raise NotConfigured
	#idle_number
	idle_number = crawler.settings.getint('IDLE_NUMBER', 10)

        # get the number of items from settings

        item_count = crawler.settings.getint('ITEM_NUMBER', 10000000)

        # instantiate the extension object

        ext = cls(item_count,idle_number,crawler)

        # connect the extension object to signals

        crawler.signals.connect(ext.spider_opened, signal=signals.spider_opened)

        crawler.signals.connect(ext.spider_closed, signal=signals.spider_closed)

        crawler.signals.connect(ext.item_scraped, signal=signals.item_scraped)

        crawler.signals.connect(ext.spider_idle, signal=signals.spider_idle)

        # return the extension object

        return ext

    def spider_opened(self, spider):
        logger.info("opened spider %s redis spider Idle, Continuous idle limit： %d", spider.name, self.idle_number)

    def spider_closed(self, spider,reason='finished'):  #默认结束reason是finished，如果spider是被引擎的 close_spider 方法所关闭，则其为调用该方法时传入的 reason 参数(默认为 'cancelled')
        logger.info("closed spider %s, idle count %d , Continuous idle count %d ,closed reason %s",
                spider.name, self.idle_count, len(self.idle_list),reason)

    def item_scraped(self, item, spider):
	if item:
            self.items_scraped += 1
	    print('self.items: ',item)
            if self.items_scraped % self.item_count == 0:
                spider.log("scraped %d items" % self.items_scraped)

    def spider_idle(self, spider):
        self.idle_count += 1                        # 空闲计数
        print('idle_count:',self.idle_count)
        self.idle_list.append(time.time())       # 每次触发 spider_idle时，记录下触发时间戳
        idle_list_len = len(self.idle_list)         # 获取当前已经连续触发的次数
        if idle_list_len > 8:
           # 连续触发的次数达到配置次数后关闭爬虫
           logger.info('\n continued idle number exceed {} Times'
                        '\n meet the idle shutdown conditions, will close the reptile operation'
                        '\n idle start time: {},  close spider time: {}'.format(8,
                                                                              self.idle_list[0], self.idle_list[-1]))
	   self.crawler.engine.close_spider(spider, 'closespider_ForNullRun')
