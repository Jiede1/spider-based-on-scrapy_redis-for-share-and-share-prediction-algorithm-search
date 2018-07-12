# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/items.html

import scrapy
from scrapy.loader import ItemLoader
from scrapy.loader.processors import MapCompose, TakeFirst, Join


class ShareCodeItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    number = scrapy.Field()
    
    data = scrapy.Field()

class ShareLoader(ItemLoader):
    default_item_class = ShareCodeItem
    default_input_processor = MapCompose(lambda s: s.lstrip().replace('\r',''))#去掉左边空格
    default_output_processor = Join()
    description_out = Join()
