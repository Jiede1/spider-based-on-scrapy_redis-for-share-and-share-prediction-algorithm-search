# -*- coding: utf-8 -*-
"""
Created on Fri Apr 20 01:08:28 2018

@author: Administrator
"""

class zoo:
    b=1
    def __init__(self):
        print('init',self.b)
    @classmethod
    def ok(cls,iin):
        print('classmethod: ',iin)
    print('hello')
    global a
    a=1
    a+=1
    print(a)
zoo()