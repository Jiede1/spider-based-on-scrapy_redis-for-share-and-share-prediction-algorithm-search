# -*- coding: utf-8 -*-
"""
Created on Fri Apr 13 17:35:23 2018

@author: Administrator
"""

from multiprocessing import Process, Queue
 
def f(q,n):
    q.put([42, n, 'hello'])
 
if __name__ == '__main__':
    q = Queue()
    p_list=[]
    for i in range(3):
        p = Process(target=f, args=(q,i))
        p_list.append(p)
        p.start()
    print(q.get())
    print(q.get())
    print(q.get())
    for i in p_list:
        i.join()