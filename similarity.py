#coding:utf-8
'''
complete Time Series Trend Similarity
D(msd)= D(Euclid)*(2-ASD/SAD)
ASD --- Difference in absolute value
SAD --- manhaton distance
'''
import numpy as np

def MSD(a,b):
    if len(a)!=len(b):
	print('a,b:',len(a),len(b))
        print('length not equal,quit')
        return
    if not isinstance(a,np.ndarray):
        a=np.array(a).reshape(-1)
    if not isinstance(b,np.ndarray):
        b=np.array(b).reshape(-1)
    a=a.reshape(-1)
    b=b.reshape(-1)
    if (a==b).all():return 0
    #欧式距离
    Deuclid=np.linalg.norm(a-b)
    #print('Deuclid: ',Deuclid)
    #曼哈顿距离
    Dmahat=sum(abs(a-b))
    #print('mahaton: ',Dmahat)
    #ASD
    ASD=abs(sum(a-b))
    #print('ASD: ',ASD)
    #MSD
    msd=Deuclid*(2-ASD/Dmahat)
    #print('msd: ',msd)
    return msd
