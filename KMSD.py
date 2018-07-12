#coding:utf-8
from similarity import MSD
import pandas as pd
import numpy as np
import copy
import random

def local_MSD_layer(a,b):   #对应于出现断层数据
    l1=np.nonzero(a!=-1)[0]
    l2=np.nonzero(b!=-1)[0]
    l=list(set(l1) & set(l2))
    sim=MSD(a[l],b[l])
    return sim

#在这里我将欧式距离改为了MSD距离
def distEclud(vecA,vecB):
    return local_MSD_layer(vecA,vecB)
#随机初始化K个质心(质心满足数据边界之内)
def randCent(dataSet,k):
    #得到数据样本的维度
    n=np.shape(dataSet)[1]
    #初始化为一个(k,n)的矩阵
    centroids=np.mat(np.zeros((k,n)))
    #遍历数据集的每一维度
    for j in range(n):
        #得到该列数据的最小值
        minJ=min(dataSet[:,j])
        #得到该列数据的范围(最大值-最小值)
        rangeJ=float(max(dataSet[:,j])-minJ)
        #k个质心向量的第j维数据值随机为位于(最小值，最大值)内的某一值
        centroids[:,j]=minJ+rangeJ*np.random.rand(k,1)
    #返回初始化得到的k个质心向量
    return np.array(centroids)
#k-均值聚类算法
#@dataSet:聚类数据集
#@k:用户指定的k个类
#@distMeas:距离计算方法，默认欧氏距离distEclud()
#@createCent:获得k个质心的方法，默认随机获取randCent()
def kMeans(dataSet,k,distMeas=distEclud,createCent=randCent):
    dataSet=np.array(dataSet)
    #获取数据集样本数
    m=np.shape(dataSet)[0]
    #初始化一个(m,2)的矩阵
    clusterAssment=np.mat(np.zeros((m,2)))
    #创建初始的k个质心向量
    centroids=createCent(dataSet,k)
    #聚类结果是否发生变化的布尔类型
    clusterChanged=True
    #只要聚类结果一直发生变化，就一直执行聚类算法，直至所有数据点聚类结果不变化
    while clusterChanged:
        #聚类结果变化布尔类型置为false
        clusterChanged=False
        #遍历数据集每一个样本向量
        for i in range(m):
            #初始化最小距离最正无穷；最小距离对应索引为-1
            minDist=np.inf;minIndex=-1
            #循环k个类的质心
            for j in range(k):
                #计算数据点到质心的欧氏距离
                distJI=distMeas(centroids[j,:],dataSet[i,:].reshape(centroids[j,:].shape))
                #如果距离小于当前最小距离
                if distJI<minDist:
                    #当前距离定为当前最小距离；最小距离对应索引对应为j(第j个类)
                    minDist=distJI;minIndex=j
        #当前聚类结果中第i个样本的聚类结果发生变化：布尔类型置为true，继续聚类算法
        if clusterAssment[i,0] !=minIndex:clusterChanged=True
        #更新当前变化样本的聚类结果和平方误差
	print('minDist: ',minDist)
        clusterAssment[i,:]=minIndex,minDist**2
    #打印k-均值聚类的质心
    print centroids
    #遍历每一个质心
    for cent in range(k):
        #将数据集中所有属于当前质心类的样本通过条件过滤筛选出来
        ptsInClust=dataSet[np.nonzero(clusterAssment[:,0].A==cent)[0]]
        #计算这些数据的均值（axis=0：求列的均值），作为该类质心向量
        centroids[cent,:]=np.mean(ptsInClust,axis=0)
    #返回k个聚类，聚类结果及误差
    return centroids,clusterAssment
#算法如下：
#如果两个序列的0占比不多于30%，则取出这些序列作为新的DD，然后聚类，其他的作为最后一个聚类
def kmeans_MSD(DD,k=3):  #返回dict,value是DF
    newD=[]
    leftD=[]
    for i in range(DD.shape[1]):
	if len(np.nonzero(DD[:,i]==0)[0])<=int(0.3*DD.shape[0]):
	    if len(newD)==0:
		newD=copy.deepcopy(DD[:,i]).reshape(-1,1)
	    else:
		newD=np.concatenate((newD,copy.deepcopy(DD[:,i]).reshape(-1,1)),axis=1)
   	else:
	    if len(leftD)==0:
		leftD=copy.deepcopy(DD[:,i]).reshape(-1,1)
	    else:
		leftD=np.concatenate((leftD,copy.deepcopy(DD[:,i]).reshape(-1,1)),axis=1)
    if isinstance(leftD,np.ndarray):leftD=leftD.T
    if isinstance(newD,np.ndarray):newD=newD.T
    print('KMSD newD leftD: ',newD.shape,leftD.shape)
    centroids,clusterAss= kMeans(copy.deepcopy(newD),k) 
    print('clusterAss: ',clusterAss.shape)
    return centroids,np.array(clusterAss)
	

