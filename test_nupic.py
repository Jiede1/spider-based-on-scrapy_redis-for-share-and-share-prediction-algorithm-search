#coding:utf-8

import time
import numpy as np
import pandas as pd
import sklearn
from hdfs.client import Client
from similarity import MSD
import copy
from sklearn.neural_network import MLPRegressor
from sklearn import preprocessing
from sklearn.model_selection import GridSearchCV
from KMSD import kmeans_MSD
from share_experiment import swarm
from nupic.frameworks.opf.model_factory import ModelFactory
from nupic_output import NuPICFileOutput, NuPICPlotOutput
from nupic.swarming import permutations_runner
'''
filepath='/sdbadmin/hadoop/input'
try:
    client=Client('http://192.168.111.130:50070')
except Exception as e:
    print(e)

dirs=client.list(filepath)
'''

import os
filepath='/opt/share_code_data'
dirs=os.listdir(filepath)

#将hdfs本地化
print('there are %d shares'%(len(dirs)))
'''
try:
    for i in range(len(dirs)):
        client.download(filepath+'/'+dirs[i],'/opt/share_code_data/'+dirs[i])
	print('download success')
except Exception as e:
    print(e)
'''
#即使是使用nupic，也考虑预处理
min_max_scaler = preprocessing.MinMaxScaler(feature_range=(0, 1))
DD=pd.DataFrame([])
for i in range(len(dirs)):
    df=pd.read_csv('/opt/share_code_data/'+dirs[i],index_col=0) #利用第一列做索引
    if len(DD)==len(df) and len(df)!=0:
        trun = min_max_scaler.fit_transform(copy.deepcopy(df.iloc[:,5]).values.reshape(-1, 1) )
        DD[dirs[i].strip().split('.')[0]]=trun
    elif len(df)!=0:
        cols=DD.columns
	trun = min_max_scaler.fit_transform(copy.deepcopy(df.iloc[:,5]).values.reshape(-1,1))
        DD=pd.concat([DD,pd.DataFrame(trun)],axis=1)   #长度不一致的合并
        f=list(cols)
        f.append(dirs[i].strip().split('.')[0])
        DD.columns=f
DD.fillna(0, inplace=True)

print('DD shape: ',np.shape(DD))
print(DD.head()) #[6690 rows x 169 columns]
#time.sleep(6)
clusters_DF=kmeans_MSD(DD.values,k=3)   # 返回一个dict，里面包含了所有的聚类,k是聚类数
clusterAss=clusters_DF[1]

newD=[]
leftD=[]
for i in range(DD.shape[1]):
    if len(np.nonzero(DD.values [:,i]==0)[0])<=int(0.3*DD.shape[0]):
        if len(newD)==0:
            newD=copy.deepcopy(DD.iloc[:,i])
        else:
            newD=pd.concat([newD,copy.deepcopy(DD.iloc[:,i])],axis=1)
    else:
        if len(leftD)==0:
            leftD=copy.deepcopy(DD.iloc[:,i])
        else:
            leftD=pd.concat([leftD,copy.deepcopy(DD.iloc[:,i])],axis=1)

ser=set(list(np.array(clusterAss[:,0])))  #聚类数
print('cluster leibie: ',ser)

#对newD的数据进行操作
for clu in ser:
    if str(clu)=='0.0':continue
    print('newD:',newD.shape,clusterAss.shape)
    Data=copy.deepcopy(newD.iloc[:,np.nonzero(clusterAss[:,0]==clu)[0]])    #选出某一聚类的所有数据
    clusterA=copy.deepcopy(clusterAss[np.nonzero(clusterAss[:,0]==clu)[0]])  #选出某一聚类的所有类分类结果数据
    minV=min(clusterA[:,1])  #选出最短距离
    index=list(clusterA[:,1]).index(minV)   #找出最短距离对应的share数据是哪一条

    data=copy.deepcopy(Data.iloc[:,index].values)
    data=data[np.nonzero(data!=0)[0]]   #swarm只考虑非0数据
    print('Data.columns: ',Data.columns)
    #time.sleep(6)
    paras=swarm(data,number=index,col=Data.columns[index])   #运行swarm
    print('paras: ',paras)   #best params
    import csv
    model = ModelFactory.create(paras)
    model.enableInference({"predictedField": "value"})

    output = NuPICFileOutput("output"+str(clu), show_anomaly_score=True)

    for i in range(Data.shape[1]):
	input_file='/opt/share_code_data/'+str(Data.columns[i])+'.csv'
        with open(input_file, "rb") as sine_input:
            csv_reader = csv.reader(sine_input)
            # the real data
           
            # skip header rows
       	    csv_reader.next()
       	    csv_reader.next()
            csv_reader.next()
    
       	    for row in csv_reader: 
                timeS=row[0]
            	value = float(row[6])  
    	    	result = model.run({"value": value})
            	output.write(timeS,value, result, prediction_step=3)

            
    
#对leftD数据进行操作
maxl=0;maxi=-1
for i in range(leftD.shape[1]):    #找出最长的非0序列
    if maxl<len(np.nonzero(leftD.iloc[:,i]==0)):maxl=len(np.nonzero(leftD.iloc[:,i]==0));maxi=i
if maxi!=-1:
    data=leftD.values[np.nonzero(leftD.values[:,maxi]!=0)[0],maxi]   #swarm只考虑非0数据
    paras=swarm(data,number=maxi,col=leftD.columns[maxi])   #运行swarm
    print('paras: ',paras)   #best params
    model = ModelFactory.create(paras)
    model.enableInference({"predictedField": "value"})

    output = NuPICFileOutput("leftD_output", show_anomaly_score=True)

    for i in range(leftD.shape[1]):
        input_file='/opt/share_code_data/'+str(leftD.columns[i])+'.csv'
        with open(input_file, "rb") as sine_input:
            csv_reader = csv.reader(sine_input)
            # the real data

            # skip header rows
            csv_reader.next()
            csv_reader.next()
            csv_reader.next()

            for row in csv_reader:
                timeS=row[0]
                value = float(row[6])
                result = model.run({"value": value})
                output.write(timeS,value, result, prediction_step=3)

