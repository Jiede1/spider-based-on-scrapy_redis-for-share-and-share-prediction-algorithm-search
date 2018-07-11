#coding:utf-8
#  version-1  #
#将hdfs数据本地化---读入DataFrame----利用BP+SMD
#下面讲述如何利用BP+SMD
#将数据的‘开盘价’取出，形成一列。对所有股票操作，形成多列。使用SMD
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
'''
filepath='/sdbadmin/hadoop/input'
try:
    client=Client('http://192.168.111.130:50070')
except Exception as e:
    print(e)

dirs=client.list(filepath)
#将hdfs本地化
print('there are %d shares'%(len(dirs)))

try:
    for i in range(len(dirs)):
        client.download(filepath+'/'+dirs[i],'/opt/share_code_data/'+dirs[i])
except Exception as e:
    print(e)

min_max_scaler = preprocessing.MinMaxScaler()
DD=pd.DataFrame([])
for i in range(len(dirs)):
    df=pd.read_csv('/opt/share_code_data/'+dirs[i],index_col=0)
    if len(DD)==len(df):
	trun = min_max_scaler.fit_transform(copy.deepcopy(df.iloc[:,5]))
        DD[dirs[i].strip().split('.')[0]]=trun
    else:
	cols=DD.columns
	DD=pd.concat([DD,copy.deepcopy(df.iloc[:,5])],axis=1)   #长度不一致的合并
	f=list(cols)
	f.append(dirs[i].strip().split('.')[0])
	DD.columns=f
DD.fillna(-1, inplace=True)

print('DD shape: ',np.shape(DD))
print(DD) #[6690 rows x 169 columns]
'''


#计算MSD
#只针对非-1序列值
#设定距离长为100,计算得到的share称为 局部msd最优share
#算法如下：
'''
1.选取要进行预测的share
2.计算剩余的share中，msd最小的20条局部msd最优share
3.对2中share计算百分比*share值，作为输入，输出为下一刻的目标值,取70%长度作为训练
'''
def local_MSD(a,b,start=0,end=100): #局部最优，默认100
    return MSD(a.iloc[start:end,],b.iloc[start:end,])

def local_MSD_layer(a,b):   #对应于出现断层数据
    l1=np.nonzero(a!=-1)[0]
    l2=np.nonzero(b!=-1)[0]
    l=list(set(l1) & set(l2))
    sim=local_MSD(a.iloc[l,],b.iloc[l,],end=len(a))
    return sim
def sortedTw(a):
    res=sorted(a,reverse=True)
    D={}
    for i in range(20):
	val=res[i].values()[0].split('-')[1]
	D[val]=res[i].keys()[0]   #序列号做keys,相似度做value
    return D
def update_train(tempDF,D,detect,trainD,trainL):
    #key=D.keys()
    #value=D.values()
    #tempLength=len(tempDF)
    FG=[]
    for key,value in D.items():   #value 就是相似度 ， key是对应序列
        print('key,value:',key,value)
        temp=copy.deepcopy(tempDF.iloc[:,int(key)])
	temp=temp/float(value) if value!=0 else temp
	FG.append(temp)
    if len(trainD)!=0:
        trainD=np.concatenate([trainD,np.array(FG).T],axis=0)  #向下拼接
	trainL=np.concatenate([trainL,tempDF.iloc[:,detect].values.reshape(-1)],axis=0)
    else:
	trainD=np.array(FG).T
	trainL=tempDF.iloc[:,detect].values.reshape(-1)
    print('trainD,trainL:',trainD.shape,trainL.shape)
    return trainD,trainL

def create_examples(DF,detect,pro=1.0,msd_len=100):  #DF是pandas，pro可选，这里默认为1，意味着train/test数据集都生成，msd_len默认msd距离长为100,detect是目标列
    length=int(len(DF)*pro)
    print('length: ',length)
    i=0
    Data=np.array([])   #train data
    Label=np.array([])   #train label
    while i<length:
	start=i
	i+=msd_len
        end=start+msd_len if start+msd_len<=length else length  #训练集不得超过length
        tempDF=DF.iloc[start:end,:]   #截取msd_len长度的Data
	if len(np.nonzero(tempDF.iloc[:,detect]==-1)[0])>=int(0.1*len(tempDF)):    #对于目标列，限制为10%的好处是，可以避免这种情况：两个序列中，有一个序列非常多-1，导致计算不准确
	    print('Not satisfied!')
	    continue
	simL=[]
	for j in range(tempDF.shape[1]):  #遍历所有股票数
	    if j!=detect:  #自己不会跟自己计算
		l=len(np.nonzero(tempDF.iloc[:,j]==-1)[0])
		if l<=int(0.1*len(tempDF)):    #如果-1占比不超过10%,计算
		    sim=local_MSD_layer(tempDF.iloc[:,detect],tempDF.iloc[:,j]) 
		    simL.append({sim:str(detect)+'-'+str(j)})  #相似度做keys
		else:
		    print('%d col is not suitable for %d line because -1 too much '%(j,detect))
        D=sortedTw(simL)   #得到与detect符合的20个序列序号，以及相似度
	Data,Label = update_train(tempDF,D,detect,Data,Label)   #更新train训练集和标签	
	print('Data print: ',Data)
	#time.sleep(6)
    return Data,Label

def percent(a):
    a=pd.DataFrame(a)
    a.columns=['origin','predict']
    b=[]
    for i in range(len(a)):
	d=abs(a.iloc[i,0]-a.iloc[i,1])/a.iloc[i,1] if a.iloc[i,1] else 'Error'
	if not isinstance(d,str):
	    d=str(d*100)+'%'
	b.append(d)
    a['loss']=b
    return a
def use_algorithm(Data,Label,pro=0.7):   #利用sklearn的神经网络实现预测，默认0.7训练集
    #from sklearn.neural_network import MLPRegressor    
    train_length=int(pro*len(Data))
    trainD=Data[:train_length,:]
    testD=Data[train_length:,:]
    trainL=Label[:train_length]
    testL=Label[train_length:]
    print('trainD,trainL,testD,testL: ',trainD.shape,trainL.shape,testD.shape,testL.shape)
    #exit(1)
    # 神经网络
    parameters = {  
        'hidden_layer_sizes': [(15,),(15,7,3),(21,3),(7,3,3),(15,7,5)], 
	'max_iter': [20000,100000,200000],
	'momentum': [0,0.5,0.7,1],
	'learning_rate': ['adaptive','constant','invscaling'],\
	'solver': ['sgd','adam'],\
	'shuffle': [False],\
	'activation': ['logistic','relu','tanh']
    }
    mlp = MLPRegressor()
    clf = GridSearchCV(mlp, parameters)
    model=clf.fit(trainD,trainL)
    bestp=clf.best_params_   
    with open('best_params.csv','w') as f:
	f.write(str(bestp))
	f.close()
    with open('best_params.csv','r') as f:
        bestp=eval(f.read())
        f.close()
    print('bestp: ',bestp)
    #clf = MLPRegressor(hidden_layer_sizes=(15,), max_iter=100000,learning_rate='adaptive',solver='sgd',shuffle=False,activation='logistic')
    #model = clf.fit(trainD,trainL)
    predictD = model.predict(testD)   #predict data

    nnDF=np.concatenate([predictD.reshape(-1,1),testL.reshape(-1,1)],axis=1)
    loss=percent(nnDF)
    print('loss: ',loss)
    #coefficient of determination ---- 1.0 is the best
    score = model.score(testD,testL)
    print('score: ',score)
    loss.to_csv('/home/sdbadmin/loss.csv',index=False)   #将预测结果本地化

if __name__=='__main__':       
    
    import time
    beginTime=time.time()
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
    except Exception as e:
        print(e)
    '''
    min_max_scaler = preprocessing.MinMaxScaler()
    DD=pd.DataFrame([])
    for i in range(len(dirs)):
	print('which csv: ',dirs[i])
        df=pd.read_csv('/opt/share_code_data/'+dirs[i],index_col=0)
        if len(DD)==len(df) or len(DD)==0 and len(df)!=0:
            trun = min_max_scaler.fit_transform(copy.deepcopy(df.iloc[:,5]).values.reshape(-1,1))
            DD[dirs[i].strip().split('.')[0]]=trun.ravel()	
        elif len(df)!=0:
            cols=DD.columns
	    try:
	    	trun = min_max_scaler.fit_transform(copy.deepcopy(df.iloc[:,5]).values.reshape(-1,1))
	    except Exception as e:
		print(e)
		print(df.iloc[:,5])
		time.sleep(56)
            DD=pd.concat([DD,pd.DataFrame(trun)],axis=1)   #长度不一致的合并
            f=list(cols)
            f.append(dirs[i].strip().split('.')[0])
            DD.columns=f
    DD.fillna(-1, inplace=True)

    print('DD shape: ',np.shape(DD))
    print(DD) #[6690 rows x 169 columns]

    Data,Label=create_examples(DD,detect=0)	
	
    use_algorithm(Data,Label)		
    endTime=time.time()
    print('\nalgorithm runTime: ',endTime-beginTime)

'''
for i in range(y):  #遍历所有share
    print('Now is %s'%DD.columns[i])
    for j in range(y):
	if i!=j:
	    l1=np.nonzero(DD.iloc[:,i]!=-1)[0]
	    l2=np.nonzero(DD.iloc[:,j]!=-1)[0]
	    l=set(l1) & set(l2)
	    tempDF=copy.deepcopy(DD.iloc[list(l),[i,j]])
	    print('tempDF shape: ',tempDF.shape)
	    train_len=int(len(tempDF)*0.7)
	    test_len=int(len(tempDF)*0.3)
'''	     
