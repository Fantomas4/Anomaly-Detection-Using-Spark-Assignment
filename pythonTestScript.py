#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Dec 30 12:34:39 2019

@author: mavroudo
"""
points=[]
with open("data201920.csv") as f:
    for data in f:
        point=data.split(",")
        if len(point)==2 and "" not in point:
            points.append([float(point[0]),float(point[1])])

import pandas as pd
df =pd.DataFrame(points,columns=["x","y"])

#transformatio min max
maxX,maxY=df.max()
minX,minY=df.min()

pointsTransformed=[]
for x,y in points:
    pointsTransformed.append([(x-minX)/(maxX-minX),(y-minY)/(maxY-minY)])
dfTransformed=pd.DataFrame(pointsTransformed,columns=["x","y"])
            
from sklearn.cluster import KMeans
#centers seems ok
print("Centers")


import matplotlib.pyplot as plt
dfTransformed.plot(x="x",y="y",kind="scatter")
plt.savefig("transformedData.png")

for n in [5,10,20,50]:
    dfTransformed=pd.DataFrame(pointsTransformed,columns=["x","y"])
    kmeans=KMeans(n_clusters=n).fit(dfTransformed)
    print(kmeans.cluster_centers_)
    dfTransformed['cluster']=kmeans.predict(dfTransformed)
    dfTransformed.plot(x="x",y="y",c="cluster",kind="scatter",colormap='summer')
    plt.savefig("withClusters-"+str(n)+".png")
