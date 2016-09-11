import h5py
import numpy
import pathList
from pyspark import SparkContext 
from operator import add
from matplotlib import pyplot as plt

def h5Open(file_name):
	try:
		h5file = h5py.File(file_name)
		year = h5file['musicbrainz/songs'][0][1]
		Title = h5file['metadata/songs'][0][18]
		Artist_Name = h5file['metadata/songs'][0][9]
		songsInfo = [year, Title, Artist_Name]
		return songsInfo
	except:
		pass

def plot_graph(a):
	x = []
	y = []
	for i in range(0,len(yearForMusicReleased)):
		x.append(a[i][0])
		y.append(a[i][1])
	plt.plot(x, y, 'or')
	plt.xlabel('Year')
	plt.ylabel('Number of songs')
	plt.legend(loc='upper right')
	plt.title('Number of songs released each year(missing value mixed)')
	plt.axis([x[0],x[-1],0,(max(y)+20)])
	plt.show(block=True)

sc = SparkContext(appName="SparkHDF5")
file_path = sc.textFile(pathList.Path.pathToPathList,minPartitions=2)
Inti_rdd = file_path.map(h5Open).filter(lambda x: x != None).cache()
#Find the missing data. (Data that year = 0)
Missing_rdd = Inti_rdd.filter(lambda x: x[0]==0).map(lambda x: (x[2],(x[0],x[1]))).cache()
#Put data that is with year information in another RDD
Available_rdd = Inti_rdd.filter(lambda x: x[0]!=0).cache()
#Calculate the average year that songs were released by each artist.
Available_rdd_GetYear = Available_rdd.map(lambda x:(x[2],x[0])).map(lambda x:(x[0],(x[1],1))).reduceByKey(lambda x,y:(x[0]+y[0],x[1]+y[1])).map(lambda x:(x[0],int(x[1][0]/x[1][1])))
#Replace the missing value by that average year.
Missing_rdd_fixed = Available_rdd_GetYear.join(Missing_rdd).map(lambda x:(x[1][0], x[1][1][1], x[0]))
FinalRdd = Missing_rdd_fixed.union(Available_rdd)
year = FinalRdd.map(lambda x:(x[0],1))
y = year.reduceByKey(add)
yearForMusicReleased = y.collect()
sc.stop()

plot_graph(sorted(yearForMusicReleased, key = lambda x:x[0]))
print (sorted(yearForMusicReleased, key = lambda x:x[0]))





