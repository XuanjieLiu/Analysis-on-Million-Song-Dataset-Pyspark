import h5py
import numpy
import sys
import pathList
from matplotlib import pyplot as plt
from pyspark import SparkContext 
from operator import add

def h5Open(file_name):
	try:
		h5file = h5py.File(file_name)
		year = h5file['musicbrainz/songs'][0][1]
		tag = h5file['musicbrainz/artist_mbtags'][:]
		l = len(tag)
		dict = ['rock', 'pop', 'blues', 'jazz', 'rap', 'punk', 'metal', 'classic', 'hip hop', 'country', 'folk']#If you want to check some tags else, such as 'country', just add it in this list.
		for i in range(0,l):
			str = tag[i].decode('ascii')
			if any(x in str for x in dict):
				for item in dict:
					if str.find(item) >= 0:
						s = item
						break
			else:
				s = "other"
		
		songsInfo = [year, s]
		return songsInfo
	except:
		pass

def plot_graph(a, b):
	x=[]
	y=[]
	for i in range(0, len(a)):
		x.append(a[i][0][1])
		y.append(a[i][1])	
	plt.pie(y, labels=x, autopct="%5.2f%%", pctdistance=0.6, shadow=True, labeldistance=1.1, startangle=None, radius=None)
	plt.title('Proportion of each type of music tracks released in ' + b)
	plt.show()


year = int(sys.argv[1])
sc = SparkContext(appName="SparkHDF5")
file_path = sc.textFile(pathList.Path.pathToPathList)
#Remove the empty data and the music that did not relesed in usr's specified year
rdd = file_path.map(h5Open).filter(lambda x: x != None).filter(lambda x:x[0]==year)
#Set (year, tag) as key, 1 as value
tag = rdd.map(lambda x:((x[0],x[1]),1)).reduceByKey(add)
yearForTag = tag.collect()
sc.stop()

plot_graph(yearForTag, str(year))
