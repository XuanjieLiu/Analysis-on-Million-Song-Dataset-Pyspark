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
		Title = h5file['metadata/songs'][0][18]
		Artist_Name = h5file['metadata/songs'][0][9]
		Hotness = h5file['metadata/songs'][0][16]
		songsInfo = [year, Hotness, Title, Artist_Name]
		return songsInfo
	except:
		pass

#Get user's specified year.
year = int(sys.argv[1])
sc = SparkContext(appName="SparkHDF5")
file_path = sc.textFile(pathList.Path.pathToPathList)
#Remove empty data, musics were not released in user's specified year, NaN data. Set hotness as key, (title, artist_name) as value
rdd = file_path.map(h5Open).filter(lambda line: line != None).filter(lambda x: x[0]==year).filter(lambda x: x[1] == x[1]).map(lambda x:(x[1],(x[2],x[3])))
Top10 = rdd.sortByKey().collect()
sc.stop()

print (year)
for i in range(len(Top10)-10,len(Top10))[::-1]:
	print (Top10[i])





