import h5py
import sys
import numpy
import pathList
from pyspark import SparkContext


def h5Open(file_name, tag, option):
	try:
		h5file = h5py.File(file_name)
		year = h5file['musicbrainz/songs'][0][1]
		info = 0
		if year == 0:
			pass
		if option == 'tempo':
			info = h5file['analysis/songs'][0][-4]
		if option == 'loudness':
			info = h5file['analysis/songs'][0][-8]
		if option == 'hotness':
			info = h5file['metadata/songs'][0][-4]
		if info == 0 or info != info:
			pass
		if tag == 'general':
			return (year, info)
		tags = h5file['musicbrainz/artist_mbtags'][:]
		if len(tags) == 0:
			pass
		haveTag = 0
		#Search the tag typed in
		for i in range(0, len(tags)):
			if tag in tags[i]:
				haveTag = 1
		if haveTag == 0:
			pass
		else:
			return (year, info)	
	except:
		pass

#Get and control the input
tag = str(sys.argv[1])
option = str(sys.argv[2])
if option != 'tempo' and option != 'loudness' and option != 'hotness' :
	print ("Please just select from 'tempo', 'loudness' and 'hotness'.")
	sys.exit(0)
sc = SparkContext(appName="sparkHDF5")
#Remove empty data, 0 data and NaN data.
data = sc.textFile(pathList.Path.pathToPathList).map(lambda path: h5Open(path, tag, option)).filter(lambda line: line != None).filter(lambda x: x[1] != 0).filter(lambda x: x[1] == x[1])
#Group the tempo, loudness or hotness data by each year
yearInfo = data.groupByKey().collect()
if len(yearInfo) == 0:
	print("No such tag")
	sys.exit(0)
yearInfo = sorted(yearInfo, key = lambda x: x[0])
#Plot the graph
import matplotlib.pyplot as plt
yearX = []
mean = []
for year in yearInfo:
	if year[0] >= 1960:
		yearX.append(year[0])
		y = list(year[1])
		mean.append(numpy.mean(y))
		x = [a - a + year[0] for a in y]
		plt.scatter(x,y,c='r',s=10,alpha=0.6,edgecolors='white')
plt.plot(yearX, mean, 'b',label = 'Mean value in each year')
plt.title(tag+' music information')
plt.xlabel('Year')
plt.ylabel(option)
plt.legend()
plt.grid(True)
plt.show()