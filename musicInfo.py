import h5py
from pyspark import SparkContext
import numpy
import sys
import pathList



def h5Open(file_name, option):
	try:
		h5file = h5py.File(file_name)
		info = 0
		if option == 'tempo':
			info = h5file['analysis/songs'][0][-4]
		if option == 'loudness':
			info = h5file['analysis/songs'][0][-8]
		if option == 'hotness':
			info = h5file['metadata/songs'][0][-4]
		if info == 0 or info != info:
			pass
		tag = h5file['musicbrainz/artist_mbtags'][:]
		l = len(tag)
		dict = ['punk', 'metal', 'hip-hop', 'blues', 'jazz', 'rap', 'rock', 'pop', 'classic', 'disco']#If you want to check some tags else, such as 'country', just add it in this list.
		for i in range(0,l):
			str = tag[i].decode('ascii')
			if any(x in str for x in dict):
				for item in dict:
					if str.find(item) >= 0:
						s = item
						break
			else:
				pass
		return (s, info)
	except:
		pass

#Get and control the input
option = str(sys.argv[1])
if option != 'tempo' and option != 'loudness' and option != 'hotness' :
	print ("Please just select from 'tempo', 'loudness' and 'hotness'.")
	sys.exit(0)
sc = SparkContext(appName="sparkHDF5")
#Remove empty data, 0 data and NaN data.
data = sc.textFile(pathList.Path.pathToPathList).map(lambda path: h5Open(path, option)).filter(lambda line: line != None).filter(lambda x: x[1] != 0).filter(lambda x: x[1] == x[1])
#Group the tempo, loudness or hotness data by tags of the musics
musicInfo2 = data.map(lambda x: (x[0], x[1])).groupByKey().collect()
musicInfo3 = []
for tag in musicInfo2:
	musicInfo3.append((tag[0], numpy.mean(list(tag[1])), numpy.std(list(tag[1]))))
#Plot the graph
def plot(musicInfo3):
	import matplotlib.pyplot as plt
	fig, ax = plt.subplots()
	index = numpy.arange(len(musicInfo3))
	num_tags = len(musicInfo3)
	position = []
	
	bar_width = 0.4
	opacity = 0.4
	error_config = {'ecolor': '0.3'}
	tags = [x[0] for x in musicInfo3]
	i = 0
	for tag in musicInfo3:
		rects = plt.bar([i], tag[1], bar_width, alpha=opacity, color='b', yerr=tag[2], error_kw=error_config)#, label=tag[0])
		position.append(i)
		i += bar_width
		
	plt.xlabel('Different music type')
	plt.ylabel(option)
	plt.title(option+' of different music type')
	position = [x + 0.5*bar_width for x in position]
	plt.xticks(position, tags)
	plt.legend()
	plt.tight_layout()
	plt.show()

print(musicInfo3)
plot(musicInfo3)