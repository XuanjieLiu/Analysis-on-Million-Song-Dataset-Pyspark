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
		dict = ['rock', 'pop', 'blues', 'jazz', 'rap', 'punk', 'metal', 'classic', 'hip hop', 'country', 'folk']
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

#Plot the graph
def plot_graph(a, b, c):
	Y = []
	rock = []
	pop = []
	blues = []
	jazz = []
	rap = []
	punk = []
	metal = []
	classic = []
	hiphop = []
	country = []
	folk = []
	other = []
	for year in range(0,c-b+1):
		Y.append(year + b)
		rock.append(0)
		pop.append(0)
		blues.append(0)
		jazz.append(0)
		rap.append(0)
		punk.append(0)
		metal.append(0)
		classic.append(0)
		hiphop.append(0)
		country.append(0)
		folk.append(0)
		other.append(0)
		for i in range(0, len(a)):
			if a[i][0][1] == 'rock' and a[i][0][0] == year + b:
				rock[year] = a[i][1]
			if a[i][0][1] == 'pop' and a[i][0][0] == year + b:
				pop[year] = a[i][1]
			if a[i][0][1] == 'blues' and a[i][0][0] == year + b:
				blues[year] = a[i][1]
			if a[i][0][1] == 'jazz' and a[i][0][0] == year + b:
				jazz[year] = a[i][1]
			if a[i][0][1] == 'rap' and a[i][0][0] == year + b:
				rap[year] = a[i][1]
			if a[i][0][1] == 'punk' and a[i][0][0] == year + b:
				punk[year] = a[i][1]
			if a[i][0][1] == 'metal' and a[i][0][0] == year + b:
				metal[year] = a[i][1]
			if a[i][0][1] == 'classic' and a[i][0][0] == year + b:
				classic[year] = a[i][1]
			if a[i][0][1] == 'hip hop' and a[i][0][0] == year + b:
				hiphop[year] = a[i][1]
			if a[i][0][1] == 'country' and a[i][0][0] == year + b:
				country[year] = a[i][1]
			if a[i][0][1] == 'folk' and a[i][0][0] == year + b:
				folk[year] = a[i][1]
			if a[i][0][1] == 'other' and a[i][0][0] == year + b:
				other[year] = a[i][1]

		sum = float(rock[year]+pop[year]+blues[year]+jazz[year]+rap[year]+punk[year]+metal[year]+classic[year]+hiphop[year]+country[year]+folk[year])
					
		rock[year] = rock[year]/sum
		pop[year] = pop[year]/sum	
		blues[year] = blues[year]/sum	
		jazz[year] = jazz[year]/sum
		rap[year] = rap[year]/sum
		punk[year] = punk[year]/sum
		metal[year] = metal[year]/sum
		classic[year] = classic[year]/sum
		hiphop[year] = hiphop[year]/sum
		country[year] = country[year]/sum
		folk[year] = folk[year]/sum

	p1 = plt.plot(Y, rock, 'r')
	p2 = plt.plot(Y, pop, 'g')
	p3 = plt.plot(Y, blues, 'b')
	p4 = plt.plot(Y, jazz, 'y')
	p5 = plt.plot(Y, rap, 'c')
	p6 = plt.plot(Y, punk, 'm')
	p7 = plt.plot(Y, metal, 'k')
	p8 = plt.plot(Y, classic, 'k--')
	p9 = plt.plot(Y, hiphop, 'r--')
	p10 = plt.plot(Y, country, 'g--')
	p11 = plt.plot(Y, folk, 'y--')
	plt.axis([Y[0],Y[-1]+10,0,1])
	plt.legend(('rock', 'pop', 'blues', 'jazz', 'rap', 'punk', 'metal', 'classic', 'hip hop', 'country', 'folk'))
	plt.ylabel('proportion of music tracks release')
	plt.xlabel('year')
	plt.title('The trend of each typical music tag of music released in years')
	plt.show()


year1 = int(sys.argv[1])
year2 = int(sys.argv[2])
sc = SparkContext(appName="SparkHDF5")
file_path = sc.textFile(pathList.Path.pathToPathList)
#Remove the empty data and musics that were not released in usr's specified year range
rdd = file_path.map(h5Open).filter(lambda x: x != None).filter(lambda x:x[0]>=year1).filter(lambda x:x[0]<=year2)
#Set (year, tag) as key, 1 as value
tag = rdd.map(lambda x:((x[0],x[1]),1)).reduceByKey(add)
yearForTag = tag.collect()
sc.stop()

yearForTag = sorted(yearForTag, key = lambda x:x[0][1],)
print (yearForTag)
plot_graph(yearForTag, year1, year2)

