import h5py


from pyspark import SparkContext

def h5Open(file_name):
	try:
		h5file = h5py.File(file_name.encode("utf-8"))
		year = h5file['musicbrainz']['songs'][0][1]
		return year
	except:
		pass

sc = SparkContext(appName="sparkHDF5")
#Remove all the empty data
data = sc.textFile("/home/xuanjie/Downloads/MillionSongSubset/pathList.txt").map(h5Open).filter(lambda line: line != None)
#Map 1 to each year and reduce by year
yearAmount = data.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y).collect()
sc.stop()

total = sum(x[1] for x in yearAmount)
yearAmount = sorted(yearAmount, key = lambda x: x[0])
noInfo = yearAmount[0][1]
print(yearAmount)

yearX = []
yearY = []
for year in yearAmount:
	if year[0] >= 1960:
		yearX.append(year[0])
		yearY.append(year[1])

counted = sum(yearY)
#Plot the graph
from matplotlib import pyplot as plt
plt.plot(yearX, yearY, 'or')
plt.xlabel('Year')
plt.ylabel('Number of songs released')
plt.legend(loc='upper right')
plt.axis([yearX[0],yearX[-1],0,(max(yearY)+20)])
plt.show(block=True)

print("%d songs in total, %d songs have not yearInfo, %d songs are counted" %(total, noInfo, counted))