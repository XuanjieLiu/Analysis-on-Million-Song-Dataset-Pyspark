import os
import time

#This class is for other function code to import the path to the pathList.txt
class Path:
	pathToDataset = "/home/xuanjie/Downloads/MillionSongSubset/data" #The path to the whole dataset
	pathToPathList = "/home/xuanjie/Downloads/MillionSongSubset/pathList.txt" #The path of the pathList.txt where you want to put it

#Get the paths to all the file in dataset
def getPathList(path):
	pathList = []
	for root, dirs, files in os.walk(path):
		for fn in files:
			pathList.append(root+'/'+fn)
	return pathList

#Create pathList.txt
if __name__ == '__main__':
	t0 = time.clock()
	p = Path()
	pathList = getPathList(p.pathToDataset)
	f = open(Path.pathToPathList,'w')
	for path in pathList:
		f.write(path)
		f.write('\n')
	f.close()

	t1 = time.clock()
	print (t1-t0)