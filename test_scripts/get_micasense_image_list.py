#!/usr/bin/python
#
import os
import glob
from collections import defaultdict,OrderedDict

subFolder='/Users/mlucas/Desktop/Micasense/uav_staging/20180404_172748_20m_MRE_90_Still_0/003/'
imageType='tif'
pathToImages=os.path.join(subFolder+'*.'+imageType)

imageFileList = os.listdir(subFolder)
imageFileList.sort()
print(len(imageFileList))
imagePathList = glob.glob(pathToImages)
imagePathList.sort()
print(len(imagePathList))

imageCheckDict=defaultdict(list)

for f in imageFileList:
    primaryImageName=f.rpartition('_')[0]
    imageCheckDict[primaryImageName].append(f)

imageCheckDict=defaultdict(list)

for f in imagePathList:
    a=f.split('/')[-1]
    primaryImageName=a.rpartition('_')[0]
    imageSize=os.stat(f).st_size
    imageCheckDict[primaryImageName].append([f,imageSize])

validImageList=[]
validImagePathList=[]
print("Items in Dictionary before checking",len(imageCheckDict))

for k,v in sorted(imageCheckDict.items()):
    truncatedImage=False
# Check for image sets which have less than 5 images or more than 5 images and remove them from the valid list if found
    if len(v) != 5:
        #print(k,v)
        imageCheckDict.pop(k,)
        print('Deleted Key',k)
        break
# Check for images that have been truncated (less than a threshold) and remove the set from the valid list if found
    for s in range(0,5):
        #print(k,v[s][0],v[s][1])
        imageSize=v[s][1]
        if imageSize < 2465260:
            truncatedImage=True
    if truncatedImage:
        imageCheckDict.pop(k, )
        print('Deleted Key Due to Truncated Image', k)

print("Items in Dictionary after checking",len(imageCheckDict))

# Build the list of valid images to be processed further

for k,v in sorted(imageCheckDict.items()):
    for i in range(0,5):
        validImagePathList.append(v[i])
pass

