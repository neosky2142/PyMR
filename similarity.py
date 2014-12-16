from MapperSimilarity import MapperSimilarity
from ReducerSimilarity import ReducerSimilarity
from MapReduce import MapReduce
from FileHelper import FileHelper
import time

startTime = time.time();
nThreads = 2;

# Create instances for mapper and reducer
theMapper = MapperSimilarity(20);
theReducer = ReducerSimilarity();

# MapReduce
theMapReducer = MapReduce(theMapper,theReducer,['files/pictSimilarity'],0,nThreads)
resultDict = theMapReducer.execute()

# Write output
outFileFirectory = 'files/'
outfileName = 'similarityOut.txt';
FileHelper.writeDictionnary(outFileFirectory+outfileName,resultDict)
print(nThreads,time.time() - startTime)
