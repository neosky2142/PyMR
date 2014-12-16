from MapperSimilarity import MapperSimilarity
from ReducerSimilarity import ReducerSimilarity
from MapReduce import MapReduce
from FileHelper import FileHelper

# Create instances for mapper and reducer
theMapper = MapperSimilarity(20);
theReducer = ReducerSimilarity();

# MapReduce
theMapReducer = MapReduce(theMapper,theReducer,['dataFiles/pictSimilarity'],0,1)
resultDict = theMapReducer.execute()

# Write output
outFileFirectory = 'outputs/'
outfileName = 'similarityResuts.txt';
FileHelper.writeDictionnary(outFileFirectory+outfileName,resultDict)

