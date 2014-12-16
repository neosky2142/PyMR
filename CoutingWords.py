from MapperCountingWords import MapperCountingWords
from ReducerCountingWords import ReducerCountingWords
from MapReduce import MapReduce
from FileHelper import FileHelper

# Create instances for mapper and reducer
theMapper = MapperCountingWords();
theReducer = ReducerCountingWords();

# parse the file : one word/line
#inFiles = ['C:/mapReduceCountingWords/dataFile'];
inFiles = ['files/dataFile'];

# we can have more than one text file
inFileParsed = 'files/dataFileParsed';
FileHelper.transformTextIntoListOfWords(inFiles,inFileParsed)

# MapReduce
theMapReducer = MapReduce(theMapper,theReducer,[inFileParsed],silent=0,nThreads=5)
resultDict = theMapReducer.execute()

# Write output
# outFileFirectory = 'C:/mapReduceCountingWords/'
outFileFirectory = 'files/'
outfileName = 'coutingWordsResults.txt';
FileHelper.writeDictionnary(outFileFirectory+outfileName,resultDict)
