from MapperCountingWords import MapperCountingWords
from ReducerCountingWords import ReducerCountingWords
from MapReduce import MapReduce
from FileHelper import FileHelper

# Create instances for mapper and reducer
theMapper = MapperCountingWords();
theReducer = ReducerCountingWords();

# parse the file : one word/line
inFiles = ['dataFiles/text'];

# we can have more than one text file
inFileParsed = 'dataFiles/textParsed';
FileHelper.transformTextIntoListOfWords(inFiles,inFileParsed)

# MapReduce
theMapReducer = MapReduce(theMapper,theReducer,[inFileParsed],silent=-1,nThreads=5)
resultDict = theMapReducer.execute()

# Write output
outFileFirectory = 'outputs/'
outfileName = 'coutingWordsResults.txt';
FileHelper.writeDictionnary(outFileFirectory+outfileName,resultDict)
