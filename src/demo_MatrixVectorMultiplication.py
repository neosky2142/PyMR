from MapperMatrixVector import MapperMatrixVector
from ReducerMatrixVector import ReducerMatrixVector
from MapReduce import MapReduce
from FileHelper import FileHelper

# Create instances for mapper and reducer
# Note that the vector is stored in the instance
theReducerMatrixVector = ReducerMatrixVector();
theMapperMatrixVector = MapperMatrixVector('dataFiles/b');

# the file where the matrix is stored
matrixFile = ['dataFiles/A'];

# MapReduce
theMapReducerMatrixVector = MapReduce(theMapperMatrixVector,theReducerMatrixVector,matrixFile,0,1)
resultDict = theMapReducerMatrixVector.execute();

# Write output
outFileFirectory = 'outputs/'
outfileName = 'matrixVectorResults.txt';
FileHelper.writeDictionnary(outFileFirectory+outfileName,resultDict)
