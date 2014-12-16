from MapperMatrixVector import MapperMatrixVector
from ReducerMatrixVector import ReducerMatrixVector
from MapReduce import MapReduce
from FileHelper import FileHelper

# Create instances for mapper and reducer
# Note that the vector is stored in the instance
theReducerMatrixVector = ReducerMatrixVector();
theMapperMatrixVector = MapperMatrixVector('C:/mapReduceMatrixMultiplication/b_vector');

# the file where the matrix is stored
matrixFile = ['C:/mapReduceMatrixMultiplication/A_matrix'];

# MapReduce
theMapReducerMatrixVector = MapReduce(theMapperMatrixVector,theReducerMatrixVector,matrixFile,0,1)
resultDictMatrixVector = theMapReducerMatrixVector.execute();

# Write output
resultListMatrixVector = [];
for i in range(len(resultDictMatrixVector),0,-1):
    if resultDictMatrixVector.has_key(str(i)):
        resultListMatrixVector.append(resultDictMatrixVector[str(i)])
    else:
        resultListMatrixVector.append(0)
FileHelper.writeListInFile('C:/mapReduceMatrixMultiplication/MatrixVectorResults.txt',resultListMatrixVector)
