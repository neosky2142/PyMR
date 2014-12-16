from MapperMatrixVector import MapperMatrixVector
from ReducerMatrixVector import ReducerMatrixVector
from MapReduce import MapReduce
from FileHelper import FileHelper
import time

for n in [100, 1000, 10000, 100000]:

	start = time.time()

	# Create instances for mapper and reducer
	# Note that the vector is stored in the instance
	theReducerMatrixVector = ReducerMatrixVector();
	theMapperMatrixVector = MapperMatrixVector('/Users/lcambier/TempMapReduce/matrices/b_{}'.format(n));

	# the file where the matrix is stored
	matrixFile = ['/Users/lcambier/TempMapReduce/matrices/A{}/A_{}_{}'.format(n,n,n)];

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
	FileHelper.writeListInFile('/Users/lcambier/TempMapReduce/matrices_results/Ax{}'.format(n),resultListMatrixVector)

	end = time.time()
	print "Time with n = {} is {}.".format(n,end - start)
