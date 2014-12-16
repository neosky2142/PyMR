import string

class FileHelper:
    
    @staticmethod
    def appendListInFile(fileName,stackOfValues):
	with open(fileName, 'a') as filePointer:
            while(len(stackOfValues)>0):
                filePointer.write(str(stackOfValues.pop()) + "\n")

    @staticmethod
    def appendFileInFile(inFileName,outFileName):
	with open(outFileName, 'a') as outfile:
            with open(inFileName, 'r') as infile:
                for line in infile:
                    outfile.write(line);

    @staticmethod
    def transformTextIntoListOfWords(inFileNameList,outFileName):
	open(outFileName, "w+").close() # create empty file
                                        # Or erase contents of an existing file
	for InputfileName in inFileNameList:
	    infile = open(InputfileName, "r")
	    for line in infile:
                # remove \n
		theline = line.rstrip('\n')
		# lower case
		theline = theline.lower()
		# split into words
		listWord = theline.split(' ')
		# remove empty string from list
		listWord = filter(None, listWord)
		# Write the list and remove the punctuation
		FileHelper.appendListInFileWordsRemovePunctation(outFileName,listWord)

    @staticmethod
    def writeListInFile(fileName,stackOfValues):
        with open(fileName, 'w+') as filePointer:
            while(len(stackOfValues)>0):
                filePointer.write(str(stackOfValues.pop()).rstrip('\n')+'\n') # Remove \n first, just in case

    @staticmethod
    def writeListInFileWordsRemovePunctation(fileName,stackOfValues):
        table = string.maketrans("","")        
        with open(fileName, 'w+') as filePointer:
            while(len(stackOfValues)>0):
                line = str(stackOfValues.pop())                 
                for word in line.rstrip('\n').split(): # Iterate over words
                    toWrite = word.translate(table, string.punctuation) 
                    if not (toWrite == '')  :                 
                        filePointer.write(toWrite+'\n')
    @staticmethod
    def appendListInFileWordsRemovePunctation(fileName,stackOfValues):
        table = string.maketrans("","")        
        with open(fileName, 'a') as filePointer:
            while(len(stackOfValues)>0):
                line = str(stackOfValues.pop())                 
                for word in line.rstrip('\n').split(): # Iterate over words
                    toWrite = word.translate(table, string.punctuation) 
                    if not (toWrite == '')  :                 
                        filePointer.write(toWrite+'\n')


    @staticmethod
    def copyFile(inputFile,outputFile):
        with open(outputFile, 'w+') as outfile:
            with open(inputFile, 'r') as infile:
                for line in infile:
                    outfile.write(line);
        return

    @staticmethod
    def writeDictionnary(outputFile,dictio):
        with open(outputFile, 'w+') as outfile:
            for key, value in dictio.iteritems():
                outfile.write(str(key) + " : " + str(value) + "\n")
