import sys
from FileHelper import FileHelper

# This chunck factory creates chunks from textfile
class ChuncksFactory:

    # Files if a list of files
    def __init__(self, files):
        self.chunck_completed = 0 ;
        self.files = files ;
        self.nChunks = -1 ;

    def divideIntoChunks(self,filenameGenerator):
        # input : a file with one input/line
        # output : chunks with one word/lines (chunks == give size)
        stackOfValues = []
        counter = 0;
        for InputfileName in self.files:
            filePointer = open(InputfileName, "r")
            for line in filePointer:                                
                stackOfValues.append(line)                
                if(sys.getsizeof(stackOfValues)>536870912): # more than 64Mo
                    FileHelper.writeListInFile(filenameGenerator(counter),stackOfValues)
                    counter = counter + 1
            filePointer.close()
        if len(stackOfValues) > 0: # Just in case ...
            FileHelper.writeListInFile(filenameGenerator(counter),stackOfValues)
            counter = counter + 1;
        self.nChunks = counter
        return

