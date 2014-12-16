class ReduceFromGroupIterator:
    # read a chunk, store it in memory, and can pop all element of the chunk

    def __init__(self,globalNodeFileName):
        self.globalNodeName = globalNodeFileName;
	self.listOfFileName = []
        filePointer = open(globalNodeFileName, "r")
        for line in filePointer:
            self.listOfFileName.append(line.rstrip('\n'))

        self.actualFileName = self.listOfFileName.pop();
        self.filePointer = open(self.actualFileName, "r")
        return

    def getNext(self):
        value = self.filePointer.readline()
        value = value.rstrip('\n')
        if value: #value is not none
            return value
        else : # we need to load the next file
            self.filePointer.close()
            if self.listOfFileName: # list not empty
                # We load the next file and redo
                self.actualFileName = self.listOfFileName.pop();
                self.filePointer = open(self.actualFileName, "r")
                return self.getNext();
            else :# list empty
                return None # We have finished
        



