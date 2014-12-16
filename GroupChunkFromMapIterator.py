class GroupChunkFromMapIterator:

    def __init__(self,fileName,nElem):
        self.filename = fileName
        print fileName
        self.filePointer = open(fileName, "r")
        self.nElemLeft = nElem;
        return

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.filePointer.close()

    def loadNext(self):
        self.nElemLeft = self.nElemLeft - 1;
        self.key = self.filePointer.readline()
        self.value = self.filePointer.readline()

    def hasNext(self):
        return self.nElemLeft >= 1 ;



