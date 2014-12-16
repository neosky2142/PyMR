class MapChunkIterator:
    # read a chunk, store it in memory, and can pop all element of the chunk
    def __init__(self,fileName):        
        self.filename = fileName
        self.filePointer = open(fileName, "r")        
        return

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.filePointer.close()

    def getNext(self):
        line = self.filePointer.readline()               
        return line.rstrip('\n')





