class MapContext:
    # read a chunk, store it in memory, and can pop all element of the chunk
    def __init__(self,outputMapFileName,iterator):
        self.keysList = []
        self.valuesList = []
        self.key = None
        self.value = None
        self.iterator = iterator;
        return

    def putKeyValue(self,key,value):
        self.keysList.append(str(key).rstrip('\n'))
        self.valuesList.append(str(value).rstrip('\n'))
        return

    def loadNext(self):
        self.key = self.keysList.pop();
        self.value = self.valuesList.pop();
        return

    def hasNext(self):
        return len(self.keysList) >= 1


