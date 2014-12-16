class MapperSimilarity:

    def __init__ (self,maxPictures):
        self.maxPictures = maxPictures;
    
    def map(self,theContext):
        mapIterator = theContext.iterator;
        pictureIdx = mapIterator.getNext().rstrip('\n');
        while pictureIdx:
            pictureIdx = int(pictureIdx);
            for i in range(pictureIdx,self.maxPictures):
                theContext.putKeyValue(pictureIdx, i)
            pictureIdx = mapIterator.getNext().rstrip('\n');
