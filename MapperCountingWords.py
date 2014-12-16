class MapperCountingWords:
    def map(self,theContext):
        mapIterator = theContext.iterator;
        word = mapIterator.getNext().rstrip('\n');
        while word:
            theContext.putKeyValue(word, 1)
            word = mapIterator.getNext().rstrip('\n');
