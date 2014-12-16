class Mapper:

    @staticmethod
    def map(theContext):
        mapIterator = theContext.iterator;        
        word = mapIterator.getNext();
        while word:
            if len(word) >= 8 :                               
                theContext.putKeyValue(word, 1)
            word = mapIterator.getNext();            

