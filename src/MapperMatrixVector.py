class MapperMatrixVector:

    def __init__(self,vectorFile):
        num_lines = sum(1 for line in open(vectorFile))
        self.vector = [0 for i in range(num_lines)];
        f=open(vectorFile, "r")
        counter = 0;
        for line in f:
            self.vector[counter] = float(line.rstrip('\n'))
            counter = counter+1;

    def map(self,theContext):
        mapIterator = theContext.iterator;        
        entry = mapIterator.getNext();
        while entry:
            entry = entry.split();
            line = entry[0].rstrip('\n');
            col = entry[1].rstrip('\n');
            val = float(entry[2])
            theContext.putKeyValue(line,val*self.vector[int(col)-1] )
            entry = mapIterator.getNext();        

