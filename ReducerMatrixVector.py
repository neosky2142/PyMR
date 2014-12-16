class ReducerMatrixVector:

    def reduce(self,context):
        iterator = context.iterator;
        totValue = 0;
        actualValue = iterator.getNext()
        while actualValue is not None:
            totValue = totValue + float(actualValue)
            actualValue = iterator.getNext()        
        return totValue
