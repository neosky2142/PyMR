
class Reducer:

    @staticmethod
    def reduce(context):
        key = context.key;
        iterator = context.iterator;
        totValue = 0;
        actualValue = iterator.getNext()
        while actualValue:
            totValue = totValue + int(actualValue)
            actualValue = iterator.getNext()        
        return totValue