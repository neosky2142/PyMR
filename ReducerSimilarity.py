import time

class ReducerSimilarity:
    def reduce(self,context):
        pict1Idx = int(context.key);
        iterator = context.iterator;
        similarVec = [];
        pict2 = iterator.getNext()
        
        def computeSimilarity(pict1Idx,pict2Idx):
            time.sleep(0.25); # waits 1/10 seconds
            return (abs(pict1Idx-pict2Idx)<=1)
    
        while pict2:
            pict2 = int(pict2)
            if(computeSimilarity(pict1Idx,pict2) == 1):
                similarVec.append(pict2)
            pict2 = iterator.getNext()
        return similarVec

     
