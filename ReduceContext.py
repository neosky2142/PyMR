class ReduceContext:
    def __init__(self,key,iterator):
        self.key = key
        self.value = None
        self.iterator = iterator;
        return

    def loadNextValue(self):
        self.key = self.keysList.pop();
        self.value = self.valuesList.pop();
        return


