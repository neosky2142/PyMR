import os.path
from FileHelper import FileHelper

class Grouper:

    # Input : list of KeyValue
    # Output : list of KeyValueList

# Warning : at the begining of the process, we need to clear the directory (with, for example, shutil.rmtree(), wich will delete a directory and all its contents.)

    def __init__(self,grouperId,grouperNCall,grouperOldNCall,directory):
        self.directory = directory;
        self.id = grouperId;
        self.nCall = grouperNCall;
        self.oldNCall = grouperOldNCall;
        self.dicFromKeyToNewChunk = dict();
        self.dictFromKeyToNodeFile = dict(); # Ascociate a key to a number of node file. This file contains a list of all files with values
        self.oldDictFromKeyToNodeFile = dict(); # Old dictio

        if self.isExistSaveState() :
            self.loadSaveState();
        else :
            self.nDifferentKeys = 0; # Same than number of NodeFiles
            self.nDifferentChunks = 0; # Used to assign an ID to a chunk


    ###
    # File name generator (static)
    ###
    
    @staticmethod
    def genericGlobalNodeFileName(nodeIdx,directory):
        ## g for global node
        return directory + "g_" + str(nodeIdx);

    @staticmethod
    def genericGrouperLogFileName(selfId,selfNCall,directory):
        ## s for save state
        return directory + "s_" + str(selfId) + "_" + str(selfNCall) # grouper_id_##_call_##_saveState

    @staticmethod
    def genericChunkFileName(selfId,chunkIdx,selfNCall,directory):
        ## c for chunk
        return directory + "c_" + str(selfId) + "_" + str(selfNCall) + "_" + str(chunkIdx); # grouper_id_##_call_##_chunkno_##

    @staticmethod
    def genericNodeFileName(selfId,chunkIdx,selfNCall,directory):
        ## n for nodefile
        return directory + "n_" + str(selfId) + "_" + str(selfNCall) + "_" + str(chunkIdx); # grouper_id_##_call_##_nodefile_##

    ###
    # File name generator (self)
    ###
    

    def getGrouperLogFile(self):
        return Grouper.genericGrouperLogFileName(self.id,self.nCall,self.directory);

    def getGrouperOldLogFile(self):
        return Grouper.genericGrouperLogFileName(self.id,self.oldNCall,self.directory);

    def chunkNameGenerator(self,chunkIdx):
        return Grouper.genericChunkFileName(self.id,chunkIdx,self.nCall,self.directory);

    def nodeFileNameGenerator(self,chunkIdx):
        return Grouper.genericNodeFileName(self.id,chunkIdx,self.nCall,self.directory);

    def oldNodeFileNameGenerator(self,chunkIdx):
        return Grouper.genericNodeFileName(self.id,chunkIdx,self.oldNCall,self.directory);

    ###
    # SaveState
    ###

    def isExistSaveState(self):
        # check if there exist a file with name mapper_and_groupper_logs/grouper_id_##_saveState.
        fileName = self.getGrouperOldLogFile();
        return os.path.isfile(fileName)

    def loadSaveState(self):
        # load the dictionary dictFromKeyToNodeFile from file mapper_and_groupper_logs/grouper_id_##_saveState
        fileName = self.getGrouperOldLogFile();
        if fileName is not None:
            filePointer = open(fileName, "r")
            self.nDifferentKeys = int(filePointer.readline())
            self.nDifferentChunks = int(filePointer.readline())
            # for i in range(0,self.nDifferentKeys):
            key = filePointer.readline()
            while len(key)>0:
                value = filePointer.readline()
                value = value.rstrip('\n');
                key = key.rstrip('\n');
                self.oldDictFromKeyToNodeFile[key] = value;
                key = filePointer.readline()
        return

    def saveStateGrouper(self):
        # write the dictionnary into file mapper_and_groupper_logs/grouper_id_##_saveState.
        logFileName = self.getGrouperLogFile()
        with open(logFileName, 'w') as filePointer:
            filePointer.write(str(self.nDifferentKeys) + "\n" + str(self.nDifferentChunks) + "\n")
            for key in self.dictFromKeyToNodeFile:
                value = self.dictFromKeyToNodeFile[key]
                keyStr = str(key).rstrip('\n');
                valueStr = str(value).rstrip('\n');
                filePointer.write(keyStr + "\n" + valueStr + "\n")
        return;

    ###
    # Grouping
    ###

    def writeDictio(self,dicFromKeyToListOfValues):
        # At first, we check if all keys are ascociate to a node file.
        # If yes, we copy the old node file
        # Id not, we create an empty node file
        # Then, we write all values in a chunk.
        # At the end, we append the chunkname into the correct node file
		
	# Finally, all nodes ascociated with keys which are not in the actual chunk (but already seen) are copied.

        for key, listOfValues in dicFromKeyToListOfValues.iteritems():
            # We write the list of value into a chunk (for a given key)
            self.nDifferentChunks = self.nDifferentChunks+1;
            chunkFilename = self.chunkNameGenerator(self.nDifferentChunks)
            FileHelper.writeListInFile(chunkFilename,listOfValues)

            # We check if the NodeFile has been created before.
            #   If yes : we copy it into a new file
            #   If not : we create a new empty file
            if self.oldDictFromKeyToNodeFile.has_key(key): # the node file exist
                nodeFileIdx = self.oldDictFromKeyToNodeFile[key];
                self.dictFromKeyToNodeFile[key] = nodeFileIdx;
                oldNodeFileName = self.oldNodeFileNameGenerator(nodeFileIdx);
                nodeFileName = self.nodeFileNameGenerator(nodeFileIdx);
                FileHelper.copyFile(oldNodeFileName,nodeFileName)
            else:
                self.nDifferentKeys = self.nDifferentKeys+1;
                nodeFileIdx = self.nDifferentKeys
                self.dictFromKeyToNodeFile[key] = nodeFileIdx;                
                nodeFileName = self.nodeFileNameGenerator(nodeFileIdx);
                open(nodeFileName, 'w+').close(); # create empty file

            # We append the new chunk name (without the directory) into the node file
            with open(nodeFileName, 'a') as nodePointer:                
                nodePointer.write(chunkFilename + "\n")
                
        for key, listOfValues in self.oldDictFromKeyToNodeFile.iteritems():
            if not dicFromKeyToListOfValues.has_key(key):
                nodeFileIdx = self.oldDictFromKeyToNodeFile[key];
                self.dictFromKeyToNodeFile[key] = nodeFileIdx;
                oldNodeFileName = self.oldNodeFileNameGenerator(nodeFileIdx);
                nodeFileName = self.nodeFileNameGenerator(nodeFileIdx);
                FileHelper.copyFile(oldNodeFileName,nodeFileName)

        return;

    def group(self,context):
        # Group all values into the same list in dict[key].
        dicFromKeyToListOfValues = dict()
        while context.hasNext():
            context.loadNext();            
            if dicFromKeyToListOfValues.has_key(context.key):
                dicFromKeyToListOfValues[context.key].append(context.value)
            else:
                dicFromKeyToListOfValues[context.key] = [context.value]        
        self.writeDictio(dicFromKeyToListOfValues)
        self.saveStateGrouper()        
        return self.getGrouperLogFile(); # return the log file

    ###
    # Global grouping
    ###

    @staticmethod
    def mergeDictionaries(dict1,dict2):
        # Merge 2 dict.
        # If there exist 2 dict[key], then we append the two values into a list.
        # dict2[] must constain strings.        
        outputDict = dict1.copy();        
        for key,value in dict2.iteritems():                        
            if not outputDict.has_key(key): # Initialize dict[key] with a list
                outputDict[key] = [value];
            else:
                outputDict[key].append(value)
        return outputDict

    @staticmethod
    def readSaveStateIntoDictionnary(saveStateName,grouperNum,grouperLastCallNum,directory):
        # read a savestate, a load the dictionnary :
        # For all keys, we have a node file name        
        filePointer = open(saveStateName, "r")        
        nDifferentKeys = int(filePointer.readline())
        nDifferentChunks = int(filePointer.readline())
        outputDict = dict();        
        key = filePointer.readline().rstrip('\n')        
        while len(key) > 0:            
            nodeFileIdx = filePointer.readline().rstrip('\n')
            nodeFileName = Grouper.genericNodeFileName(grouperNum,nodeFileIdx,grouperLastCallNum,directory)            
            outputDict[key] = nodeFileName;
            key = filePointer.readline().rstrip('\n')        
        return outputDict;        

    @staticmethod
    def globalGrouper(listSaveState,listGrouperNum,listLastCallNum,listOfDirectory,globalGrouperDirectory):
        # We read all grouper's last savestates, and put all nodefilename into a global nodefile.
        globalDictListNodeFile = dict()
        globalDictFromKeyToGlobalNodeFile = dict()
        for i in range(0,len(listSaveState)):            
            saveStateName = listSaveState.pop()
            grouperNum = listGrouperNum.pop()
            grouperLastCallNum = listLastCallNum.pop()
            directory = listOfDirectory.pop()            
            globalDictListNodeFile = Grouper.mergeDictionaries(globalDictListNodeFile,Grouper.readSaveStateIntoDictionnary(saveStateName.rstrip('\n'),grouperNum,grouperLastCallNum,directory))                                    

        counter = 0;
        for key, listOfValues in globalDictListNodeFile.iteritems():
            counter = counter + 1;
            globalNodeFileName = Grouper.genericGlobalNodeFileName(counter,globalGrouperDirectory)
            globalDictFromKeyToGlobalNodeFile[key] = globalNodeFileName
            open(globalNodeFileName, 'w+').close(); # create empty file
            for nodeFileName in listOfValues:                
                FileHelper.appendFileInFile(nodeFileName,globalNodeFileName)
        return globalDictFromKeyToGlobalNodeFile



