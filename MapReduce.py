
from ChuncksFactory import ChuncksFactory
from MapChunkIterator import MapChunkIterator
from Grouper import Grouper
from MapContext import MapContext
from ReduceContext import ReduceContext
from ReduceFromGroupIterator import ReduceFromGroupIterator
from GroupChunkFromMapIterator import GroupChunkFromMapIterator
from Queue import Queue
from threading import Thread
import os

class MapReduce:



    def __init__(self,theMapper,theReducer,listOfFiles,silent=0,nThreads=1):
        #
        #
        #   Inputs :
        #       -   theMapper
        #
        #       -   theReducer
        #
        #       -   listOfFiles [list(string)]
        #               a list containing all files that need to be analysed.
        #
        #       -   silent [Integer]
        #                1) print nothing
        #                0) print the evolution of the algorithm
        #               -1) print also the output
        #
        #       -   nThreads
        #               Number of threads that will be used to execute the MapReduce algortihm.
        #               Note that if the number of chuncks is lower than the number of threads, then nThreads = nChuncks.
        #
        
        if silent<1:
            print('Creating MapReduce with silent {} and nThreads {}'.format(silent,nThreads)) ;

        self.Mapper = theMapper;
        self.Reducer = theReducer;
        self.listOfFiles = listOfFiles;
        self.nThreads = nThreads;
        self.silent = silent;
        if(nThreads<1) :
            raise ValueError ('nThreads must be >=1')
        

    def execute(self):
        
        def mapChunksNameGenerator(idx):
            pathName = 'mc/'
            if not os.path.exists(pathName):
                os.makedirs(pathName)
            return pathName + 'mc_'+str(idx) # map chunk


        def groupChunksNameGenerator(idx):
            pathName = 'gc/'
            if not os.path.exists(pathName):
                os.makedirs(pathName)
            return pathName + 'gc_'+str(idx) # grouper chunk
        
        ##################
        # Create chuncks #
        ##################
        if self.silent<1:
            print 'Creating chuncks ...'
        cf = ChuncksFactory(self.listOfFiles)
        cf.divideIntoChunks(mapChunksNameGenerator)
        if self.silent<1:
            print str(cf.nChunks) + ' chunks created.'

        ###################
        # Prepare grouper #
        ###################
        totalNumberOfGrouper = min(self.nThreads,cf.nChunks) ; # One grouper = 1 core, basically
        if self.silent<1:
            print 'Actual number of threads to be used : {}'.format(totalNumberOfGrouper)
        listGrouperNum = [0 for i in range(totalNumberOfGrouper)];
        listLastCallNum = [-1 for i in range(totalNumberOfGrouper)];
        saveStateNameGrouper = [0 for i in range(totalNumberOfGrouper)];
        listGrouperNum = range(0,totalNumberOfGrouper);
        directory = 'mgl/' # mapper and grouper log
        if not os.path.exists(directory):
            os.makedirs(directory)

        ##################################
        # Map and local group (parallel) #
        ##################################
        
        if self.silent<1:
            print '------------------------------'
            print 'Mapping and local grouping ...'
            print '------------------------------'
        chunkLeft = cf.nChunks ;

        def jobMap(grouperNum, chunksQueue, listSaveStateNameGrouper, listListLastCallNum):
            if self.silent<1: 
                print 'Starting mapper worker ' + str(grouperNum)      
            while True:
                # Get new chunck to process
                chunk = chunksQueue.get()         
                # Work
                if self.silent<1:
                    print 'Worker ' + str(grouperNum) + ' mapping chunk ' + str(chunk)
                MapIterator = MapChunkIterator(mapChunksNameGenerator(chunk)) # Iterator to iterate through the chunck        
                theContext = MapContext(groupChunksNameGenerator(chunk),MapIterator)        
                self.Mapper.map(theContext)
                if self.silent<1:
                    print 'Worker ' + str(grouperNum) + ' grouping locally chunck ' + str(chunk)        
                idx = listListLastCallNum[grouperNum]+1        
                theGrouper = Grouper(grouperNum,idx,idx-1,directory);        
                listSaveStateNameGrouper[grouperNum] = theGrouper.group(theContext)
                listListLastCallNum[grouperNum] = idx ;      
                # "Close" chunk
                chunksQueue.task_done()

        # Create workers
        chunksQueue = Queue()
        for i in range(totalNumberOfGrouper):
            worker = Thread(target=jobMap, args=(i, chunksQueue, saveStateNameGrouper, listLastCallNum))
            worker.setDaemon(True)    
            worker.start()
        # Feed workers
        for chunk in range(chunkLeft):
            chunksQueue.put(chunk)
        # Wait for map completion
        if self.silent<1:
            print '\nMain thread waiting ...'
        chunksQueue.join()
        if self.silent<1:
            print 'All workers have finished.'
            print 'Mapping and local grouping done. {} chuncks grouped by {} threads.'.format(cf.nChunks, totalNumberOfGrouper)

        ################
        # Global group #
        ################
        if self.silent<1:
            print '------------------'
            print('Global grouping...')
            print '------------------'
        listOfDirectory = []
        globalGrouperDirectory = 'mgl/' # mapper and grouper log
        for i in range(0,totalNumberOfGrouper):
            listOfDirectory.append(globalGrouperDirectory)
        globalDict = Grouper.globalGrouper(saveStateNameGrouper,listGrouperNum,listLastCallNum,listOfDirectory,globalGrouperDirectory)
        if self.silent<1:
            print('Global grouping done.')

        ############
        # Reducing #
        ############

        def jobReduce(reducerNum, jobsQueue, outputDictionary):            
            if self.silent<1: 
                print 'Starting reducer worker ' + str(reducerNum)      
            while True:
                # Get new (key,globalNodeFileName) to process
                (key, globalNodeFileName) = jobsQueue.get()         
                # Work                
                reduceIterator = ReduceFromGroupIterator(globalNodeFileName)
                theReduceContext = ReduceContext(key,reduceIterator)
                outputDictionary[key] = self.Reducer.reduce(theReduceContext)                
                # "Close" job
                jobsQueue.task_done()

        # Create workers
        jobsQueue = Queue()
        outputDict = dict()
        for i in range(totalNumberOfGrouper):
            worker = Thread(target=jobReduce, args=(i, jobsQueue, outputDict))
            worker.setDaemon(True)    
            worker.start()
        # Feed workers
        for key, globalNodeFileName in globalDict.iteritems():
            jobsQueue.put((key, globalNodeFileName))
        # Wait for reduce completion
        if self.silent<1:
            print '\nMain thread waiting ...'
        jobsQueue.join()
        if self.silent<1:
            print 'All workers have finished.'
            print 'Reducing done by {} threads.'.format(totalNumberOfGrouper)


        # Serial version
        # if self.silent<1:
        #     print '------------'
        #     print('Reducing ...')
        #     print '------------'
        # outputDict = dict()
        # for key, globalNodeFileName in globalDict.iteritems():
        #     reduceIterator = ReduceFromGroupIterator(globalNodeFileName)
        #     theReduceContext = ReduceContext(key,reduceIterator)
        #     outputDict[key] = self.Reducer.reduce(theReduceContext)
        # if self.silent<1:
        #     print('Reducing done.')
        

        ##########
        # OUTPUT #
        ##########
        if self.silent == -1:
            print '\n------------------------------\nOutput\n------------------------------\n'
            for key in outputDict :
                print str(key) + ' - ' + str(outputDict[key])
        return outputDict;
