# Dispatcher is responsible for dispatching tasks between mapper, doing the grouping, and then dispatching tasks amongst reducers

# 1. Run chunkfactory, and feed the output to Mapper. This doesn't have to be sequential
# 2. Group all outputs of all mapper togheter
# 3. Feed reducers with the result of grouping
# 4. Terminates with some postprocessing of the reducer's outputs

print '\n------------------------------\nRunning dispatcher\n------------------------------\n'

# Test
from chuncksFactory import ChuncksFactory
from mapChunkIterator import MapChunkIterator
from mapper import Mapper
from grouper import Grouper
from reducer import Reducer
from mapContext import MapContext
from reduceContext import ReduceContext
from reduceFromGroupIterator import ReduceFromGroupIterator
from groupChunkFromMapIterator import GroupChunkFromMapIterator

from Queue import Queue
from threading import Thread

def mapChunksNameGenerator(idx):
    #return 'chunks_map/dispatcher_chunk_n'+str(idx)
    return '/Users/lcambier/TempMapReduce/chunks_map2/dispatcher_chunk_n'+str(idx)


def groupChunksNameGenerator(idx):
    #return 'chunks_map/dispatcher_group_chunk_n'+str(idx)
    return '/Users/lcambier/TempMapReduce/chunks_map2/dispatcher_chunk_n'+str(idx)


##################
# Create chuncks #
##################
print 'Creating chuncks ...'
cf = ChuncksFactory(['files/file4'])
cf.divideIntoChunks(mapChunksNameGenerator)
print str(cf.nChunks) + ' chunks created.'

###################
# Prepare grouper #
###################
totalNumberOfGrouper = 2 ; # One grouper = 1 core, basically
listGrouperNum = [0 for i in range(totalNumberOfGrouper)];
listLastCallNum = [-1 for i in range(totalNumberOfGrouper)];
saveStateNameGrouper = [0 for i in range(totalNumberOfGrouper)];
listGrouperNum = range(0,totalNumberOfGrouper);
directory = '/Users/lcambier/TempMapReduce/mapper_and_groupper_logs2/'

##################################
# Map and local group (parallel) #
################################## (oui, je sais, s'pas encore top (pas de queue de job, mais juste dispatche entre les threads, mais j'experimente)
print '------------------------------'
print 'Mapping and local grouping ...'
print '------------------------------'
chunkLeft = cf.nChunks ;

def job(grouperNum, chunksQueue, listSaveStateNameGrouper, listListLastCallNum):
    print 'Starting worker ' + str(grouperNum)      
    while True:
        # Get new chunck to process
        chunk = chunksQueue.get()         
        # Work
        print 'Worker ' + str(grouperNum) + ' mapping chunk ' + str(chunk)
        MapIterator = MapChunkIterator(mapChunksNameGenerator(chunk)) # Iterator to iterate through the chunck        
        theContext = MapContext(groupChunksNameGenerator(chunk),MapIterator)        
        Mapper.map(theContext)
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
    worker = Thread(target=job, args=(i, chunksQueue, saveStateNameGrouper, listLastCallNum))
    worker.setDaemon(True)    
    worker.start()
# Feed workers
for chunk in range(chunkLeft):
    chunksQueue.put(chunk)
# Wait for map completion  
print 'Main thread waiting ...'
chunksQueue.join()
print 'All workers have finished.'

print 'Mapping and local grouping done. {} chuncks grouped by {} threads.'.format(cf.nChunks, totalNumberOfGrouper)

################
# Global group #
################
print '------------------'
print('Global grouping...')
print '------------------'
listOfDirectory = []
globalGrouperDirectory = '/Users/lcambier/TempMapReduce/mapper_and_groupper_logs2/'
for i in range(0,totalNumberOfGrouper):
    listOfDirectory.append('/Users/lcambier/TempMapReduce/mapper_and_groupper_logs2/')
globalDict = Grouper.globalGrouper(saveStateNameGrouper,listGrouperNum,listLastCallNum,listOfDirectory,globalGrouperDirectory)
print('Global grouping done.')

############
# Reducing #
############
print '------------'
print('Reducing ...')
print '------------'
outputDict = dict()
for key, globalNodeFileName in globalDict.iteritems():
    reduceIterator = ReduceFromGroupIterator(globalNodeFileName)
    theReduceContext = ReduceContext(key,reduceIterator)
    outputDict[key] = Reducer.reduce(theReduceContext)
print('Reducing done.')

##########
# OUTPUT #
##########
print '\n------------------------------\nOutput\n------------------------------\n'
for key in outputDict :
    print str(key) + ' - ' + str(outputDict[key])
# print 'apta : ' + str(outputDict['apta']) + ' vs 7'