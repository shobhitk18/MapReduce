##########################################################################
## MyMapReduceSystem.py  v 0.325
##
## Implements a basic version of MapReduce intended to run
## on multiple threads of a single system. This implementation
## is simply intended as an instructional tool for students
## to better understand what a MapReduce system is doing
## in the backend in order to better understand how to
## program effective mappers and reducers. 
##
## MyMapReduce is meant to be inheritted by programs
## using it. See the example "WordCountMR" class for 
## an exaample of how a map reduce programmer would
## use the MyMapReduce system by simply defining
## a map and a reduce method. 
##
##
## Original Code written by H. Andrew Schwartz
## for SBU's Big Data Analytics Course 
## version 0.325 - Spring 2019
##
## Student Name: Shobhit
## Student ID: 


import sys
from abc import ABCMeta, abstractmethod
from multiprocessing import Process, Manager
from pprint import pprint
import numpy as np
from random import random


##########################################################################
##########################################################################
# MapReduceSystem: 

class MyMapReduce:
    __metaclass__ = ABCMeta

    def __init__(self, data, num_map_tasks=5, num_reduce_tasks=3, use_combiner = False): 
        self.data = data  #the "file": list of all key value pairs
        self.num_map_tasks=num_map_tasks #how many processes to spawn as map tasks
        self.num_reduce_tasks=num_reduce_tasks # " " " as reduce tasks
        self.use_combiner = use_combiner #whether or not to use a combiner within map task
        
    ###########################################################   
    #programmer methods (to be overridden by inheriting class)

    @abstractmethod
    def map(self, k, v): 
        print("Need to override map")

    
    @abstractmethod
    def reduce(self, k, vs): 
        print("Need to overrirde reduce")
        

    ###########################################################
    #System Code: What the map reduce backend handles

    def mapTask(self, data_chunk, namenode_m2r, combiner=False): 
        #runs the mappers on each record within the data_chunk and assigns each k,v to a reduce task
        mapped_kvs = [] #stored keys and values resulting from a map 
        for (k, v) in data_chunk:
            #run mappers:
            chunk_kvs = self.map(k, v) #the resulting keys and values after running the map task
            mapped_kvs.extend(chunk_kvs) 
			
	#assign each kv pair to a reducer task
        if combiner:
            #[ADD COMBINER HERE]
            combined_kvs = dict()
            print("\nCombiner starting now.")
            for (k, v) in mapped_kvs:
                try:
                    combined_kvs[k] += 1
                except KeyError:
                    combined_kvs[k] = 1

            for kvpairs in combined_kvs.items():
                namenode_m2r.append((self.partitionFunction(kvpairs[0]), kvpairs))
        else:
            for (k, v) in mapped_kvs:
                namenode_m2r.append((self.partitionFunction(k), (k, v)))


    def partitionFunction(self,k): 
        #given a key returns the reduce task to send it
        node_number = np.sum([ord(c) for c in str(k)]) % self.num_reduce_tasks
        return node_number


    def reduceTask(self, kvs, namenode_fromR): 
        #sort all values for each key (can use a list of dictionary)
        vsPerK = dict()
        for (k, v) in kvs:
            try:
                vsPerK[k].append(v)
            except KeyError:
                vsPerK[k] = [v]


        #call reducers on each key with a list of values
        #and append the result for each key to namenoe_fromR
        for k, vs in vsPerK.items():
            if vs:
                fromR = self.reduce(k, vs)
                if fromR:#skip if reducer returns nothing (no data to pass along)
                    namenode_fromR.append(fromR)

		
    def runSystem(self): 
        #runs the full map-reduce system processes on mrObject

        #[SEGMENT 1]
    # What: <your description here>
    # We are creating two seperate process managers lists. One for the MAP tasks and one for the REDUCE tasks
    # This is basically done to ease out the commumication between MAP-REDUCE tasks. We can think of them as 
    # message queues.

    # Why: <your reasoning here>
    # We are doing this to communicate between different processes which are working on seperate but 
    # related tasks in order to achieve a cumulative result in the end.
    
    #the following two lists are shared by all processes
        #in order to simulate the communication
        namenode_m2r = Manager().list() #stores the reducer task assignment and 
                                          #each key-value pair returned from mappers
                                          #in the form: [(reduce_task_num, (k, v)), ...]
                                          #[COMBINER: when enabled this might hold]
        namenode_fromR = Manager().list() #stores key-value pairs returned from reducers
                                          #in the form [(k, v), ...]
        
        #[SEGMENT 2]
    # What: <your description here>
    # We are dividing the whole data into chunks basically this is same as sharding the data and giving
    # each chunk to a seperate process <Map task> for processing.
    # 
    # Why: <your reasoning here>
    # We are doing this to increase throughput since each chunk can be processed by the map task 
    # in parallel and then we can send out the results from these map jobs to the reducers for final 
    # aggregation.
    
    #divide up the data into chunks accord to num_map_tasks, launch a new process
        #for each map task, passing the chunk of data to it. 
        #the following starts a process
        #      p = Process(target=self.mapTask, args=(chunk,namenode_m2r))
        #      p.start()  
        processes = []
        chunkSize = int(np.ceil(len(self.data) / int(self.num_map_tasks)))
        chunkStart = 0
        while chunkStart < len(self.data):
            chunkEnd = min(chunkStart+chunkSize, len(self.data))
            chunk = self.data[chunkStart:chunkEnd]
            #print(" starting map task on ", chunk) #debug
            processes.append(Process(target=self.mapTask, args=(chunk,namenode_m2r,self.use_combiner)))
            processes[-1].start()
            chunkStart = chunkEnd

        #[SEGMENT 3]
    # What: <your description here>
    # We are looping over all the map processes i.e map tasks and calling join on them. Join will actually
    # cause the parent thread to wait till the child executes. Basically waiting for map task to complete.

    # Why: <your reasoning here>
    # We are starting all the map tasks simultaneously and once the processes return they will have the
    # results (reduce_task_num, (k, v)) in the namenode_m2r list. Since we cannot run reduce before
    # every map tasks finishes, hence we need to wait for them to complete, therefore we have .join() 

    #join map task processes back
        for p in processes:
            p.join()
                #print output from map tasks 
        print("namenode_m2r after map tasks complete:")
        pprint(sorted(list(namenode_m2r)))

        ##[SEGMENT 4]
    # What: <your description here>
    # We have already partitioned key value pairs generated by the map tasks into 
    # groups = #Number of reduce tasks. Here we are just gathering them together in a list of list
    # where all k-v pairs for a reduce node are present together as a list in this list of list.
    
    # Why: <your reasoning here>
    # This is required because we want to start each reduce job at their respective reduce node with
    # all the keys which belong to that reduce node. 

    #"send" each key-value pair to its assigned reducer by placing each 
        #into a list of lists, where to_reduce_task[task_num] = [list of kv pairs]
        to_reduce_task = [[] for i in range(self.num_reduce_tasks)] 
        for (rtask_num, kv) in namenode_m2r:
            to_reduce_task[rtask_num].append(kv)

    #[SEGMENT 5]
    # What: <your description here>
    # Here we loop over total number of reduce tasks to launch and one by one assign
    # them their respective list of keys. We also specify the reduce function as the argument and 
    # also pass the placeholder for resulting list/output from the reduce nodes so as to collect them 
    # in one place.
    
    # Why: <your reasoning here>
    # We need to do this operation of reduce/aggregation in parallel in order to achieve good performance 
    # therefore we launch seperate processes for every reduce task with the set of keys allocted to them
    # on which they operate and finally produce the results. 
        #launch the reduce tasks as a new process for each. 
        processes = []
        for kvs in to_reduce_task:
            processes.append(Process(target=self.reduceTask, args=(kvs, namenode_fromR)))
            processes[-1].start()

    #[SEGMENT 6]
    # What: <your description here>
    # We finally wait for all the reduce jobs to finish. At this point all the reduce jobs have been
    # completed and the results of the reduce operation is present in a sort of gloabal list namenode_fromR
    # We then sort this list based on key and print the result.
    
    # Why: <your reasoning here>
    # It is important to wait for all the reduce jobs to finish before we can actually output the complete
    # data because each reduce task is working on a distinct set of keys and we want to get result for each 
    # of those keys before outputting result back to the user. 
    #
        #join the reduce tasks back
        for p in processes:
            p.join()
        #print output from reducer tasks 
        print("namenode_fromR after reduce tasks complete:")
        pprint(sorted(list(namenode_fromR)))

        #return all key-value pairs:
        return namenode_fromR


##########################################################################
##########################################################################
##Map Reducers:
            
class WordCountMR(MyMapReduce): #[DONE]
    #the mapper and reducer for word count
    def map(self, k, v): #[DONE]
        counts = dict()
        for w in v.split():
            w = w.lower() #makes this case-insensitive
            try:  #try/except KeyError is just a faster way to check if w is in counts:
                counts[w] += 1
            except KeyError:
                counts[w] = 1
        return counts.items()
    
    def reduce(self, k, vs): #[DONE]
        return (k, np.sum(vs))        

class WordCountBasicMR(MyMapReduce): #[DONE]
    #mapper and reducer for a more basic word count 
	# -- uses a mapper that does not do any counting itself
    def map(self, k, v):
        kvs = []
        counts = dict()
        for w in v.split():
            kvs.append((w.lower(), 1))
        return kvs

    def reduce(self, k, vs): 
        return (k, np.sum(vs))  
		

class SetDifferenceMR(MyMapReduce): 
    #contains the map and reduce function for set difference
    #Assume that the mapper receives the "set" as a list of any primitives or comparable objects
    def map(self, k, v):
        toReturn = []
        for i in v:
            toReturn.append((i, k))
        return toReturn

    def reduce(self, k, vs):
        if len(vs) == 1 and vs[0] == 'R':
            return k
        else:
            return None

class MatrixMultMR(MyMapReduce): #[TODO]
    def map(self, k, v):
        #<COMPLETE>
        label = k[0]
        row = k[1]
        col = k[2]
        mapped_kv = []
        mat, sA, sB = label.split(":")
        m1, n1 = sA.split(",")
        m2, n2 = sB.split(",")
        
        if mat == 'A':
            for k in range(0,int(n2)):
                mapped_kv.append(((row,k),(mat,col,v)))
        elif mat == 'B':
            for i in range(0,int(m1)):
                mapped_kv.append(((i,col),(mat,row,v)))
        
        #print(mapped_kv)
        return mapped_kv
    
    def reduce(self, k, vs):
        #<COMPLETE>
        listA = [mat for mat in vs if mat[0] == 'A']
        listB = [mat for mat in vs if mat[0] == 'B']
        listA = sorted(listA , key=lambda val: val[1])
        listB = sorted(listB , key=lambda val: val[1])
        
        j = 0
        listR = [] 
        for elem in listA:
            while j < len(listB) and listB[j][1] <= elem[1]:
                if elem[1] == listB[j][1]:
                    listR.append(elem[2]*listB[j][2])
                #We can just fall through without break as it will be handled in while loop cond
                j += 1
            
            if j >= len(listB):
                break
                    
        res = np.sum(listR)
        return (k, res)
			
			
##########################################################################
##########################################################################

from scipy import sparse
def createSparseMatrix(X, label):
	sparseX = sparse.coo_matrix(X)
	list = []
	for i,j,v in zip(sparseX.row, sparseX.col, sparseX.data):
		list.append(((label, i, j), v))
	return list

if __name__ == "__main__": #[Uncomment peices to test]
    
    ###################
    ##run WordCount:
    
    print("\n\n*****************\n Word Count\n*****************\n")
    data = [(1, "The horse raced past the barn fell"),
            (2, "The complex houses married and single soldiers and their families"),
            (3, "There is nothing either good or bad, but thinking makes it so"),
            (4, "I burn, I pine, I perish"),
            (5, "Come what come may, time and the hour runs through the roughest day"),
            (6, "Be a yardstick of quality."),
            (7, "A horse is the projection of peoples' dreams about themselves - strong, powerful, beautiful"),
            (8, "I believe that at the end of the century the use of words and general educated opinion will have altered so much that one will be able to speak of machines thinking without expecting to be contradicted."),
            (9, "The car raced past the finish line just in time."),
	    (10, "Car engines purred and the tires burned.")]
    '''
    print("\nWord Count Basic WITHOUT Combiner:")
    mrObjectNoCombiner = WordCountBasicMR(data, 4, 3)
    mrObjectNoCombiner.runSystem()
    
    print("\nWord Count Basic WITH Combiner:")
    mrObjectWCombiner = WordCountBasicMR(data, 4, 3, use_combiner=True)
    mrObjectWCombiner.runSystem()
    '''
  
    ####################
    ##run SetDifference (nothing to do here; just another test)
    print("\n\n*****************\n Set Difference\n*****************\n")
    test1=[('R',[]), ('S',[1,2])]
    test2=[('R',[1,3,5]), ('S',[2,4])]
    test3=[('R',range(1,50001)), ('S',range(2,50000))]
    mrObject = SetDifferenceMR(test1, 2, 2)
    mrObject.runSystem()
    mrObject = SetDifferenceMR(test2, 2, 2)
    mrObject.runSystem()
    '''
    mrObject = SetDifferenceMR(test3, 2, 2, use_combiner=True)
    mrObject.runSystem()
    '''
      
    ###################
    ##run Matrix Multiply:
    
    print("\n\n*****************\n Matrix Multiply\n*****************\n")
    #format: 'A|B:A.size:B.size
    test1 = [(('A:2,1:1,2', 0, 0), 2.0), (('A:2,1:1,2', 1, 0), 1.0), (('B:2,1:1,2', 0, 0), 1), (('B:2,1:1,2', 0, 1), 3)]
    test2 = createSparseMatrix([[1, 2, 4], [4, 8, 16]], 'A:2,3:3,3') + createSparseMatrix([[1, 1, 1], [2, 2, 2], [4, 4, 4]], 'B:2,3:3,3')
    
    test3 = createSparseMatrix(np.random.randint(-10, 10, (10,100)), 'A:10,100:100,12') + \
	    createSparseMatrix(np.random.randint(-10, 10, (100,12)), 'B:10,100:100,12')

    #print(test2[:10])
    #print(test3[:10])
        
    mrObject = MatrixMultMR(test1, 4, 3)
    mrObject.runSystem()

    mrObject = MatrixMultMR(test2, 6, 4)
    mrObject.runSystem()
    
    mrObject = MatrixMultMR(test3, 16, 10)
    mrObject.runSystem()
	