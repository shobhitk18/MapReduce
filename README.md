# MapReduce
Implementing matrix multiplication using MR and optimizing it using Combiner

1. Combiner
Edit the “MapTask” method to add support for running a Combiner. Look for “#[ADD COMBINER HERE]” for the place one would add this. Remember, a combiner runs the reduce task at the end of the map task in order to save communication cost of sending to multiple reducers. Note: main will run the WordCountBasicMR to test with and without the combiner. It is recommended that you look over the WordCountBasicMR to understand what it is doing.  You can assume your combiner code will only run on reducers that are both commutative and associative (see hint at bottom).
2. Matrix multiply 
Edit the “map” and “reduce” methods of “MatrixMultMR” to implement matrix multiplication across map-reduce. The MMDS book goes over serveral matrix multiplication algorithms for map-reduce. Here, you must use a version that has only one map and one reduce function. The matrices are stored in sparse format, such that each record will have the key: (<label>, <row>, <col>) and a single real value. For example, a stream of records may look like: 
[((“A:nA,mA:nB,mB”, 0, 0), 3.5), ((“A:nA,mA:nB,mB ”. 0, 1), 1.0), … (“B:nA,mA:,nB,mB”, 0, 0), 1), ((“B:nA,mA:,nB,mB”. 0, 1), 3), … ]
Output should be:
[(“AxB:n,m”, i, j), v), …]
You can assume matrices will be named A and B.  nA, mA and nB, mB will be dimensions of the matrices 
