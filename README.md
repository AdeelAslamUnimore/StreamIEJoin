# SPO-Join: Efficient Distrubuted Stream Inequality Join
# Stream Inequality Join in Distributed Stream Processing Systems

Stream Inequality Join is a fundamental operation but can be computationally expensive due to the regular insertion and updating of tuples within a bounded set of streaming data. It plays a crucial role in scenarios where real-time analysis and correlation of diverse streams of data are required. Distributed Stream Processing Systems offer an efficient and scalable environment for handling complex streaming operations like Inequality Join. These systems distribute the computation across multiple nodes, allowing for parallel processing and better performance in handling large volumes of streaming data. Due to the computational intensity of Stream Inequality Join, it's crucial to carefully design and optimize your queries to achieve the desired performance levels.


Traditionally, tree index data structures are adopted for efficient reterival of inequality join queries such as B+ tree, CSS-tree. However, these indexing structure pose challenges for larger size indexing data structure, due to more updates and index reconstructution. In this work, we use a two-way design where the sliding window content are divided into two distinct data structure, that include insert efficient B+tree, which holds only the fraction of tuples of streaming data and immutable PO-Join that is search efficient and contains majority of tuples. This procedure reduces the insertion updation cost due to smaller size of indexing data structure and provide results efficiently. 

