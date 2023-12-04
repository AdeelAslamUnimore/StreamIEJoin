# SPO-Join: Efficient Distrubuted Stream Inequality Join

Stream Inequality Join is a fundamental operation but can be computationally expensive due to the regular insertion and updating of tuples within a bounded set of streaming data. It plays a crucial role in scenarios where real-time analysis and correlation of diverse streams of data are required. Distributed Stream Processing Systems offer an efficient and scalable environment for handling complex streaming operations like Inequality Join. These systems distribute the computation across multiple nodes, allowing for parallel processing and better performance in handling large volumes of streaming data. Due to the computational intensity of Stream Inequality Join, it's crucial to carefully design and optimize your queries to achieve the desired performance levels.


Traditionally, tree index data structures are adopted for efficient reterival of inequality join queries such as B+ tree, CSS-tree. However, these indexing structure pose challenges for larger size indexing data structure, due to more updates and index reconstructution. In this work, we use a two-way design where the sliding window content are divided into two distinct data structure, that include insert efficient B+tree, which holds only the fraction of tuples of streaming data and immutable PO-Join that is search efficient and contains majority of tuples. This procedure reduces the insertion updation cost due to smaller size of indexing data structure and provide results efficiently. We name the complete join procedure is SPO-Join.
## Design of SPO Join:
This SPO is depicted by the Figure given below. New Tuple from any field is first index on his respective sliding window, based indexing data structure and needs to probe the opposite stream indexing structures. As sliding window is divided so the all new tuple is broadcasted to both designs of other indexing data structure. Final result is the accumulated result of both indexing designs
![SolutionOverview](https://github.com/AdeelAslamUnimore/StreamIEJoin/assets/98392202/2c60419c-28bd-4f94-960a-68e515f31fe3) 
## Design of Distributed SPO-Join:
### Setting Up the Cluster

Follow these guidelines to set up your cluster for running in cluster mode. Detailed instructions for each component can be found in the relevant links provided:
Fundamental requirement:
- Java above 1.8
- Python 3.5
- OS Linux perfer on all nodes

#### Nimbus (Master Node):
- Set up Nimbus as the master node.
- Detailed instructions: ([link_to_nimbus_setup](https://storm.apache.org/))

#### Supervisor (Supervisor Node):
- Set up Supervisor nodes.
- Detailed instructions: ([link_to_supervisor_setup](https://storm.apache.org/))

#### Zookeeper Nodes:
- Configure Zookeeper nodes for distributed coordination.
- Detailed instructions: ([link_to_zookeeper_setup](https://zookeeper.apache.org/))

#### Kafka Node:
- Set up Kafka for distributed event streaming.
- Detailed instructions: ([link_to_kafka_setup](https://kafka.apache.org/))

#### Redis Server:
- Configure Redis as a caching server.
- Detailed instructions: ([link_to_redis_setup](https://redis.io/))

#### Time Sync:
- Use Nimbus node as the time sync server.
- Ensure all nodes are synchronized for accurate time.
## Code POM Files
Make sure to include POM files in your program
```
        <dependencies>

        <dependency>
            <groupId> org.apache.storm</groupId>
            <artifactId> storm-core</artifactId>
            <version>2.4.0</version>
        </dependency>
        <dependency>
            <groupId>org.apache.storm</groupId>
            <artifactId>storm-kafka-client</artifactId>
            <version>2.4.0</version>
        </dependency>
        <dependency>
            <groupId>com.mysql</groupId>
            <artifactId>mysql-connector-j</artifactId>
            <version>8.0.33</version>
        </dependency>
        <dependency>
            <groupId>org.apache.kafka</groupId>
            <artifactId>kafka_2.10</artifactId>
            <version>0.10.1.0</version>

        </dependency>
        <dependency>
            <groupId>org.apache.kafka</groupId>
            <artifactId>kafka-clients</artifactId>
            <version>0.10.1.0</version>

        </dependency>
        <dependency>
            <groupId>redis.clients</groupId>
            <artifactId>jedis</artifactId>
            <version>5.0.0</version>
        </dependency>

    </dependencies>
```



