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
## Code POM Files:
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
## How to run a code
## Parameter Settings

In the code, there is a static class `com.configurationsandconstants.iejoinandbaseworks` containing constants. Update the following constants for your configuration:

- **Sliding Window:** Adjust the sliding window constant in the `com.configurationsandconstants.iejoinandbaseworks` class.

- **Slide Interval:** Modify the slide interval constant in the same class.

- **Tree Order:** Update the tree order constant as needed.

## Self Join Query

### Using Class Topology from "com.experiment.selfjoin"

1. Call the method in the main class with the following parameters:
   - `args[0]`: Kafka servers
   - `args[1]`: Topic name

2. Adjust the record translator part of the code. For example, in the NYC taxi dataset, it may look like this:

   ```java
   String[] splitValues = record.value().split(","); // Split record.value() based on a delimiter, adjust it as needed
   double value1, value2 = 0;
   try {
       value1 = Double.parseDouble(splitValues[5]);
       value2 = Double.parseDouble(splitValues[11]);
   } catch (NumberFormatException e) {
       value1 = 0;
       value2 = 0;
   }
   long kafkaTime = Long.parseLong(splitValues[splitValues.length - 1]);
   int[] id = {0}; // Assuming id is an integer array, adjust as needed
   return new Values((int) Math.round(value1), (int) Math.round(value2), id[0], kafkaTime, System.currentTimeMillis());
### Deploy the topology on cluster:
To deploy your project, it is suggested to create a JAR file and submit it to the master node of the cluster. Follow the steps below, preferably using IntelliJ IDEA:

1. **Create a Compiled JAR Using Maven:**
   - Open your project in IntelliJ IDEA.
   - Ensure that you have a `pom.xml` file in the root of your project.
   - In IntelliJ, navigate to the Maven tool window (usually on the right side).
   - Run the "clean" and "install" goals to compile and package your project into a JAR file.
   - The JAR file will be created in the `target` directory of your project.

2. **Create a JAR Using IntelliJ IDEA Artifacts:**
   - Open your project in IntelliJ IDEA.
   - Navigate to `File` > `Project Structure`.
   - In the Project Structure dialog, go to `Artifacts`.
   - Click on the `+` icon and select `JAR` > `From modules with dependencies...`.
   - Choose the module containing your main class.
   - Set the `Main Class` to your main class.
   - Click `OK` to close the dialog.

3. **Build the Artifact:**
   - Go to `Build` > `Build Artifacts`.
   - Select the artifact you just created.
   - Choose `Build`.
   - The JAR file will be created in the `out/artifacts` directory of your project.
## Deploying the JAR as a Storm Topology

1. **Rename the JAR File:**
   - Rename the generated JAR file to a suitable name. For example, for self join re-named `NYCSELFJOIN.jar`.

2. **Transfer the JAR to the Master Node:**
   - Transfer the renamed JAR file to the master node of your cluster using a tool like `scp` or any other method that suits your environment.

3. **Run the Storm Topology:**
   - Use the following command to submit and run the Storm topology on the cluster:
     ```bash
     storm jar storm_topology.jar com.proposed.iejoinandbplustreebased.CrossJoinTopology Kafka1:9092 selfjoin
     ```
     - Replace `storm_topology.jar` with the actual name of your JAR file.
     - `com.proposed.iejoinandbplustreebased.CrossJoinTopology` is the main class for your Storm topology. Adjust it based on your project structure.
     - `Kafka1:9092` is the Kafka server information.
     - `selfjoin` is the topic name.

### Cross Join

For cross join functionality, use the `com.proposed.iejoinandbplustreebased` package. Adjust your topology demands in the respective package according to your project requirements.

For example, you might need to modify the parameters, configurations, or logic within the `CrossJoinTopology` class to achieve the desired cross join behavior.

Ensure that the input data, Kafka configuration, and any other relevant settings are appropriately configured for the cross join operation.

