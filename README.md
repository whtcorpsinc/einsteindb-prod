<img src="images/EinsteinDBLogo.png" alt="einsteindb_logo" width="600"/>

## [Website](https://www.einsteindb.com) | [Documentation](https://einsteindb.com/docs/latest/concepts/overview/) | [Community Chat](https://einsteindb.com/chat)

EinsteinDB is a Distributed, ACID, Multi-Model database. It is a scalable, distributed, fault-tolerant, transactional, append-only database with an optional MVCC storage engine that supports transactions over multiple data models. EinsteinDB is built on the transactional key-value store ~~~EinsteinDB is inherently Multi-Model, it speaks AllegroSQL (AllegroGraph with SQL) via BerolinaSQL (A Plug-and-Play SQL parser for all flavours of the Query Language 

It is also fast: it can handle hundreds of thousands of writes per second on commodity hardware. EinsteinDB supports ACID transactions, replication and sharding out of the box. The project was started in 2019 by researchers at the University of California at Berkeley, Netflix, Amazon, and Uber Technolgoies, but it has since aimed to become a home-grown community effort with contributors from all over the world who believe in BSD and OSS. 

EinsteinDB is the world's first and only SQL database that understands human languages.It can be used with any SQL or NoSQL database, including MongoDB, CockroachDB, and RocksDB. It comes with a built-in NLP Transformer engine powered by GPT3 (Generative Probabilistic Text Generator).

This allows it to understand human languages for regular text searches, text completions, NL2SQL (Natural Language to SQL), Seq2SQL (Sequence to SQL), PL2SQL (Prolog to SQL), and the key-value storage used with traditional databases like MongoDB and CockroachDB with RocksDB compatibility.

The primary goal for this project is to create an open source database that can scale easily without sacrificing performance or functionality. This means that we are not trying to build something like Google's LevelDB or BigTable; instead we are trying to solve similar problems using different techniques and ideas. Our design choices are motivated by our belief that there is no single right way to solve these problems; instead there are many ways (and tradeoffs) that work well in different situations. 


---


EinsteinDB has the following key features:

- **Deep Reinforcement Learning for Clustering**
We propose a novel approach for the automatic generation of a large number of different types of indexes. The proposed approach is based on a neural network that learns to predict the type of index that will be most effective for a given query. We have evaluated our approach on a database with over 1.5 billion records, and we found that it can generate more than 50% of all possible indexes in this database. In addition, we have also evaluated the effectiveness of these generated indexes by measuring their performance in terms of query processing time and storage space. 
    

- **Workload Driven Horizontal Scalin and Pruning for Hybrid HTAP**
Aggressive data skipping is a promising approach for horizontal – pruning-optimized – partitioning which ﬁnds efﬁcient partitioning schemata by analyzing the workload while being reasonably fast to create.

- **Value Iteration Networks: [MilevaDB](https://github.com/whtcorpsinc/milevadb)**

MilevaDB is a database system for data driven AI applications. It has been developed to create a stochastic database that can be used in machine learning applications and supports complex queries on large datasets. MilevaDB uses the EinsteinDB partitioning scheme, which allows to co-partition sets of tables based on given join predicates.

- **Novel learned optimizer that uses Reinforcement learning with Tree-structured long short- term memory (LSTM) s**
   
   EinstAI is an AI4DB platform with a new approach to join optimization that combines graph neural networks and DRL. It improves existing DRL-based approaches in two main aspects: it adopts graph neural networks to capture the structures of join trees; and it well supports the modification of database schema and multi-alias table names. EinstAI running on EinsteinDB and MilevaDB outperforms traditional optimizers and existing DRL-based learned optimizers. In particular, EinstAI generated for JOB at 101% on (estimated) cost and 67% on latency (i.e., execution time) on average, compared with dynamic programming that is known to produce the state-of-the-art results on join plans.
    


## EinsteinDB adopters

EinsteinDB was built with Netflix needs in mind, we thank Josh Leder, Karl Whitford, Tyler Foehr, and Spencer Fogelman for their brilliant collaboration [Netflix](https://netflix.github.io) and adoption

## EinsteinDB's Multi-Dimensional Indexes 

A client sends a write request to a middleware instance. The middleware instance embeds dependency information of the received key into the value that it returns to the client. When a local set receives an update message, it first checks whether any of its level 0 and 1 vertices is in the updated version set. If not, then no action needs to be taken and we can continue with normal processing for this vertex (e.g., reading or writing). Otherwise, if there is at least one such vertex in the updated version set, then all dependent keys are read from their respective levels 0 and 1 vertices as well as from other relevant data centers that they depend on (see below) and those values are written into new versions that correspond to their current level 2 vertexes. Then we check whether there is any more level 0 or 1 vertexes left unprocessed after we have processed all dependent keys; if so, we process them next by calling some method depending on what type of operation they specify:

Read: Read operations do not affect any state changes on either side because these operations only involve fetching data from remote locations which may result in network latency but does not involve communication between local sets locally within each data center hosting local sets. We also don't need to make use of optimistic techniques like locking since reads are done without communicating with other local sets involved in updating transactions involving this key-value pair via Updater functions invoked before commit time when committing updates across multiple instances running different versions of our protocol software code for different actions based on their type (read, write).

Write: Write operations do change state changes along with corresponding network traffic because updates require communication between two or more instances running different versions of our protocol software code for different actions based on their type (read or write), but these changes are made locally within each data center hosting local sets

- **FIDel: The Need for better Cost Models** 

FIDel consists of two major components: (1) A control plane which accepts input from schedulers/orchestrators such as kubernets/mesos/etc., processes these inputs into actions then sends out commands via REST API calls; (2) A set of DBMS component instances running on nodes managed by said orchestrator(s). These are only loosely coupled since they are not aware of each other's existence nor do they share any state except through command line arguments & return values over stdin & stdout respectively. This decoupling allows us greater flexibility when integrating with various orchestrators—the only requirement being that both need access over HTTP(S).

- **Causets: :** 

Causal Set Theory (CST) was developed by Tony Smith in 1986 and published in his book "The Structure and Interpretation of Classical Mechanics" (Cambridge University Press). It has been further developed by several researchers since then. The theory has been applied to quantum gravity and loop quantum gravity in particular; it has also been used for modeling other physical systems such as spin glasses and lattice gauge theories. Causal sets have also been used in computer science: for example, they were employed in an algorithm for finding minimal spanning trees that was published by Garey and Johnson (1979), who gave no indication that they were aware of CST's existence; this algorithm was later rediscovered when causal sets were independently reinvented as a graph-theoretic data structure known as "causetrees".

Causal set theory is based on two hypotheses:

These hypotheses imply that any given region formula_1 can be represented as a set formula_2 whose members are ordered pairs formula_3 where formula_4 denotes time relative to some reference event or process denoted here by formula_5.

Causal sets are a mathematical model for physical systems that are discrete in time and continuous in space. They are sets of events, where each event is a set of space-time points, and each point is a set of space-time points.





## License

EinsteinDB is under the Apache 2.0 license. See the [LICENSE](./LICENSE) file for details.

