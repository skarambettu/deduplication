# deduplication
The github has 2 java classes that performs the Dedup check using the key of the message. The code uses the internal RocksDB for storing the keys.

Below line of code to the create a state store for storing keys and it uses LRU Map as the persistant store.

final StoreBuilder<KeyValueStore<String, String>> stateStore = Stores.keyValueStoreBuilder(
                    Stores.lruMap(storeName, 10000),
                    Serdes.String(),
                    Serdes.String());


Here are the maven dependencies used to run the code.

<dependency>
            <groupId>org.apache.kafka</groupId>
            <artifactId>kafka-clients</artifactId>
            <version>3.4.0</version>
        </dependency>
        <dependency>
            <groupId>org.apache.kafka</groupId>
            <artifactId>kafka-streams</artifactId>
            <version>3.4.0</version>
        </dependency>
