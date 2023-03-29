import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class DeDupFilter
{
       protected final static String storeName = "DeDupStore";

        public static void main(final String[] args) throws InterruptedException {
            final String bootstrapServers = "localhost:9092";

            final KafkaStreams streams = buildStream( bootstrapServers );

            streams.cleanUp();
            streams.start();

            CountDownLatch doneSignal = new CountDownLatch(1);

            Runtime.getRuntime().addShutdownHook(new Thread(()-> {
                doneSignal.countDown();
                streams.close();
            }));

            doneSignal.await();

        }

        static KafkaStreams buildStream(final String bootstrapServers)
        {
            final Properties streamsConfiguration = new Properties();

            streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "dedup.filter");
            streamsConfiguration.put(StreamsConfig.CLIENT_ID_CONFIG, "dedup.filter.client");
            streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

            streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
            streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
            streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

            streamsConfiguration.put("processing.guarantee",  "exactly_once");

            streamsConfiguration.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);

            final StreamsBuilder builder = new StreamsBuilder();

            // State store for tracking duplicates

            final StoreBuilder<KeyValueStore<String, String>> stateStore = Stores.keyValueStoreBuilder(
                    Stores.lruMap(storeName, 10000),
                    Serdes.String(),
                    Serdes.String());

            builder.addStateStore(stateStore);

            // read the source stream
            final KStream<String, String> inputStream = builder.stream("test-topic",
                    Consumed.with(Serdes.String(), Serdes.String()).withOffsetResetPolicy(Topology.AutoOffsetReset.LATEST).withName("mq.source"));

            inputStream.foreach((k,v) -> System.out.println("Before Key :"+k+" Before Value :"+v));
            final KStream<String, String> dedupStream = inputStream.transformValues(() -> new DeDupTransformer(storeName), Named.as("check.duplicates"), storeName)
                    .filter((k,v) -> (null!= v), Named.as("filter.duplicates"));

            dedupStream.foreach((k,v) -> System.out.println("After Key :"+k+" After Value :"+v));

            final Topology tp = builder.build();

            return new KafkaStreams(tp, streamsConfiguration);
        }
}
