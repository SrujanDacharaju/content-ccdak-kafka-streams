package com.linuxacademy.ccdak.streams;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;

public class AggregationsMain {

    public static void main(String[] args) {
        // Set up the configuration.
        final Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "aggregations-example");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        // Since the input topic uses Strings for both key and value, set the default Serdes to String.
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        // Get the source stream.
        final StreamsBuilder builder = new StreamsBuilder();
        KStream<String,String> source = builder.stream("aggregations-input-topic");

        final KGroupedStream<String, String> groupedStream = source.groupByKey();

        final KTable<String, Integer> aggregatedTable = groupedStream.aggregate(
                () -> 0,
                (key, value, aggregate) -> aggregate + value.length(),
                Materialized.with(Serdes.String(), Serdes.Integer()));
        aggregatedTable.toStream().to("agg-countchars-output-topic", Produced.with(Serdes.String(),Serdes.Integer()));

        final KTable<String, Long> countedTable = groupedStream.count(Materialized.with(Serdes.String(), Serdes.Long()));
        countedTable.toStream().to("agg-counted-output-topic");


        final KTable<String, String> reducedTable = groupedStream.reduce((aggValue, newValue) -> aggValue + " " + newValue);
        reducedTable.toStream().to("agg-reduced-output-topic");


        final Topology topology = builder.build();
        final KafkaStreams streams = new KafkaStreams(topology, props);
        // Print the topology to the console.
        System.out.println(topology.describe());
        final CountDownLatch latch = new CountDownLatch(1);

        // Attach a shutdown handler to catch control-c and terminate the application gracefully.
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });

        try {
            streams.start();
            latch.await();
        } catch (final Throwable e) {
            System.out.println(e.getMessage());
            System.exit(1);
        }
        System.exit(0);
    }

}
