package com.linuxacademy.ccdak.streams;

import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.ForeachAction;
import org.apache.kafka.streams.kstream.KStream;

public class StatelessTransformationsMain {

    public static void main(String[] args) {
        // Set up the configuration.
        final Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "stateless-transformations-example");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        // Since the input topic uses Strings for both key and value, set the default Serdes to String.
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        // Get the source stream.
        final StreamsBuilder builder = new StreamsBuilder();
        
        //Implement streams logic Begin.

        KStream<String, String> source = builder.stream("stateless-xform-input-topic");
        KStream<String, String>[] branch = source.branch(((key, value) -> key.startsWith("a")), ((key, value) -> true));
        KStream<String, String> aKeyStream = branch[0];
        KStream<String, String> otherStream = branch[1];
        //Filter aKeyStream with key beging with letter a
        aKeyStream = aKeyStream.filter((key, value) -> key.startsWith("a"));

        //Convert stream
        aKeyStream = aKeyStream.flatMap((key, value) -> {
         List<KeyValue<String,String>> result = new LinkedList();
         result.add(KeyValue.pair(key,value.toUpperCase()));
         result.add(KeyValue.pair(key,value.toLowerCase()));
         return result;
        });

        //convert each record key to upper case value
        aKeyStream.map((key, value) -> KeyValue.pair(key.toUpperCase(),value));

        //Merge two stream back together
        KStream<String, String> mergedStream = aKeyStream.merge(otherStream);

        mergedStream = mergedStream.peek((key, value) -> System.out.println("key= " + key + ",value= " + value));

        mergedStream.to("stateless-xform-output-topic");

        //Implement streams logic End.

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
