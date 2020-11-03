package com.gpsservice;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.gpsservice.dto.GPSOutputDTO;
import com.gpsservice.mappers.TypeColorMapper;
import com.gpsservice.dto.GPSInputDTO;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class Enricher {

    public static final String INPUT_TOPIC = "streams-gpsdata-unenriched";
    public static final String OUTPUT_TOPIC = "streams-gpsdata-enriched";

    private static Properties getStreamsConfig() {
        final Properties props = new Properties();

        props.putIfAbsent(StreamsConfig.APPLICATION_ID_CONFIG, "gps-data-enricher-application");
        props.putIfAbsent(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.putIfAbsent(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        props.putIfAbsent(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.putIfAbsent(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.putIfAbsent(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        return props;
    }

    static void enrichGPSDataStream(final StreamsBuilder builder) {
        Gson gson = new GsonBuilder()
                .setDateFormat("yyyy-MM-dd'T'hh:mm:ss.S'Z'")
                .create();
        TypeColorMapper typeColorMapper = new TypeColorMapper();

        KStream<String, String> resourceStream = builder.stream(INPUT_TOPIC);

        resourceStream.mapValues(value -> {
            GPSInputDTO inputJson = gson.fromJson(value, GPSInputDTO.class);
            String color = typeColorMapper.getColorFromType(inputJson.getType());
            GPSOutputDTO outputJson = new GPSOutputDTO(inputJson.unit, inputJson.type, inputJson.coordinates, color);
            return outputJson.toString();
        }).to(OUTPUT_TOPIC);
    }

    public static void main(final String[] args) {
        final Properties props = getStreamsConfig();

        final StreamsBuilder builder = new StreamsBuilder();

        enrichGPSDataStream(builder);

        final Topology topology = builder.build();

        final KafkaStreams streams = new KafkaStreams(topology, props);

        final CountDownLatch latch = new CountDownLatch(1);

        // attach shutdown handler to catch control-c
        Runtime.getRuntime().addShutdownHook(new Thread("streams-gpsdata-shutdown-hook") {
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
            System.exit(1);
        }
        System.exit(0);
    }
}
