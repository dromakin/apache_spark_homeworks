package com.epam.bd201;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class KStreamsApplication {
    private static ObjectMapper objectMapper = new ObjectMapper();

    private static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd");

    public static void main(String[] args) throws Exception {

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, System.getenv().get("APPLICATION_ID"));
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, System.getenv().get("BOOTSTRAP_SERVERS"));
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put("schema.registry.url", System.getenv().get("SCHEMA_REGISTRY_URL"));

        //If needed
        final String INPUT_TOPIC_NAME = System.getenv().get("INPUT_TOPIC_NAME");
        final String OUTPUT_TOPIC_NAME = System.getenv().get("OUTPUT_TOPIC_NAME");

        final StreamsBuilder builder = new StreamsBuilder();
        final KStream<String, String> input_records = builder.stream(INPUT_TOPIC_NAME, Consumed.with(Serdes.String(), Serdes.String()));

        KStream<String, String> outputStream = input_records.mapValues(value -> {
            try {
                Map<String, String> valueMap = objectMapper.readValue(value, HashMap.class);
                String checkIn = valueMap.getOrDefault("srch_ci", "");
                String checkOut = valueMap.getOrDefault("srch_co", "");

                String category = getCategory(checkIn, checkOut);
                valueMap.put("category", category);

                String result = objectMapper.writeValueAsString(valueMap);
                System.out.println(result);

                return result;
            } catch (JsonProcessingException e) {
                System.out.println("Can't parse record: " + value);
                throw new RuntimeException("Can't parse record: " + value);
            }
        });

        outputStream.to(OUTPUT_TOPIC_NAME);

        final Topology topology = builder.build();
        System.out.println(topology.describe());

        final KafkaStreams streams = new KafkaStreams(topology, props);
        final CountDownLatch latch = new CountDownLatch(1);

        // attach shutdown handler to catch control-c
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
        } catch (Throwable e) {
            System.exit(1);
        }
        System.exit(0);
    }

    /**
     * Calculate days difference between 2 dates - checkIn & checkOut
     *
     * @param checkIn  String
     * @param checkOut String
     * @return long
     */
    private static long calculatePeriod(String checkIn, String checkOut) {
        try {
            LocalDate checkInDate = LocalDate.parse(checkIn, DATE_TIME_FORMATTER);
            LocalDate checkOutDate = LocalDate.parse(checkOut, DATE_TIME_FORMATTER);
            return ChronoUnit.DAYS.between(checkInDate, checkOutDate);
        } catch (Exception e) {
            System.out.println("Can't parse dates, check checkIn:" + checkIn + " & checkOut: " + checkOut);
        }
        return 0;
    }

    /**
     * This function return type of days stay
     *
     * @param checkIn  String
     * @param checkOut String
     * @return String
     */
    private static String getCategory(String checkIn, String checkOut) {
        long stayDays = calculatePeriod(checkIn, checkOut);

        String dayType = "Erroneous data";

        if (stayDays >= 11 && stayDays <= 14) {
            dayType = "Standard stay";
        }

        if (stayDays > 14) {
            dayType = "Standard extended stay";
        }

        if (stayDays >= 1 && stayDays <= 4) {
            dayType = "Short stay";
        }

        if (stayDays >= 5 && stayDays <= 10) {
            dayType = "Short stay";
        }

        return dayType;
    }

}
