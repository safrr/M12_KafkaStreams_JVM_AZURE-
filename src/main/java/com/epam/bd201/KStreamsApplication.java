package com.epam.bd201;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.json.JSONObject;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class KStreamsApplication {
    
    public static void main(String[] args) throws Exception {

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, System.getenv().get("APPLICATION_ID"));
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, System.getenv().get("BOOTSTRAP_SERVERS"));
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        final String INPUT_TOPIC_NAME = System.getenv().get("INPUT_TOPIC_NAME");
        final String OUTPUT_TOPIC_NAME = System.getenv().get("OUTPUT_TOPIC_NAME");

        final StreamsBuilder builder = new StreamsBuilder();

        final KStream<String, String> input_records = builder.stream(INPUT_TOPIC_NAME, Consumed.with(Serdes.String(), Serdes.String()));

        // Transform records
        input_records.map(KStreamsApplication::mapValues).to(OUTPUT_TOPIC_NAME);

        final Topology topology = builder.build();
        System.out.println(topology.describe());

        final KafkaStreams streams = new KafkaStreams(topology, props);
        final CountDownLatch latch = new CountDownLatch(1);

        // Attach shutdown handler to catch control-c
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

    private enum StayCategory {
        ERRONEOUS_DATA("Erroneous data", 0),
        SHORT_STAY("Short stay", 1),
        STANDARD_STAY("Standard stay", 5),
        STANDARD_EXTENDED_STAY("Standard extended stay", 11),
        LONG_STAY("Long stay", 15);

        private static final DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
        private final String description;
        private final long minDaysOfStay;

        StayCategory(String description, long minDaysOfStay) {
            this.description = description;
            this.minDaysOfStay = minDaysOfStay;
        }

        public String getDescription() {
            return description;
        }

        // return type of days stay
        public static StayCategory getCategory(String checkIn, String checkOut) {
            if (checkIn == null || checkOut == null) {
                return ERRONEOUS_DATA;
            }

            LocalDate checkInDate = LocalDate.parse(checkIn, dateTimeFormatter);
            LocalDate checkOutDate = LocalDate.parse(checkOut, dateTimeFormatter);

            long daysOfStay = ChronoUnit.DAYS.between(checkInDate, checkOutDate);

            StayCategory[] stayValues = StayCategory.values();

            for (StayCategory value : stayValues) {
                if (value.minDaysOfStay <= daysOfStay) {
                    return value;
                }
            }

            throw new IllegalStateException("Error parsing dates: " + checkIn + ", " + checkOut);
        }
    }

    public static KeyValue<String, String> mapValues(String key, String value) {
        JSONObject valueObject = new JSONObject(value);
        StayCategory stayCategory = StayCategory.getCategory(valueObject.getString("srch_ci"), valueObject.getString("srch_co"));
        valueObject.put("category", stayCategory.getDescription());
        return KeyValue.pair(key, valueObject.toString());
    }
}
