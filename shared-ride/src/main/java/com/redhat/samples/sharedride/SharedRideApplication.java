package com.redhat.samples.sharedride;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Properties;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class SharedRideApplication implements CommandLineRunner {

	private static final Logger log = LoggerFactory.getLogger(SharedRideApplication.class);

	public static void main(String[] args) {
		SpringApplication.run(SharedRideApplication.class, args);
	}

	@Override
	public void run(String... args) throws Exception {
		Properties streamsConfiguration = new Properties();
		streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "transport-matcher");
		streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
				"hack-cluster-kafka-bootstrap.kafka.svc:9092");
		streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Long().getClass().getName());
		streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Long().getClass().getName());

		// Serializers/deserializers (serde) for String and Long types
		final Serde<String> stringSerde = Serdes.String();
		final Serde<Long> longSerde = Serdes.Long();
		final Serde<Pickup> pickupSerde = new Pickup();

		final StreamsBuilder builder = new StreamsBuilder();

		// Construct a `KStream` from the input topic
		KStream<String, String> pickups = builder.stream("user1-pickups", Consumed.with(stringSerde, stringSerde));
		KStream<Long, Pickup> pickupTimes = pickups
			.map((key, value) -> {
				Pickup pickup = Pickup.parse(value);
				return KeyValue.pair(pickup.getArrivaltime().getTime(), pickup);
			});
		KTable<Windowed<Long>, Long> grouped = pickupTimes.groupByKey(Grouped.with(longSerde, pickupSerde))
				.windowedBy(TimeWindows.of(Duration.ofMinutes(1)))
				.aggregate(
					() -> 0L, 
					(aggKey, newValue, aggValue) -> aggValue + newValue.getTraveler());
		
		grouped.toStream().peek( 
			(key, value) -> log.info("Pickup Time: " + SimpleDateFormat.getTimeInstance().format(new Timestamp(key.key())) + " Travelers: " + value));
			
		final Topology topology = builder.build();

		System.out.println(topology.describe());

		KafkaStreams streams = new KafkaStreams(topology, streamsConfiguration);
		
		Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
				streams.close();
            }
		});
		
		streams.start();		
	}
}
