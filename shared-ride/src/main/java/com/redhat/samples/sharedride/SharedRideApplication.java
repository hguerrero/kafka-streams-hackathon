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
import org.apache.kafka.streams.kstream.Joined;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.kstream.Windowed;
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

		/* **** Replace 'x' with your user number **** */
		streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "userX-transport-matcher");

		streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
				"hack-cluster-kafka-bootstrap.kafka.svc:9092");
		streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Integer().getClass().getName());
		streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Integer().getClass().getName());

		// Serializers/deserializers (serde) for String and Long types
		final Serde<String> stringSerde = Serdes.String();
		final Serde<Long> longSerde = Serdes.Long();
		final Serde<Integer> integerSerde = Serdes.Integer();
		final Serde<Pickup> pickupSerde = new Pickup();
		final Serde<Transport> transportSerde = new Transport();

		final StreamsBuilder builder = new StreamsBuilder();

		// 1. Check how many passangers to pickup by time 
		KStream<String, String> pickups = builder.stream("user1-pickups", Consumed.with(stringSerde, stringSerde));
		KStream<Integer, String> pickupSpace = ...
			
		// 2. Check the cheapest ride by numer of available spaces
		KStream<String, String> rides = builder.stream("user1-uber", Consumed.with(stringSerde, stringSerde));
		KTable<Integer, Transport> cheapest = ...

		// 3. Join the datasets to match time pickups with cheaper ride
		KStream<Integer, String> matches = ...

		// Send the results back to paste in the datasheet
		matches.to("user1-results", Produced.with(integerSerde, stringSerde));

		final Topology topology = builder.build();

		System.out.println(topology.describe());

		KafkaStreams streams = new KafkaStreams(topology, streamsConfiguration);
		
		streams.cleanUp();
		streams.start();	

		Runtime.getRuntime().addShutdownHook(new Thread(streams::close));		
	}
}
