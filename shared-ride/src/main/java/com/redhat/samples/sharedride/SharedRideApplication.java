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
import org.apache.kafka.streams.kstream.KGroupedStream;
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

		// 0. Check the below code example. Is getting the pickuptimes for your speakers. Take as a base for the next tasks.

		// 1. Update the aggreggation function to sum the attendees instead of just adding the amount of records.

		// 2. Copy the code and apply the same process to get the events records comming now from the userX-shareride topic
		//    map the json values to a Vehicle helper class where the space available is the key and 
		//    group the to find the cheapest car

		// 3. Add a new transformation to the key from the pickupTimes table and map that to now get the pax pickup 
		//    as the key and the pickup time as the value. Call this pickupSpace

		// 4. Join the pickupSpace stream with the cheapest ride so you can match the prefered one.

		// 6. Write the results back to AMQ Streams under the "userX-results" (use the .to() method) write it as a 
		//    json string so you can take it back the google sheet. 

		// 7. Enjoy!

		/* ***************** */

		// pickups KStream is receiving data for the "user1-pickups topic"
		KStream<String, String> pickups = builder.stream("user1-pickups", Consumed.with(stringSerde, stringSerde));
		// raw data comes in the <String, String> format as it's a null Kafka key and a
		// JSON String so we need to map the stream to transform the events into <Long, Pickup> 
		KStream<Long, Pickup> mappedPickups = pickups.map((key, value) -> {
			// Pickup pojo has a parse method to serialize and deserialize from json
			Pickup pickup = Pickup.parse(value);
			// for easy processing the new key is the milliseconds (Long) representing the pickup date
		return KeyValue.pair(pickup.getArrivaltime().getTime(), pickup);
		});
		// with our data ready to process, we will group records by key (pickup time expresed in milliseconds)
		// as we are changing the data type of the key and value we need to explicit set the ser/des types
		KGroupedStream<Long, Pickup> groupedPickups = mappedPickups.groupByKey(Grouped.with(longSerde, pickupSerde));
		// with the grouped records we can then execute an aggregation transformation and get a table 
		// with the aggregated results
		KTable<Long, Integer> pickupTimes = groupedPickups.aggregate(
			// we need a counter initialized in 0
			() -> 0,
			// with each record we increment the counter
			(aggKey, newValue, aggValue) -> aggValue + 1
		);
		// if we transform the table again to a stream, we can `peek` into the new stream to check the new records
		pickupTimes.toStream().peek( 
			(key, value) -> log.info("Pickup Time: " + value + " Travelers: " + key));

		final Topology topology = builder.build();

		System.out.println(topology.describe());

		KafkaStreams streams = new KafkaStreams(topology, streamsConfiguration);
		
		streams.cleanUp();
		streams.start();	

		Runtime.getRuntime().addShutdownHook(new Thread(streams::close));		
	}
}
