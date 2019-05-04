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
		streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "user1-transport-matcher");
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

		// Check how many to pickup by time
		KStream<String, String> pickups = builder.stream("user1-pickups", Consumed.with(stringSerde, stringSerde));
		KTable<Windowed<Long>, Integer> pickupTimes = pickups.map(
			(key, value) -> {
				Pickup pickup = Pickup.parse(value);
				return KeyValue.pair(pickup.getArrivaltime().getTime(), pickup);
			}
		).groupByKey(
			Grouped.with(longSerde, pickupSerde)
		).windowedBy(
			TimeWindows.of(Duration.ofMinutes(1))
		).aggregate(
			() -> 0, 
			(aggKey, newValue, aggValue) -> aggValue + newValue.getTraveler()
		);

		KStream<Integer, String> pickupSpace = pickupTimes.toStream()
		.map(
			(key, value) -> KeyValue.pair(value, SimpleDateFormat.getTimeInstance().format(new Timestamp(key.key())))
		);
		pickupSpace.peek( 
			(key, value) -> log.info("Pickup Time: " + value + " Travelers: " + key));
			
		// Check the cheapest ride by numer of available spaces
        KStream<String, String> uber = builder.stream("user1-uber", Consumed.with(stringSerde, stringSerde));
		KTable<Integer, Transport> uberCost = uber.map(
			(key, value) -> {
				Transport transport = Transport.parse(value);
				transport.setCompany("uber");
				return KeyValue.pair(transport.getAvailableSpace(), transport);
			}
		).filter(
		    (key, value) -> value.getAvailable()
		).groupByKey(
			Grouped.with(integerSerde, transportSerde)
		).reduce(
			(aggValue, newValue) -> newValue
		);
		uberCost.toStream().peek( 
			(key, value) -> log.info("Uber => Spaces:" + key + " Cost: " + String.format("$ %3d", value.getCost()) + " USD")
		);
		
		KStream<String, String> lyft = builder.stream("user1-lyft", Consumed.with(stringSerde, stringSerde));
		KTable<Integer, Transport> lyftCost = lyft.map(
			(key, value) -> {
				String[] attr = value.split(",");
				Transport transport = new Transport(
					"lyft",
					Double.valueOf(attr[1]).longValue(),
					Integer.valueOf(attr[2]), 
					Integer.valueOf(attr[3]), 
					Boolean.valueOf(attr[4]));
				return KeyValue.pair(transport.getAvailableSpace(), transport);
			}
		).filter(
		    (key, value) -> value.getAvailable()
		).groupByKey(
			Grouped.with(integerSerde, transportSerde)
		).reduce(
			(aggValue, newValue) -> newValue
		);
		lyftCost.toStream().peek( 
			(key, value) -> log.info("Lyft => Spaces:" + key + " Cost: " + String.format("$ %3d", value.getCost()) + " USD")
		);

	    KTable<Integer, Transport> rides = uberCost.leftJoin(
			lyftCost,
			new ValueJoiner<Transport, Transport, Transport>() {
				@Override
				public Transport apply(Transport leftValue, Transport rightValue) {
				  if (leftValue == null) return new Transport("no transport",0L,0,0,false);
				  if (rightValue == null) return leftValue;
				  return leftValue.getCost() > rightValue.getCost() ? rightValue : leftValue;
				}
			}
		);
		rides.toStream().peek( 
			(key, value) -> log.info("Cheapest Ride => Spaces:" + key + " Company: " + value.getCompany() + " Cost: " + String.format("$ %3d", value.getCost()) + " USD")
		);
 
		KStream<Integer, String> matches = pickupSpace.leftJoin(
			rides,
			new ValueJoiner<String, Transport, String>() {
				@Override
				public String apply(String leftValue, Transport rightValue) {
					return "{ \"pickupTime\": \"" + leftValue + 
						"\", \"company\": \"" + (rightValue == null ? "" : rightValue.getCompany()) + 
						"\", \"cost\": " + (rightValue == null ? "" : rightValue.getCost()) + 
						", \"passengers\": " + (rightValue == null ? "" : rightValue.getAvailableSpace()) + "}";
				}
			},
			Joined.with(integerSerde, stringSerde, transportSerde)
		);

		matches.peek( 
			(key, value) -> log.info("Matches "  + value)
		);

		matches.to("user1-results", Produced.with(integerSerde, stringSerde));

		final Topology topology = builder.build();

		System.out.println(topology.describe());

		KafkaStreams streams = new KafkaStreams(topology, streamsConfiguration);
		
		streams.cleanUp();
		streams.start();	

		Runtime.getRuntime().addShutdownHook(new Thread(streams::close));		
	}
}
