package com.redhat.samples.sharedride;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.Map;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

public class Pickup implements Serializer<Pickup>, Deserializer<Pickup>, Serde<Pickup> {

	static Logger log = LoggerFactory.getLogger(Pickup.class);

	static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
		.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

	Timestamp arrivaltime;
	String speakerName;
	String status;
	Integer traveler;

	public Pickup() {}

	public Pickup(Timestamp arrival, String name, Integer travelers) {
		this.arrivaltime = arrival;
		this.speakerName = name;
		this.traveler = travelers;
	}

	@Override
	public String toString() {
		try {
			return OBJECT_MAPPER.writeValueAsString(this);
		} catch (Exception e) {
			log.error("could not serialize", e);
		}
		return super.toString();
	}

	public static Pickup parse(String json) {
		try {
			return OBJECT_MAPPER.readValue(json, Pickup.class);
		} catch (Exception e) {
			log.error("could not parse", e);
		}
		return new Pickup(new Timestamp(0), "No Speaker", 0);
	}

	public String getSpeakerName() {
		return speakerName;
	}

	public void setSpeakerName(String speakerName) {
		this.speakerName = speakerName;
	}

	public String getStatus() {
		return status;
	}

	public void setStatus(String status) {
		this.status = status;
	}

	public Integer getTraveler() {
		return traveler;
	}

	public void setTraveler(Integer traveler) {
		this.traveler = traveler;
	}

	@JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "HH:mm:ss")
	public Timestamp getArrivaltime() {
		return arrivaltime;
	}

	public void setArrivaltime(Timestamp arrivaltime) {
		this.arrivaltime = arrivaltime;
	}

	@Override
	public Pickup deserialize(String topic, byte[] data) {
		if (data == null) {
			return null;
		}
		try {
			return OBJECT_MAPPER.readValue(data, Pickup.class);
		} catch (final IOException e) {
			throw new SerializationException(e);
		}
	}

	@Override
	public void configure(Map configs, boolean isKey) {}

	@Override
	public byte[] serialize(String topic, Pickup data) {
		if (data == null) {
			return null;
		}
		try {
			return OBJECT_MAPPER.writeValueAsBytes(data);
		} catch (final Exception e) {
			throw new SerializationException("Error serializing JSON message", e);
		}
	}

	@Override
	public void close() {}

	@Override
	public Serializer<Pickup> serializer() {
		return this;
	}

	@Override
	public Deserializer<Pickup> deserializer() {
		return this;
	}

}
