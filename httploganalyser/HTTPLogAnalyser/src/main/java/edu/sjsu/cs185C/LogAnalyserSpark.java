package edu.sjsu.cs185C;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function3;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.State;
import org.apache.spark.streaming.StateSpec;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaMapWithStateDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka09.ConsumerStrategies;
import org.apache.spark.streaming.kafka09.KafkaUtils;
import org.apache.spark.streaming.kafka09.LocationStrategies;

import scala.Serializable;
import scala.Tuple2;

public class LogAnalyserSpark {

	//
	//private static boolean isInitialRun = true;

	/*
	 * private static void setInitialRunFalse(JavaStreamingContext jssc) {
	 * List<Tuple2<Integer, Integer>> a = new ArrayList<>(); a.add(new Tuple2<>(0,
	 * 0)); JavaPairRDD<Integer, Integer> toggleIsInitialRun =
	 * jssc.sparkContext().parallelizePairs(a); toggleIsInitialRun.foreach(rdd -> {
	 * System.out.println("---------------  " + isInitialRun); isInitialRun = false;
	 * System.out.println("---------------  " + isInitialRun); }); }
	 */

	// Cumulative count function for Integer Key
	private static Function3<Integer, Optional<Integer>, State<Integer>, Tuple2<Integer, Integer>> mapIntKeyWithState = (
			intKey, one, state) -> {
		//int sum = one.orElse(0) + (state.exists() && !isInitialRun ? state.get() : 0);
		int sum = one.orElse(0) + (state.exists() ? state.get() : 0);
		//System.out.println("InitRun: " + isInitialRun + " , sum: " + sum + " , one: " + one.orElse(0));
		state.update(sum);
		return new Tuple2<>(intKey, sum);
	};

	// Cumulative count function for String Key
	private static Function3<String, Optional<Integer>, State<Integer>, Tuple2<String, Integer>> mapStringKeyWithState = (
			stringKey, one, state) -> {
		//int sum = one.orElse(0) + (state.exists() && !isInitialRun ? state.get() : 0);
		int sum = one.orElse(0) + (state.exists() ? state.get() : 0);
		state.update(sum);
		return new Tuple2<>(stringKey, sum);
	};

	private static JavaPairRDD<Integer, Integer> getInitialIntKeyRDD(JavaStreamingContext jssc) {
		return jssc.sparkContext().parallelizePairs(new ArrayList<Tuple2<Integer, Integer>>());
	}

	private static JavaPairRDD<String, Integer> getInitialStringKeyRDD(JavaStreamingContext jssc) {
		return jssc.sparkContext().parallelizePairs(new ArrayList<Tuple2<String, Integer>>());
	}

	public static void main(String[] args) {
		if (args.length < 4) {
			System.err.println("Usage: LogAnalyser <in-topic> <out-topic> <cg> <interval>");
			System.exit(1);
		}

		// set variables from command-line arguments
		String inTopic = args[0];
		String outTopic = args[1];
		// String consumerGroup = args[2];
		int interval = Integer.parseInt(args[3]);

		// define topic to subscribe to
		Pattern inTopicPattern = Pattern.compile(inTopic, Pattern.CASE_INSENSITIVE);

		// set Kafka client parameters
		Map<String, Object> kafkaParams = new HashMap<String, Object>();
		kafkaParams.put("key.deserializer", "org.apache.kafka.common.serialization.IntegerDeserializer");
		kafkaParams.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		kafkaParams.put("bootstrap.servers", "localhost:9092");
		// kafkaParams.put("group.id", consumerGroup);
		kafkaParams.put("auto.offset.reset", "earliest");

		// initialize the streaming context
		JavaStreamingContext jssc = new JavaStreamingContext("local[2]", "LogAnalyserSpark", new Duration(interval));
		jssc.sparkContext().getConf().set("spark.streaming.stopGracefullyOnShutdown", "true");
		jssc.checkpoint("./checkpoint");

		// configure Kafka producer props
		Properties producerProps = new Properties();
		producerProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		producerProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		producerProps.put("bootstrap.servers", "localhost:9092");

		// pull ConsumerRecords out of the stream
		JavaInputDStream<ConsumerRecord<Integer, String>> rawLogInputDStream = KafkaUtils.createDirectStream(jssc,
				LocationStrategies.PreferConsistent(),
				ConsumerStrategies.<Integer, String>SubscribePattern(inTopicPattern, kafkaParams));

		// generate validlogDStream
		JavaDStream<Log> validlogDStream = rawLogInputDStream.map(Log.parseRawLog).filter(Log.checkValidLog).cache();

		// calculate responseCodesAll, responseCodes4xx and responseCodes5xx with their
		// counts and send
		JavaPairDStream<Integer, Integer> responseCodeCountPairs = validlogDStream.mapToPair(Log.mapResponseCode)
				.reduceByKey(Log.reduceBySum);

		JavaMapWithStateDStream<Integer, Integer, Integer, Tuple2<Integer, Integer>> responseCodeCountDstream = responseCodeCountPairs
				.mapWithState(StateSpec.function(mapIntKeyWithState)// .timeout(Durations.seconds(300))
						.initialState(getInitialIntKeyRDD(jssc)));
		responseCodeCountDstream.foreachRDD(rdd -> {
			if (rdd.count() > 0) {
				KafkaProducer<String, String> producer = new KafkaProducer<String, String>(producerProps);
				ProducerRecord<String, String> resCodesAll = new ProducerRecord<String, String>(outTopic, "ResCodesAll",
						rdd.collect().toString());
				ProducerRecord<String, String> resCodes4xx = new ProducerRecord<String, String>(outTopic, "ResCodes4xx",
						rdd.filter(Log.responseCode4xx).collect().toString());
				ProducerRecord<String, String> resCodes5xx = new ProducerRecord<String, String>(outTopic, "ResCodes5xx",
						rdd.filter(Log.responseCode5xx).collect().toString());
				producer.send(resCodesAll);
				producer.send(resCodes4xx);
				producer.send(resCodes5xx);
				producer.close();
				//isInitialRun = false;
			}
		});

		// calculate top 10 ipAddresses with their counts and send
		JavaPairDStream<String, Integer> ipAddressCountPairs = validlogDStream.mapToPair(Log.mapIpAddress)
				.reduceByKey(Log.reduceBySum);

		JavaMapWithStateDStream<String, Integer, Integer, Tuple2<String, Integer>> ipAddressCountDstream = ipAddressCountPairs
				.mapWithState(StateSpec.function(mapStringKeyWithState)// .timeout(Durations.seconds(300))
						.initialState(getInitialStringKeyRDD(jssc)));
		ipAddressCountDstream.foreachRDD(rdd -> {
			if (rdd.count() > 0) {
				List<Tuple2<String, Integer>> topIpAddresses = rdd.takeOrdered(10,
						(Comparator<Tuple2<String, Integer>> & Serializable) (o1, o2) -> {
							return o2._2().compareTo(o1._2());
						});
				KafkaProducer<String, String> producer = new KafkaProducer<String, String>(producerProps);
				ProducerRecord<String, String> prodRec = new ProducerRecord<String, String>(outTopic, "IpAddress",
						topIpAddresses.toString());
				producer.send(prodRec);
				producer.close();
			}
		});

		// calculate top 10 resources with their counts and send
		JavaPairDStream<String, Integer> resourceCountPairs = validlogDStream.mapToPair(Log.mapResource)
				.reduceByKey(Log.reduceBySum);

		JavaMapWithStateDStream<String, Integer, Integer, Tuple2<String, Integer>> resourceCountDstream = resourceCountPairs
				.mapWithState(StateSpec.function(mapStringKeyWithState)// .timeout(Durations.seconds(300))
						.initialState(getInitialStringKeyRDD(jssc)));
		resourceCountDstream.foreachRDD(rdd -> {
			if (rdd.count() > 0) {
				List<Tuple2<String, Integer>> topResources = rdd.takeOrdered(10,
						(Comparator<Tuple2<String, Integer>> & Serializable) (o1, o2) -> {
							return o2._2().compareTo(o1._2());
						});
				KafkaProducer<String, String> producer = new KafkaProducer<String, String>(producerProps);
				ProducerRecord<String, String> prodRec = new ProducerRecord<String, String>(outTopic, "Resources",
						topResources.toString());
				producer.send(prodRec);
				producer.close();
			}
		});

		// calculate top 10 resources with errors counts and send
		JavaPairDStream<String, Integer> resourceErrorCountPairs = validlogDStream.mapToPair(Log.mapResourceError)
				.filter(Log.checkValidTuple).reduceByKey(Log.reduceBySum);

		JavaMapWithStateDStream<String, Integer, Integer, Tuple2<String, Integer>> resourceErrorCountDstream = resourceErrorCountPairs
				.mapWithState(StateSpec.function(mapStringKeyWithState)// .timeout(Durations.seconds(300))
						.initialState(getInitialStringKeyRDD(jssc)));
		resourceErrorCountDstream.foreachRDD(rdd -> {
			if (rdd.count() > 0) {
				List<Tuple2<String, Integer>> topResourcesWithErr = rdd.takeOrdered(10,
						(Comparator<Tuple2<String, Integer>> & Serializable) (o1, o2) -> {
							return o2._2().compareTo(o1._2());
						});
				KafkaProducer<String, String> producer = new KafkaProducer<String, String>(producerProps);
				ProducerRecord<String, String> prodRec = new ProducerRecord<String, String>(outTopic, "ResourcesErrors",
						topResourcesWithErr.toString());
				producer.send(prodRec);
				producer.close();
			}
		});

		// setInitialRunFalse(jssc);
		// jssc
		jssc.start();

		// stay in infinite loop until terminated
		try {
			jssc.awaitTermination();
		} catch (InterruptedException e) {
			System.out.println("LogAnalyserSpark is interrupted.");
		}
	}

}
