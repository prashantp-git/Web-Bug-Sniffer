package edu.sjsu.cs185C;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class LogProducerKafka {
	public static void main(String[] args) throws InterruptedException, IOException {
		// check command-line args
		if (args.length < 3) {
			System.err.println("usage: LogProducer <input-file> <output-topic> <sleep-time>");
			System.exit(1);
		}

		// Set the input file and topic to publish to.
		String inputFile = args[0];
		String outputTopic = args[1];
		long sleepTime = Long.parseLong(args[2]);

		// configure the producer
		Properties props = new Properties();
		props.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("bootstrap.servers", "localhost:9092");

		KafkaProducer<Integer, String> producer = new KafkaProducer<Integer, String>(props);

		BufferedReader reader = null;
		int key = 0;
		try {
			reader = new BufferedReader(new FileReader(new File(inputFile)));
			String record;
			while ((record = reader.readLine()) != null) {
				ProducerRecord<Integer, String> prodRecord = new ProducerRecord<Integer, String>(outputTopic, ++key,
						record);
				producer.send(prodRecord);
				System.out.println("Sent " + key + ":" + record);
				// sleep a short interval to throttle the IO.
				Thread.sleep(sleepTime);
			}
		} catch (IOException io) {
			io.printStackTrace();
		}
		reader.close();
		producer.close();
	}
}
