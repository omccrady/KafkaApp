package kafkaapp;


import java.io.FileInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

/**
 * Simple app to demo Kafka publish and subscribe
 * 
 * Supporting Kafka commands 
 * bin/zookeeper-server-start.sh
 * config/zookeeper.properties 
 * bin/kafka-server-start.sh config/server.properties 
 * bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic test
 * bin/kafka-topics.sh --list --zookeeper localhost:2181
 * bin/kafka-console-producer.sh --broker-list localhost:9092 --topic test
 * bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic test --from-beginning
 * 
 * @author omccrady
 * 2018-07-16
 *
 */

public class KafkaApp {

	final static Logger logger = Logger.getLogger(KafkaApp.class);

	public static void main(String[] args) {
		if (args.length < 2) {
			System.out.println("Error. Usage: > KafkaApp <config file> <producer|consumer|both>");
			return;
		}
		String configFileName = args[0];
		String mode = args[1];

		Properties props = new Properties();
		try {
			props.load(new FileInputStream(configFileName));
		} catch (IOException e) {
			e.printStackTrace();
		}
		PropertyConfigurator.configure(props);
		logger.info("Logger initialised.");
		logger.info("App mode: " + mode);
		int numMessages = 0;
		if (mode.equals("publish")) {
			logger.info("Publishing messages");
			numMessages = publishKafka(props);
		} else if (mode.equals("consume")) {
			logger.info("Consuming 100 messages");
			consumeKafka(props, 100);
		} else if (mode.equals("both")) {
			logger.info("Publishing and Consuming messages");
			numMessages = publishKafka(props);
			try {
				Thread.sleep(5000);
			} catch (InterruptedException e) {
				logger.error("Thread sleep interrupted: " + e.getMessage());
			}
			consumeKafka(props, numMessages);
		}
		logger.info("Program finished!");
	}

	/*
	 * Publish messages
	 */
	static public int publishKafka(Properties properties) {
		logger.info("Running publishKafka()");
		Producer<String, String> producer = new KafkaProducer<String, String>(properties);
		String topicName = properties.getProperty("kafkaapp.topic.publish");
		logger.info("Publishing to topic " + topicName);
		int numMessages = 0;
		for (int i = 0; i < 10; i++) {
			logger.info("Sending message " + i);
			producer.send(new ProducerRecord<String, String>(topicName, Integer.toString(i), Integer.toString(i)));
			numMessages++;
		}
		producer.close();
		System.out.println("Messages sent successfully.");
		logger.info("Published numMessages = " + numMessages);
		return numMessages;
	}

	/*
	 * Consume messages
	 */
	static public void consumeKafka(Properties properties, int numMessages) {
		logger.info("Running consumeKafka()");
		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
		String topicName = properties.getProperty("kafkaapp.topic.subscribe");
		consumer.subscribe(Arrays.asList(topicName));
		logger.info("Subscribed to topic " + topicName);
		logger.info("Consuming numMessages = " + numMessages);
		boolean continueConsuming = true;
		int readMessages = 0;
		while (continueConsuming) {
			ConsumerRecords<String, String> records = consumer.poll(100);
			for (ConsumerRecord<String, String> record : records) {
				String kafkaMessage = "Record: Offset = " + record.offset() + ", Key = " + record.key() + ", value = "
						+ record.value();
				System.out.println(kafkaMessage);
				logger.debug(kafkaMessage);
				readMessages++;
				if (readMessages >= numMessages) {
					continueConsuming = false;
				}
			}
		}
		consumer.close();
		System.out.println("Messages consumed successfully.");
		logger.info("Consumed numMessages = " + readMessages);
	}
	
	/*
	 * Sandbox
	 */
	static public int sandbox() {
		return 0;
	}
}
