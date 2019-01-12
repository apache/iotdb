/**
 * The class is to show how to send data to kafka
 */
package org.apache.iotdb.jdbc.kafka_iotdbDemo;

import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import java.io.BufferedReader; 
import java.io.File;
import java.io.FileReader;

public class KafkaProducer {
	private final Producer<String, String> producer;
	public final static String TOPIC = "test";
	private final static String fileName = "kafka_data.csv";

	private KafkaProducer() {

		Properties props = new Properties();

		props.put("metadata.broker.list", "127.0.0.1:9092");
		props.put("zk.connect", "127.0.0.1:2181");
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		props.put("key.serializer.class", "kafka.serializer.StringEncoder");

		props.put("request.required.acks", "-1");

		//Producer instance
		producer = new Producer<String, String>(new ProducerConfig(props));
	}

	void produce() {
		//read file
		try {
			File csv = new File(fileName); 
			BufferedReader reader = new BufferedReader(new FileReader(csv));
			String line = null;
			int messageNum = 1;
			while ((line = reader.readLine()) != null) {
				String key = String.valueOf(messageNum);
				String data = line;
				producer.send(new KeyedMessage<String, String>(TOPIC, key, data));
				System.out.println(data);
				messageNum++;
			}
			reader.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {
		new KafkaProducer().produce();
	}
}
