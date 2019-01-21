/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
/**
 * The class is to show how to send data to kafka
 */
package org.apache.iotdb.jdbc.kafka_iotdbDemo;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.Properties;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class KafkaProducer {

  public final static String TOPIC = "test";
  private final static String fileName = "kafka_data.csv";
  private final Producer<String, String> producer;

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

  public static void main(String[] args) {
    new KafkaProducer().produce();
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
}
