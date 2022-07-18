/*
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
package org.apache.iotdb.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/** The class is to show how to send data to kafka through multi-threads. */
public class Producer {

  private final KafkaProducer<String, String> producer;
  private static final Logger logger = LoggerFactory.getLogger(Producer.class);

  public Producer() {

    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Constant.KAFKA_SERVICE_URL);
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    producer = new KafkaProducer<>(props);
  }

  public static void main(String[] args) {
    Producer producer = new Producer();
    producer.produce();
    producer.close();
  }

  private void produce() {
    for (int i = 0; i < Constant.ALL_DATA.length; i++) {
      String key = Integer.toString(i);
      producer.send(new ProducerRecord<>(Constant.TOPIC, key, Constant.ALL_DATA[i]));
      logger.info(Constant.ALL_DATA[i]);
    }
  }

  public void close() {
    producer.close();
  }
}
