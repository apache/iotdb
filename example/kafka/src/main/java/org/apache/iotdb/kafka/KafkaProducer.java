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

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/** The class is to show how to send data to kafka through multi-threads. */
public class KafkaProducer {

  private final Producer<String, String> producer;
  private static final Logger logger = LoggerFactory.getLogger(KafkaProducer.class);

  public KafkaProducer() {

    Properties props = new Properties();
    props.put("metadata.broker.list", "127.0.0.1:9092");
    props.put("zk.connect", "127.0.0.1:2181");
    props.put("serializer.class", "kafka.serializer.StringEncoder");
    props.put("key.serializer.class", "kafka.serializer.StringEncoder");
    props.put("request.required.acks", "-1");

    producer = new Producer<>(new ProducerConfig(props));
  }

  public static void main(String[] args) {
    KafkaProducer kafkaProducer = new KafkaProducer();
    kafkaProducer.produce();
    kafkaProducer.close();
  }

  private void produce() {
    for (int i = 0; i < Constant.ALL_DATA.length; i++) {
      String key = Integer.toString(i);
      producer.send(new KeyedMessage<>(Constant.TOPIC, key, Constant.ALL_DATA[i]));
      logger.info(Constant.ALL_DATA[i]);
    }
  }

  public void close() {
    producer.close();
  }
}
