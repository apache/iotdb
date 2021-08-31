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

import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.serializer.StringDecoder;
import kafka.utils.VerifiableProperties;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * The class is to show how to get data from kafka through multi-threads. The data is sent by class
 * KafkaProducer.
 */
public class KafkaConsumer {

  private ConsumerConnector consumer;

  private KafkaConsumer() {
    /** Consumer configuration */
    Properties props = new Properties();

    /** Zookeeper configuration */
    props.put("zookeeper.connect", "127.0.0.1:2181");
    props.put("group.id", "consumeGroup");
    props.put("zookeeper.session.timeout.ms", "400");
    props.put("zookeeper.sync.time.ms", "200");
    props.put("rebalance.max.retries", "5");
    props.put("rebalance.backoff.ms", "1200");
    props.put("auto.commit.interval.ms", "1000");

    /**
     * What to do when there is no initial offset in ZooKeeper or if an offset is out of range
     * smallest : automatically reset the offset to the smallest offset
     */
    props.put("auto.offset.reset", "smallest");

    /** serializer class */
    props.put("serializer.class", "kafka.serializer.StringEncoder");

    ConsumerConfig config = new ConsumerConfig(props);
    consumer = kafka.consumer.Consumer.createJavaConsumerConnector(config);
  }

  public static void main(String[] args) {
    new KafkaConsumer().consume();
  }

  private void consume() {
    /** Specify the number of consumer thread */
    Map<String, Integer> topicCountMap = new HashMap<>();
    topicCountMap.put(Constant.TOPIC, Constant.CONSUMER_THREAD_NUM);

    /** Specify data decoder */
    StringDecoder keyDecoder = new StringDecoder(new VerifiableProperties());
    StringDecoder valueDecoder = new StringDecoder(new VerifiableProperties());

    Map<String, List<KafkaStream<String, String>>> consumerMap =
        consumer.createMessageStreams(topicCountMap, keyDecoder, valueDecoder);

    List<KafkaStream<String, String>> streams = consumerMap.get(Constant.TOPIC);
    ExecutorService executor = Executors.newFixedThreadPool(Constant.CONSUMER_THREAD_NUM);
    for (final KafkaStream<String, String> stream : streams) {
      executor.submit(new KafkaConsumerThread(stream));
    }
  }
}
