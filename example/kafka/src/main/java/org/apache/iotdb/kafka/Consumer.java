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

import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.pool.SessionPool;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * The class is to show how to get data from kafka through multi-threads. The data is sent by class
 * Consumer.
 */
public class Consumer {

  private List<KafkaConsumer<String, String>> consumerList;
  private static final Logger logger = LoggerFactory.getLogger(Consumer.class);
  private static SessionPool pool;

  private Consumer(List<KafkaConsumer<String, String>> consumerList) {
    this.consumerList = consumerList;
    pool =
        new SessionPool.Builder()
            .host(Constant.IOTDB_CONNECTION_HOST)
            .port(Constant.IOTDB_CONNECTION_PORT)
            .user(Constant.IOTDB_CONNECTION_USER)
            .password(Constant.IOTDB_CONNECTION_PASSWORD)
            .maxSize(Constant.SESSION_SIZE)
            .build();
  }

  public static void main(String[] args) {
    List<KafkaConsumer<String, String>> consumerList = new ArrayList<>();
    for (int i = 0; i < Constant.CONSUMER_THREAD_NUM; i++) {

      /** Consumer configuration */
      Properties props = new Properties();

      props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Constant.KAFKA_SERVICE_URL);
      /** serializer class */
      props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
      props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
      /**
       * What to do when there is no initial offset in ZooKeeper or if an offset is out of range
       * earliest: automatically reset the offset to the earliest offset latest:automatically reset
       * the offset to the latest offset
       */
      props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
      /** There is only one consumer in a group */
      props.put(ConsumerConfig.GROUP_ID_CONFIG, Constant.TOPIC);

      KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
      consumerList.add(consumer);
      consumer.subscribe(Collections.singleton(Constant.TOPIC));
    }
    Consumer consumer = new Consumer(consumerList);
    initIoTDB();
    consumer.consumeInParallel();
  }

  @SuppressWarnings("squid:S2068")
  private static void initIoTDB() {

    try {
      for (String storageGroup : Constant.STORAGE_GROUP) {
        addStorageGroup(storageGroup);
      }
      for (String[] sql : Constant.CREATE_TIMESERIES) {
        createTimeseries(sql);
      }
    } catch (IoTDBConnectionException | StatementExecutionException e) {
      logger.error(e.getMessage());
    }
  }

  private static void addStorageGroup(String storageGroup)
      throws IoTDBConnectionException, StatementExecutionException {
    pool.setStorageGroup(storageGroup);
  }

  private static void createTimeseries(String[] sql)
      throws StatementExecutionException, IoTDBConnectionException {
    String timeseries = sql[0];
    TSDataType dataType = TSDataType.valueOf(sql[1]);
    TSEncoding encoding = TSEncoding.valueOf(sql[2]);
    CompressionType compressionType = CompressionType.valueOf(sql[3]);
    pool.createTimeseries(timeseries, dataType, encoding, compressionType);
  }

  private void consumeInParallel() {
    /** Specify the number of consumer thread */
    ExecutorService executor = Executors.newFixedThreadPool(Constant.CONSUMER_THREAD_NUM);
    for (int i = 0; i < consumerList.size(); i++) {
      ConsumerThread consumerThread = new ConsumerThread(consumerList.get(i), pool);
      executor.submit(consumerThread);
    }
  }
}
