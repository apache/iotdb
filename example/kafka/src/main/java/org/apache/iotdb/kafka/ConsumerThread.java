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
import org.apache.iotdb.session.Session;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/** The class is Thread class of Consumer. ConsumerThread. */
public class ConsumerThread implements Runnable {

  private static final Logger logger = LoggerFactory.getLogger(ConsumerThread.class);
  private KafkaConsumer<String, String> consumer;

  public ConsumerThread(KafkaConsumer<String, String> consumer) {
    this.consumer = consumer;
  }

  /** insert data to IoTDB */
  private void insert(Session session, String data)
      throws IoTDBConnectionException, StatementExecutionException {
    String[] dataArray = data.split(",");
    String device = dataArray[0];
    long time = Long.parseLong(dataArray[1]);
    List<String> measurements = Arrays.asList(dataArray[2].split(":"));
    List<TSDataType> types = new ArrayList<>();
    for (String type : dataArray[3].split(":")) {
      types.add(TSDataType.valueOf(type));
    }

    List<Object> values = new ArrayList<>();
    String[] valuesStr = dataArray[4].split(":");
    for (int i = 0; i < valuesStr.length; i++) {
      switch (types.get(i)) {
        case INT64:
          values.add(Long.parseLong(valuesStr[i]));
          break;
        case DOUBLE:
          values.add(Double.parseDouble(valuesStr[i]));
          break;
        case INT32:
          values.add(Integer.parseInt(valuesStr[i]));
          break;
        case TEXT:
          values.add(valuesStr[i]);
          break;
        case FLOAT:
          values.add(Float.parseFloat(valuesStr[i]));
          break;
        case BOOLEAN:
          values.add(Boolean.parseBoolean(valuesStr[i]));
          break;
      }
    }

    session.insertRecord(device, time, measurements, types, values);
  }

  @Override
  public void run() {
    try {
      do {
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
        Session session =
            new Session(
                Constant.IOTDB_CONNECTION_HOST,
                Constant.IOTDB_CONNECTION_PORT,
                Constant.IOTDB_CONNECTION_USER,
                Constant.IOTDB_CONNECTION_PASSWORD);
        session.open();
        for (ConsumerRecord<String, String> record : records) {
          insert(session, record.value());
        }
      } while (true);
    } catch (Exception e) {
      logger.error(e.getMessage());
    }
  }
}
