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
package org.apache.iotdb.pipe.external.kafka;

import org.apache.iotdb.session.pool.SessionPool;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** The class is Thread class of Consumer. ConsumerThread. */
public class ConsumerThreadSync implements Runnable {

  private static final Logger logger = LoggerFactory.getLogger(ConsumerThreadSync.class);
  private final KafkaConsumer<String, String> consumer;
  private final SessionPool pool;
  private boolean is_open = true;
  private final boolean single_partition;
  private final KafkaLoaderLogManager logManager;

  public ConsumerThreadSync(
      KafkaConsumer<String, String> consumer,
      SessionPool pool,
      boolean single_partition,
      KafkaLoaderLogManager logManager) {
    this.consumer = consumer;
    this.pool = pool;
    this.single_partition = single_partition;
    this.logManager = logManager;
  }

  /**
   * insert data to IoTDB, support deletion iff the partition of this topic is single. TODO: support
   * aligned records insertion
   */
  private void handleDatas(List<String> datas) {
    int size = datas.size();
    List<String> deviceIds = new ArrayList<>(size);
    List<String> deviceStringIds = new ArrayList<>(size);
    List<Long> times = new ArrayList<>(size);
    List<List<String>> measurementsList = new ArrayList<>(size);
    List<List<TSDataType>> typesList = new ArrayList<>(size);
    List<List<Object>> valuesList = new ArrayList<>(size);
    List<List<String>> valuesStringList = new ArrayList<>(size);

    Map<String, Integer> deleteMap = new HashMap<>();

    List<String> delTimeDeviceIds = new ArrayList<>(size);

    for (String data : datas) {
      String[] suffix = data.split(":");
      if (suffix[0].equals("delete")) {
        if (single_partition) {
          if (!deleteMap.containsKey(suffix[1])
              || Integer.parseInt(suffix[2]) > deleteMap.get(suffix[1])) {
            deleteMap.put(suffix[1], Integer.parseInt(suffix[2]));
          }
        }
        continue;
      } else if (suffix[0].equals("del_time")) {
        if (single_partition) {
          delTimeDeviceIds.add(suffix[1]);
        }
        continue;
      }
      String[] dataArray = data.split(",");
      boolean is_string_insert = false;
      String device = dataArray[0];
      long time = Long.parseLong(dataArray[1]);
      List<String> measurements = Arrays.asList(dataArray[2].split(":"));
      List<TSDataType> types = new ArrayList<>();

      for (String type : dataArray[3].split(":")) {
        try {
          types.add(TSDataType.valueOf(type));
        } catch (IllegalArgumentException e) {
          is_string_insert = true;
          break;
        }
      }

      if (is_string_insert) {
        deviceStringIds.add(device);
        valuesStringList.add(Arrays.asList(dataArray[3].split(":")));
        times.add(time);
        measurementsList.add(measurements);
        break;
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
      deviceIds.add(device);
      valuesList.add(values);
      typesList.add(types);
      times.add(time);
      measurementsList.add(measurements);
    }

    try {
      pool.insertRecords(deviceIds, times, measurementsList, typesList, valuesList);
    } catch (Exception e) {
      logger.error("Kafka sync insertion failure, data batch = \n{}", String.join("\n", datas), e);
    }

    try {
      pool.insertRecords(deviceStringIds, times, measurementsList, valuesStringList);
    } catch (Exception e) {
      logger.error(
          "Kafka sync string insertion failure, data batch = \n{}", String.join("\n", datas), e);
    }

    try {
      for (String deleteDeviceId : deleteMap.keySet()) {
        pool.deleteData(deleteDeviceId, deleteMap.get(deleteDeviceId));
      }
    } catch (Exception e) {
      logger.error(
          "Kafka sync data deletion failure, data batch = \n{}", String.join("\n", datas), e);
    }

    try {
      pool.deleteTimeseries(delTimeDeviceIds);
    } catch (Exception e) {
      logger.error(
          "Kafka sync time-series deletion failure, data batch = \n{}",
          String.join("\n", datas),
          e);
    }
  }

  public void close() {
    this.is_open = false;
  }

  public void open() {
    this.is_open = true;
  }

  @Override
  public void run() {
    try {
      do {
        ConsumerRecords<String, String> records = this.consumer.poll(Duration.ofSeconds(1));
        List<String> datas = new ArrayList<>(records.count());
        for (ConsumerRecord<String, String> record : records) {
          datas.add(record.value());
        }
        handleDatas(datas);
        if (records.count() > 0) {
          logManager.writeOffset();
        }
      } while (this.is_open);
      logger.info("One consumer thread shut down.\n");
    } catch (Exception e) {
      logger.error(e.getMessage());
    }
  }
}
