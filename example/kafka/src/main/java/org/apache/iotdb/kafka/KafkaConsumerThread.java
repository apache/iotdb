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

import kafka.consumer.KafkaStream;
import kafka.message.MessageAndMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class KafkaConsumerThread implements Runnable {

  private Connection connection = null;
  private Statement statement = null;
  private KafkaStream<String, String> stream;
  private static boolean setStorageGroup = true;
  private static boolean createTimeSeries = true;
  private String createStorageGroupSqlTemplate = "SET STORAGE GROUP TO %s";
  private String createTimeseriesSqlTemplate =
      "CREATE TIMESERIES %s WITH DATATYPE=TEXT, ENCODING=PLAIN";
  private String insertDataSqlTemplate =
      "INSERT INTO root.vehicle.deviceid(timestamp,%s) VALUES (%s,'%s')";
  private static final Logger logger = LoggerFactory.getLogger(KafkaConsumerThread.class);

  public KafkaConsumerThread(KafkaStream<String, String> stream) {
    this.stream = stream;
    /** Establish JDBC connection of IoTDB */
    initIoTDB();
  }

  private void initIoTDB() {
    try {
      Class.forName("org.apache.iotdb.jdbc.IoTDBDriver");
      connection =
          DriverManager.getConnection(
              Constant.IOTDB_CONNECTION_URL,
              Constant.IOTDB_CONNECTION_USER,
              Constant.IOTDB_CONNECTION_PASSWORD);
      statement = connection.createStatement();
      if (setStorageGroup) {
        try {
          statement.execute(String.format(createStorageGroupSqlTemplate, Constant.STORAGE_GROUP));
        } catch (SQLException e) {
        }
        setStorageGroup = false;
      }
      if (createTimeSeries) {
        for (String timeseries : Constant.ALL_TIMESERIES) {
          statement.addBatch(String.format(createTimeseriesSqlTemplate, timeseries));
        }
        statement.executeBatch();
        statement.clearBatch();
        createTimeSeries = false;
      }
    } catch (ClassNotFoundException | SQLException e) {
      logger.error(e.getMessage());
    }
  }

  /** Write data to IoTDB */
  private void writeData(String message) {

    String[] items = message.split(",");

    try {
      String sql = String.format(insertDataSqlTemplate, items[0], items[1], items[2]);
      statement.execute(sql);
    } catch (SQLException e) {
      logger.error(e.getMessage());
    }
  }

  @Override
  public void run() {
    for (MessageAndMetadata<String, String> consumerIterator : stream) {
      String uploadMessage = consumerIterator.message();
      logger.info(
          String.format(
              "%s from partiton[%d]: %s",
              Thread.currentThread().getName(), consumerIterator.partition(), uploadMessage));
      writeData(uploadMessage);
    }
  }
}
