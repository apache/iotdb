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
package org.apache.iotdb.pulsar;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClientException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PulsarConsumerThread implements Runnable {
  private Connection connection = null;
  private Statement statement = null;
  private static boolean setStorageGroup = true;
  private static boolean createTimeSeries = true;
  private static final String CREATE_SG_TEMPLATE = "SET STORAGE GROUP TO %s";
  private static final String CREATE_TIMESERIES_TEMPLATE = "CREATE TIMESERIES %s WITH DATATYPE=TEXT, ENCODING=PLAIN";
  private static final String INSERT_TEMPLATE = "INSERT INTO root.vehicle.deviceid(timestamp,%s) VALUES (%s,'%s')";
  protected static final String[] ALL_TIMESERIES = {
      "root.vehicle.deviceid.sensor1",
      "root.vehicle.deviceid.sensor2",
      "root.vehicle.deviceid.sensor3",
      "root.vehicle.deviceid.sensor4"};

  private static final Logger logger = LoggerFactory.getLogger(PulsarConsumerThread.class);

  private final Consumer<?> consumer;

  public PulsarConsumerThread(Consumer<?> consumer) {
    this.consumer = consumer;
    initIoTDB();
  }

  private void initIoTDB() {
    try {
      Class.forName("org.apache.iotdb.jdbc.IoTDBDriver");
      connection = DriverManager
          .getConnection(Constant.IOTDB_CONNECTION_URL, Constant.IOTDB_CONNECTION_USER,
              Constant.IOTDB_CONNECTION_PASSWORD);
      prepareTimeseries();
    } catch (ClassNotFoundException | SQLException e) {
      logger.error(e.getMessage());
    }
  }

  private void prepareTimeseries() throws SQLException {
    statement = connection.createStatement();
    if (setStorageGroup) {
      statement.execute(String.format(CREATE_SG_TEMPLATE, Constant.STORAGE_GROUP));
      setStorageGroup = false;
    }
    if (createTimeSeries) {
      for (String timeseries : ALL_TIMESERIES) {
        statement.addBatch(String.format(CREATE_TIMESERIES_TEMPLATE, timeseries));
      }
      statement.executeBatch();
      statement.clearBatch();
      createTimeSeries = false;
    }
  }

  /**
   * Write data to IoTDB
   */
  private void writeData(String message) {

    String[] items = message.split(",");

    try {
      String sql = String.format(INSERT_TEMPLATE, items[0], items[1], items[2]);
      statement.execute(sql);
    } catch (SQLException e) {
      logger.error(e.getMessage());
    }
  }

  @Override
  public void run() {
    do {
      try {
        Message<?> msg = consumer.receive();
        writeData(new String(msg.getData()));

        consumer.acknowledge(msg);
      } catch (PulsarClientException e) {
        logger.error(e.getMessage());
        break;
      }
    } while (true);
  }
}
