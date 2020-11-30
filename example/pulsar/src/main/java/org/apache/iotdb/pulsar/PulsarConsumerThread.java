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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PulsarConsumerThread implements Runnable {
  private Statement statement = null;
  private static final String INSERT_TEMPLATE = "INSERT INTO root.vehicle.%s(timestamp,%s) VALUES (%s,'%s')";

  private static final Logger logger = LoggerFactory.getLogger(PulsarConsumerThread.class);

  private final Consumer<?> consumer;

  public PulsarConsumerThread(Consumer<?> consumer) {
    this.consumer = consumer;
  }

  /**
   * Write data to IoTDB
   */
  private void writeData(String message) {

    String[] items = message.split(",");

    try {
      String sql = String.format(INSERT_TEMPLATE, items[0], items[1], items[2], items[3]);
      statement.execute(sql);
    } catch (SQLException e) {
      logger.error(e.getMessage());
    }
  }

  @Override
  public void run() {
    try {
      Class.forName("org.apache.iotdb.jdbc.IoTDBDriver");
      Connection connection = DriverManager
          .getConnection(Constant.IOTDB_CONNECTION_URL, Constant.IOTDB_CONNECTION_USER,
              "root");
      statement = connection.createStatement();
      do {
        Message<?> msg = consumer.receive();
        writeData(new String(msg.getData()));

        consumer.acknowledge(msg);
      } while (true);
    } catch (Exception e) {
      logger.error(e.getMessage());
    }
  }
}
