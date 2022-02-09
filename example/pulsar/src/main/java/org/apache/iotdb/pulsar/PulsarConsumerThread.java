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

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class PulsarConsumerThread implements Runnable {
  private static final String INSERT_TEMPLATE =
      "INSERT INTO root.vehicle.%s(timestamp,%s) VALUES (%s,'%s')";

  private static final Logger logger = LoggerFactory.getLogger(PulsarConsumerThread.class);

  private final Consumer<?> consumer;

  public PulsarConsumerThread(Consumer<?> consumer) throws ClassNotFoundException {
    this.consumer = consumer;
    Class.forName("org.apache.iotdb.jdbc.IoTDBDriver");
  }

  /** Write data to IoTDB */
  private void writeData(Statement statement, String message) throws SQLException {

    String[] items = message.split(",");

    String sql = String.format(INSERT_TEMPLATE, items[0], items[1], items[2], items[3]);
    statement.execute(sql);
  }

  @SuppressWarnings("squid:S2068")
  @Override
  public void run() {
    try (Connection connection =
            DriverManager.getConnection(
                Constant.IOTDB_CONNECTION_URL,
                Constant.IOTDB_CONNECTION_USER,
                Constant.IOTDB_CONNECTION_PASSWORD);
        Statement statement = connection.createStatement()) {
      do {
        Message<?> msg = consumer.receive();
        writeData(statement, new String(msg.getData()));

        consumer.acknowledge(msg);
      } while (true);
    } catch (Exception e) {
      logger.error(e.getMessage());
    }
  }
}
