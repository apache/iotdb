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
import org.apache.pulsar.client.api.KeySharedPolicy;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SubscriptionType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class PulsarConsumer {
  private static final String SERVICE_URL = "pulsar://localhost:6650";
  // Specify the number of consumers
  private static final int CONSUMER_NUM = 3;
  private List<Consumer<?>> consumerList;
  private static final String CREATE_SG_TEMPLATE = "SET STORAGE GROUP TO %s";
  private static final String CREATE_TIMESERIES_TEMPLATE =
      "CREATE TIMESERIES %s WITH DATATYPE=TEXT, ENCODING=PLAIN";
  private static final Logger logger = LoggerFactory.getLogger(PulsarConsumer.class);
  protected static final String[] ALL_TIMESERIES = {
    "root.vehicle.device1.sensor1",
    "root.vehicle.device1.sensor2",
    "root.vehicle.device2.sensor1",
    "root.vehicle.device2.sensor2",
    "root.vehicle.device3.sensor1",
    "root.vehicle.device3.sensor2",
  };

  public PulsarConsumer(List<Consumer<?>> consumerList) {
    this.consumerList = consumerList;
  }

  public void consumeInParallel() throws ClassNotFoundException {
    ExecutorService executor = Executors.newFixedThreadPool(CONSUMER_NUM);
    for (int i = 0; i < consumerList.size(); i++) {
      PulsarConsumerThread consumerExecutor = new PulsarConsumerThread(consumerList.get(i));
      executor.submit(consumerExecutor);
    }
  }

  @SuppressWarnings("squid:S2068")
  private static void prepareSchema() {
    try {
      Class.forName("org.apache.iotdb.jdbc.IoTDBDriver");
      try (Connection connection =
              DriverManager.getConnection(
                  Constant.IOTDB_CONNECTION_URL,
                  Constant.IOTDB_CONNECTION_USER,
                  Constant.IOTDB_CONNECTION_PASSWORD);
          Statement statement = connection.createStatement()) {

        statement.execute(String.format(CREATE_SG_TEMPLATE, Constant.STORAGE_GROUP));

        for (String timeseries : ALL_TIMESERIES) {
          statement.addBatch(String.format(CREATE_TIMESERIES_TEMPLATE, timeseries));
        }
        statement.executeBatch();
        statement.clearBatch();
      }
    } catch (ClassNotFoundException | SQLException e) {
      logger.error(e.getMessage());
    }
  }

  public static void main(String[] args) throws PulsarClientException, ClassNotFoundException {
    PulsarClient client = PulsarClient.builder().serviceUrl(SERVICE_URL).build();

    List<Consumer<?>> consumerList = new ArrayList<>();
    for (int i = 0; i < CONSUMER_NUM; i++) {
      // In shared subscription mode, multiple consumers can attach to the same subscription
      // and message are delivered in a round robin distribution across consumers.
      Consumer<byte[]> consumer =
          client
              .newConsumer()
              .topic(Constant.TOPIC_NAME)
              .subscriptionName("shared-subscription")
              .subscriptionType(SubscriptionType.Key_Shared)
              .keySharedPolicy(KeySharedPolicy.autoSplitHashRange())
              .subscribe();
      consumerList.add(consumer);
    }
    PulsarConsumer pulsarConsumer = new PulsarConsumer(consumerList);
    prepareSchema();
    pulsarConsumer.consumeInParallel();
  }
}
