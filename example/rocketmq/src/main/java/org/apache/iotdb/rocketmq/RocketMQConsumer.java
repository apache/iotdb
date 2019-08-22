/**
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
package org.apache.iotdb.rocketmq;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author lta
 */
public class RocketMQConsumer {

  private DefaultMQPushConsumer consumer;
  private String producerGroup;
  private String serverAddresses;
  private Connection connection;
  private Statement statement;
  private String createStorageGroupSqlTemplate = "SET STORAGE GROUP TO %s";
  private static final Logger logger = LoggerFactory.getLogger(RocketMQConsumer.class);

  public RocketMQConsumer(String producerGroup, String serverAddresses, String connectionUrl,
      String user, String password) throws ClassNotFoundException, SQLException {
    this.producerGroup = producerGroup;
    this.serverAddresses = serverAddresses;
    this.consumer = new DefaultMQPushConsumer(producerGroup);
    this.consumer.setNamesrvAddr(serverAddresses);
    initIoTDB(connectionUrl, user, password);
  }

  private void initIoTDB(String connectionUrl, String user, String password)
      throws ClassNotFoundException, SQLException {
    Class.forName("org.apache.iotdb.jdbc.IoTDBDriver");
    user = (user == null ? "root" : user);
    password = (password == null ? "root" : password);
    connection = DriverManager.getConnection(connectionUrl, user, password);
    statement = connection.createStatement();
    for (String storageGroup : Constant.STORAGE_GROUP) {
      statement.addBatch(String.format(createStorageGroupSqlTemplate, storageGroup));
    }
    for (String sql : Constant.CREATE_TIMESERIES) {
      statement.addBatch(sql);
    }
    try {
      statement.executeBatch();
    } catch (SQLException e) {
    }
    statement.clearBatch();
  }

  public void start() throws MQClientException {
    consumer.start();
  }

  /**
   * Subscribe topic and add regiser Listener
   * @throws MQClientException
   */
  public void prepareConsume() throws MQClientException {
    /**
     * Subscribe one more more topics to consume.
     */
    consumer.subscribe(Constant.TOPIC, "*");
    /**
     * Setting Consumer to start first from the head of the queue or from the tail of the queue
     * If not for the first time, then continue to consume according to the position of last consumption.
     */
    consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
    /**
     * Register callback to execute on arrival of messages fetched from brokers.
     */
    consumer.registerMessageListener((MessageListenerOrderly) (msgs, context) -> {
      logger.info(String.format("%s Receive New Messages: %s %n", Thread.currentThread().getName(),
          new String(msgs.get(0).getBody())));
      try {
        statement.execute(new String(msgs.get(0).getBody()));
      } catch (SQLException e) {
        logger.error(e.getMessage());
      }
      return ConsumeOrderlyStatus.SUCCESS;
    });
  }

  public void shutdown() {
    consumer.shutdown();
  }

  public String getProducerGroup() {
    return producerGroup;
  }

  public void setProducerGroup(String producerGroup) {
    this.producerGroup = producerGroup;
  }

  public String getServerAddresses() {
    return serverAddresses;
  }

  public void setServerAddresses(String serverAddresses) {
    this.serverAddresses = serverAddresses;
  }

  public static void main(String[] args)
      throws MQClientException, SQLException, ClassNotFoundException {
    /**
     *Instantiate with specified consumer group name and specify name server addresses.
     */
    RocketMQConsumer consumer = new RocketMQConsumer(Constant.CONSUMER_GROUP, Constant.SERVER_ADDRESS, Constant.IOTDB_CONNECTION_URL, Constant.IOTDB_CONNECTION_USER,
        Constant.IOTDB_CONNECTION_PASSWORD);
    consumer.prepareConsume();
    consumer.start();
  }
}
