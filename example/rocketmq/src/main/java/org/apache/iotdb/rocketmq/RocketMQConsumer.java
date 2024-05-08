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
package org.apache.iotdb.rocketmq;

import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class RocketMQConsumer {

  private static final Logger logger = LoggerFactory.getLogger(RocketMQConsumer.class);
  private static Session session;
  private DefaultMQPushConsumer consumer;
  private String producerGroup;
  private String serverAddresses;

  public RocketMQConsumer(
      String producerGroup,
      String serverAddresses,
      String connectionHost,
      int connectionPort,
      String user,
      String password)
      throws IoTDBConnectionException, StatementExecutionException {
    this.producerGroup = producerGroup;
    this.serverAddresses = serverAddresses;
    this.consumer = new DefaultMQPushConsumer(producerGroup);
    this.consumer.setNamesrvAddr(serverAddresses);
    initIoTDB(connectionHost, connectionPort, user, password);
  }

  public static void main(String[] args)
      throws MQClientException, StatementExecutionException, IoTDBConnectionException {
    /** Instantiate with specified consumer group name and specify name server addresses. */
    RocketMQConsumer consumer =
        new RocketMQConsumer(
            Constant.CONSUMER_GROUP,
            Constant.SERVER_ADDRESS,
            Constant.IOTDB_CONNECTION_HOST,
            Constant.IOTDB_CONNECTION_PORT,
            Constant.IOTDB_CONNECTION_USER,
            Constant.IOTDB_CONNECTION_PASSWORD);
    consumer.prepareConsume();
    consumer.addStorageGroup(Constant.ADDITIONAL_STORAGE_GROUP);
    consumer.start();
  }

  private void initIoTDB(String host, int port, String user, String password)
      throws IoTDBConnectionException, StatementExecutionException {
    if (host == null) {
      host = Constant.IOTDB_CONNECTION_HOST;
      port = Constant.IOTDB_CONNECTION_PORT;
    }
    user = (user == null ? Constant.IOTDB_CONNECTION_USER : user);
    password = (password == null ? Constant.IOTDB_CONNECTION_PASSWORD : password);
    session = new Session(host, port, user, password);
    session.open();
    for (String storageGroup : Constant.STORAGE_GROUP) {
      addStorageGroup(storageGroup);
    }
    for (String[] sql : Constant.CREATE_TIMESERIES) {
      createTimeseries(sql);
    }
  }

  private void addStorageGroup(String storageGroup)
      throws IoTDBConnectionException, StatementExecutionException {
    session.setStorageGroup(storageGroup);
  }

  private void createTimeseries(String[] sql)
      throws StatementExecutionException, IoTDBConnectionException {
    String timeseries = sql[0];
    TSDataType dataType = TSDataType.valueOf(sql[1]);
    TSEncoding encoding = TSEncoding.valueOf(sql[2]);
    CompressionType compressionType = CompressionType.valueOf(sql[3]);
    session.createTimeseries(timeseries, dataType, encoding, compressionType);
  }

  private void insert(String data) throws IoTDBConnectionException, StatementExecutionException {
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

  public void start() throws MQClientException {
    consumer.start();
  }

  /**
   * Subscribe topic and add register Listener
   *
   * @throws MQClientException
   */
  public void prepareConsume() throws MQClientException {
    /** Subscribe one more more topics to consume. */
    consumer.subscribe(Constant.TOPIC, "*");
    /**
     * Setting Consumer to start first from the head of the queue or from the tail of the queue If
     * not for the first time, then continue to consume according to the position of last
     * consumption.
     */
    consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
    /** Register callback to execute on arrival of messages fetched from brokers. */
    consumer.registerMessageListener(
        (MessageListenerOrderly)
            (msgs, context) -> {
              for (MessageExt msg : msgs) {
                logger.info(
                    String.format(
                        "%s Receive New Messages: %s %n",
                        Thread.currentThread().getName(), new String(msg.getBody())));
                try {
                  insert(new String(msg.getBody()));
                } catch (Exception e) {
                  logger.error(e.getMessage());
                }
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
}
