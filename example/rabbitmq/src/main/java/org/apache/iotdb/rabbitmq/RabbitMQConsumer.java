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

package org.apache.iotdb.rabbitmq;

import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

/**
 * The class is to show how to get data from RabbitMQ. The data is sent by class RabbitMQProducer.
 */
public class RabbitMQConsumer {

  private static final Logger logger = LoggerFactory.getLogger(RabbitMQConsumer.class);

  public RabbitMQConsumer() {}

  public static void main(String[] args) throws Exception {
    try (Session session =
        new Session(
            Constant.IOTDB_CONNECTION_HOST,
            Constant.IOTDB_CONNECTION_PORT,
            Constant.IOTDB_CONNECTION_USER,
            Constant.IOTDB_CONNECTION_PWD)) {
      session.open();
      session.setStorageGroup(Constant.STORAGE_GROUP);
      for (String[] timeseries : Constant.TIMESERIESLIST) {
        createTimeseries(session, timeseries);
      }
      RabbitMQConsumer consumer = new RabbitMQConsumer();
      Channel channel = RabbitMQChannelUtils.getChannelInstance(Constant.CONNECTION_NAME);
      AMQP.Queue.DeclareOk declareOk =
          channel.queueDeclare(
              Constant.RABBITMQ_CONSUMER_QUEUE, true, false, false, new HashMap<>());
      channel.exchangeDeclare(Constant.TOPIC, BuiltinExchangeType.TOPIC);
      channel.queueBind(declareOk.getQueue(), Constant.TOPIC, "IoTDB.#", new HashMap<>());
      DefaultConsumer defaultConsumer =
          new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(
                String consumerTag,
                Envelope envelope,
                AMQP.BasicProperties properties,
                byte[] body) {
              logger.info(consumerTag + ", " + envelope.toString() + ", " + properties.toString());
              try {
                consumer.insert(session, new String(body));
              } catch (Exception e) {
                logger.error(e.getMessage());
              }
            }
          };
      channel.basicConsume(
          declareOk.getQueue(), true, Constant.RABBITMQ_CONSUMER_TAG, defaultConsumer);
    }
  }

  private static void createTimeseries(Session session, String[] timeseries)
      throws StatementExecutionException, IoTDBConnectionException {
    String timeseriesId = timeseries[0];
    TSDataType dataType = TSDataType.valueOf(timeseries[1]);
    TSEncoding encoding = TSEncoding.valueOf(timeseries[2]);
    CompressionType compressionType = CompressionType.valueOf(timeseries[3]);
    session.createTimeseries(timeseriesId, dataType, encoding, compressionType);
  }

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
        default:
          break;
      }
    }

    session.insertRecord(device, time, measurements, types, values);
  }
}
