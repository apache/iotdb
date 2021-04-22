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

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;

/**
 * The class is to show how to get data from RabbitMQ. The data is sent by class RabbitMQProducer.
 */
public class RabbitMQConsumer {

  private static final Logger logger = LoggerFactory.getLogger(RabbitMQProducer.class);

  private static final ExecutorService executor =
      Executors.newFixedThreadPool(Constant.CONSUMER_THREAD_NUM);

  public static void main(String[] args) throws IOException, TimeoutException {
    Channel channel = RabbitMQChannelUtils.getChannelInstance(Constant.CONNECTION_NAME);
    AMQP.Queue.DeclareOk declareOk =
        channel.queueDeclare(Constant.RABBITMQ_CONSUMER_QUEUE, true, false, false, new HashMap<>());
    channel.exchangeDeclare(Constant.TOPIC, BuiltinExchangeType.TOPIC);
    channel.queueBind(declareOk.getQueue(), Constant.TOPIC, "IoTDB.#", new HashMap<>());
    DefaultConsumer defaultConsumer =
        new DefaultConsumer(channel) {
          @Override
          public void handleDelivery(
              String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) {
            logger.info(consumerTag + ", " + envelope.toString() + ", " + properties.toString());
            executor.submit(new RabbitMQConsumerThread(new String(body)));
          }
        };
    channel.basicConsume(
        declareOk.getQueue(), true, Constant.RABBITMQ_CONSUMER_TAG, defaultConsumer);
  }
}
