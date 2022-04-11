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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.TimeoutException;

/** The class is to show how to send data to RabbitMQ. */
public class RabbitMQProducer {

  private static final Logger logger = LoggerFactory.getLogger(RabbitMQProducer.class);

  public static void main(String[] args) throws IOException, TimeoutException {
    Channel channel = RabbitMQChannelUtils.getChannelInstance(Constant.CONNECTION_NAME);
    channel.exchangeDeclare(Constant.TOPIC, BuiltinExchangeType.TOPIC);
    AMQP.BasicProperties basicProperties =
        new AMQP.BasicProperties().builder().deliveryMode(2).contentType("UTF-8").build();
    for (int i = 0; i < Constant.ALL_DATA.length; i++) {
      String key = String.format("%s.%s", "IoTDB", Objects.toString(i));
      channel.basicPublish(
          Constant.TOPIC, key, false, basicProperties, Constant.ALL_DATA[i].getBytes());
      logger.info(Constant.ALL_DATA[i]);
    }
  }
}
