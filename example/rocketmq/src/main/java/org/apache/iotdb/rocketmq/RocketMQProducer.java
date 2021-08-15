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

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;

public class RocketMQProducer {

  private DefaultMQProducer producer;
  private String producerGroup;
  private String serverAddresses;
  private static final Logger logger = LoggerFactory.getLogger(RocketMQProducer.class);

  public RocketMQProducer(String producerGroup, String serverAddresses) {
    this.producerGroup = producerGroup;
    this.serverAddresses = serverAddresses;
    this.producer = new DefaultMQProducer(producerGroup);
    this.producer.setNamesrvAddr(serverAddresses);
  }

  public void start() throws MQClientException {
    producer.start();
  }

  public void sendMessage()
      throws UnsupportedEncodingException, InterruptedException, RemotingException,
          MQClientException, MQBrokerException {
    for (String sql : Constant.ALL_DATA) {
      /** Create a message instance, specifying topic, tag and message body. */
      Message msg =
          new Message(Constant.TOPIC, null, null, (sql).getBytes(RemotingHelper.DEFAULT_CHARSET));
      SendResult sendResult =
          producer.send(
              msg,
              (mqs, msg1, arg) -> {
                Integer id = (Integer) arg;
                int index = id % mqs.size();
                return mqs.get(index);
              },
              Utils.ConvertStringToInteger(Utils.getTimeSeries(sql)));
      logger.info(sendResult.toString());
    }
  }

  public void shutdown() {
    producer.shutdown();
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

  public static void main(String[] args) throws Exception {
    /** Instantiate with a producer group name and specify name server addresses. */
    RocketMQProducer producer =
        new RocketMQProducer(Constant.PRODUCER_GROUP, Constant.SERVER_ADDRESS);
    /** Launch the instance */
    producer.start();
    producer.sendMessage();
    producer.shutdown();
  }
}
