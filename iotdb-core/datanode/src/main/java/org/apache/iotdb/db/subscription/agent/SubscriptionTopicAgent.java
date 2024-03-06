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

package org.apache.iotdb.db.subscription.agent;

import org.apache.iotdb.commons.exception.StartupException;
import org.apache.iotdb.commons.service.IService;
import org.apache.iotdb.commons.service.ServiceType;
import org.apache.iotdb.db.subscription.meta.TopicMeta;
import org.apache.iotdb.rpc.subscription.payload.request.TopicConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Objects;

public class SubscriptionTopicAgent implements IService {

  private static final Logger LOGGER = LoggerFactory.getLogger(SubscriptionTopicAgent.class);

  // TODO: sync from node-commons
  private Map<String, TopicMeta> topicNameToTopicMeta;

  public void createTopic(TopicConfig topicConfig) {
    String topicName = topicConfig.getTopicName();
    if (isTopicExist(topicName)) {
      LOGGER.warn("Subscription: topic {} already exist", topicName);
      return;
    }
    topicNameToTopicMeta.put(topicName, new TopicMeta(topicConfig));
    // TODO: call CN rpc
  }

  public void dropTopic(String topicName) {
    if (!topicNameToTopicMeta.containsKey(topicName)) {
      LOGGER.warn("Subscription: topic {} not exist", topicName);
      return;
    }
    TopicMeta topicMeta = topicNameToTopicMeta.get(topicName);
    if (topicMeta.hasSubscribedConsumerGroup()) {
      LOGGER.warn("Subscription: topic {} has subscribed consumer group", topicName);
      return;
    }
    topicNameToTopicMeta.remove(topicName);
    // TODO: call CN rpc
  }

  public boolean isTopicExist(String topicName) {
    return topicNameToTopicMeta.containsKey(topicName);
  }

  public void addSubscribedConsumerGroupID(String topicName, String consumerGroupID) {
    TopicMeta topicMeta = topicNameToTopicMeta.get(topicName);
    if (Objects.isNull(topicMeta)) {
      LOGGER.warn("Subscription: topic {} not exist", topicName);
      return;
    }
    topicMeta.addSubscribedConsumerGroupID(consumerGroupID);
  }

  //////////////////////////// singleton ////////////////////////////

  // TODO: fetch meta when started
  @Override
  public void start() throws StartupException {}

  @Override
  public void stop() {}

  @Override
  public ServiceType getID() {
    return null;
  }
}
