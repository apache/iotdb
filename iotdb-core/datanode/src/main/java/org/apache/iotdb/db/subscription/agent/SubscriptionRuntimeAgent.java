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
import org.apache.iotdb.commons.subscription.config.SubscriptionConfig;

import java.util.concurrent.atomic.AtomicBoolean;

public class SubscriptionRuntimeAgent implements IService {

  private final SubscriptionConsumerAgent consumerAgent;

  private final SubscriptionBrokerAgent brokerAgent;

  private final SubscriptionTopicAgent topicAgent;

  private final AtomicBoolean isShutdown = new AtomicBoolean(false);

  SubscriptionRuntimeAgent() {
    // make it package-private
    consumerAgent = new SubscriptionConsumerAgent();
    brokerAgent = new SubscriptionBrokerAgent();
    topicAgent = new SubscriptionTopicAgent();
  }

  SubscriptionConsumerAgent consumer() {
    // make it package-private
    return consumerAgent;
  }

  SubscriptionBrokerAgent broker() {
    // make it package-private
    return brokerAgent;
  }

  SubscriptionTopicAgent topic() {
    // make it package-private
    return topicAgent;
  }

  //////////////////////////// System Service Interface ////////////////////////////

  @Override
  public void start() throws StartupException {
    if (!SubscriptionConfig.getInstance().getSubscriptionEnabled()) {
      return;
    }

    SubscriptionConfig.getInstance().printAllConfigs();

    SubscriptionAgentLauncher.launchSubscriptionTopicAgent();
    SubscriptionAgentLauncher.launchSubscriptionConsumerAgent();

    isShutdown.set(false);
  }

  @Override
  public void stop() {
    if (isShutdown.get()) {
      return;
    }
    isShutdown.set(true);

    // let PipeDataNodeRuntimeAgent to drop all related pipe tasks
  }

  public boolean isShutdown() {
    return isShutdown.get();
  }

  @Override
  public ServiceType getID() {
    return ServiceType.SUBSCRIPTION_RUNTIME_AGENT;
  }
}
