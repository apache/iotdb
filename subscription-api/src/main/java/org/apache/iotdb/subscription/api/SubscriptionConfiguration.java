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

package org.apache.iotdb.subscription.api;

import org.apache.iotdb.subscription.api.exception.SubscriptionException;
import org.apache.iotdb.subscription.api.strategy.disorder.DisorderHandlingStrategy;
import org.apache.iotdb.subscription.api.strategy.topic.TopicsStrategy;

public class SubscriptionConfiguration {

  private String host;
  private Integer port;
  private String username;
  private String password;
  private String group;
  private DisorderHandlingStrategy disorderHandlingStrategy;
  private TopicsStrategy topicStrategy;

  private SubscriptionConfiguration() {}

  private void check() throws SubscriptionException {
    if (host == null) {
      throw new SubscriptionException("Host is not set!");
    }
    if (port == null) {
      throw new SubscriptionException("Port is not set!");
    }
    if (username == null) {
      throw new SubscriptionException("Username is not set!");
    }
    if (password == null) {
      throw new SubscriptionException("Password is not set!");
    }
    if (group == null) {
      throw new SubscriptionException("Group is not set!");
    }

    if (disorderHandlingStrategy == null) {
      throw new SubscriptionException("WatermarkStrategy is not set!");
    }
    disorderHandlingStrategy.check();

    if (topicStrategy == null) {
      throw new SubscriptionException("TopicStrategy is not set!");
    }
    topicStrategy.check();
  }

  public static class Builder {

    private final SubscriptionConfiguration subscriptionConfiguration;

    public Builder() {
      subscriptionConfiguration = new SubscriptionConfiguration();
    }

    public Builder host(String host) {
      subscriptionConfiguration.host = host;
      return this;
    }

    public Builder port(int port) {
      subscriptionConfiguration.port = port;
      return this;
    }

    public Builder username(String username) {
      subscriptionConfiguration.username = username;
      return this;
    }

    public Builder password(String password) {
      subscriptionConfiguration.password = password;
      return this;
    }

    public Builder group(String group) {
      subscriptionConfiguration.group = group;
      return this;
    }

    public Builder disorderHandlingStrategy(DisorderHandlingStrategy disorderHandlingStrategy) {
      subscriptionConfiguration.disorderHandlingStrategy = disorderHandlingStrategy;
      return this;
    }

    public Builder topicStrategy(TopicsStrategy topicStrategy) {
      subscriptionConfiguration.topicStrategy = topicStrategy;
      return this;
    }

    public SubscriptionConfiguration build() {
      subscriptionConfiguration.check();
      return subscriptionConfiguration;
    }
  }
}
