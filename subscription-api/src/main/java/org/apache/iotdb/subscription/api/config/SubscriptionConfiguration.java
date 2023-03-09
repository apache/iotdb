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

package org.apache.iotdb.subscription.api.config;

import org.apache.iotdb.subscription.api.exception.SubscriptionException;
import org.apache.iotdb.subscription.api.strategy.disorderHandling.DisorderHandlingStrategy;
import org.apache.iotdb.subscription.api.strategy.topicsStrategy.TopicsStrategy;

public class SubscriptionConfiguration {
  private String host;
  private int port;
  private String username;
  private String password;
  private String group;
  private DisorderHandlingStrategy disorderHandlingStrategy;
  private TopicsStrategy topicsStrategy;

  public SubscriptionConfiguration(String host, int port, String username, String password) {
    this(host, port, username, password, null);
  }

  public SubscriptionConfiguration(
      String host, int port, String username, String password, String group) {
    this.host = host;
    this.port = port;
    this.username = username;
    this.password = password;
    this.group = group;
  }

  public SubscriptionConfiguration disorderHandlingStrategy(
      DisorderHandlingStrategy disorderHandlingStrategy) {
    this.disorderHandlingStrategy = disorderHandlingStrategy;
    return this;
  }

  public SubscriptionConfiguration topicsStrategy(TopicsStrategy topicsStrategy) {
    this.topicsStrategy = topicsStrategy;
    return this;
  }

  public void check() throws SubscriptionException {
    if (disorderHandlingStrategy == null) {
      throw new SubscriptionException("WatermarkStrategy is not set!");
    }
    disorderHandlingStrategy.check();

    if (topicsStrategy == null) {
      throw new SubscriptionException("TopicsStrategy is not set!");
    }
    topicsStrategy.check();
  }

  public static class Builder {
    private String host;
    private int port;
    private String username;
    private String password;
    // 消费者组
    private String group;

    public Builder host(String host) {
      this.host = host;
      return this;
    }

    public Builder port(int port) {
      this.port = port;
      return this;
    }

    public Builder username(String username) {
      this.username = username;
      return this;
    }

    public Builder password(String password) {
      this.password = password;
      return this;
    }

    public Builder group(String group) {
      this.group = group;
      return this;
    }

    public SubscriptionConfiguration build() {
      return new SubscriptionConfiguration(host, port, username, password, group);
    }
  }
}
