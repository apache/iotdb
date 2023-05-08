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

package org.apache.iotdb.subscription.api.strategy.topic;

import org.apache.iotdb.subscription.api.exception.SubscriptionStrategyNotValidException;
import org.apache.iotdb.tsfile.read.common.parser.PathNodesGenerator;

import org.antlr.v4.runtime.misc.ParseCancellationException;
import org.apache.commons.lang3.StringUtils;

public class SingleTopicStrategy implements TopicsStrategy {

  private final String topic;

  public SingleTopicStrategy(String topic) {
    this.topic = topic;
  }

  public String getTopic() {
    return topic;
  }

  @Override
  public void check() throws SubscriptionStrategyNotValidException {
    if (StringUtils.isAllBlank(topic)) {
      throw new SubscriptionStrategyNotValidException("topic is not set!");
    }
    try {
      PathNodesGenerator.checkPath(topic);
    } catch (ParseCancellationException e) {
      throw new SubscriptionStrategyNotValidException(
          String.format("%s is not a legal path pattern", topic), e);
    }
  }
}
