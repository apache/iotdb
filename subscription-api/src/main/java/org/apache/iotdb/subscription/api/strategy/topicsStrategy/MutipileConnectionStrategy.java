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

package org.apache.iotdb.subscription.api.strategy.topicsStrategy;

import org.apache.iotdb.subscription.api.exception.SubscriptionStrategyNotValidException;
import org.apache.iotdb.tsfile.read.common.parser.PathNodesGenerator;

import org.antlr.v4.runtime.misc.ParseCancellationException;

import java.util.List;

public class MutipileConnectionStrategy implements TopicsStrategy {
  private final List<String> topics;

  public MutipileConnectionStrategy(List<String> topics) {
    this.topics = topics;
  }

  @Override
  public void check() throws SubscriptionStrategyNotValidException {
    if (topics == null || topics.isEmpty()) {
      throw new SubscriptionStrategyNotValidException("topics is not set!");
    }
    topics.forEach(
        topic -> {
          try {
            PathNodesGenerator.checkPath(topic);
          } catch (ParseCancellationException ex) {
            throw new SubscriptionStrategyNotValidException(
                String.format("%s is not a legal path, topic is set to error value", topic), ex);
          }
        });
  }
}
