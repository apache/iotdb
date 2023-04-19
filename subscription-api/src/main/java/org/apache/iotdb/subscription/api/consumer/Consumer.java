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

package org.apache.iotdb.subscription.api.consumer;

import org.apache.iotdb.subscription.api.exception.SubscriptionException;

import java.util.List;

public interface Consumer extends AutoCloseable {

  /** Open the subscription. */
  void openSubscription() throws SubscriptionException;

  /** Close the subscription. */
  void closeSubscription() throws SubscriptionException;

  /**
   * Check if the subscription is closed.
   *
   * @return true if the subscription is closed, false otherwise
   */
  boolean isClosed();

  /**
   * Get the consumer group of the subscription.
   *
   * @return the consumer group
   * @throws SubscriptionException if the consumer group cannot be retrieved
   */
  String consumerGroup() throws SubscriptionException;

  /**
   * Get the topics of the subscription.
   *
   * @return the topics
   * @throws SubscriptionException if the topics cannot be retrieved
   */
  List<String> subscription() throws SubscriptionException;
}
