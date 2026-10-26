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

package org.apache.iotdb.session.subscription.consumer;

import org.apache.iotdb.rpc.subscription.exception.SubscriptionException;

import java.util.Set;

/**
 * A subscription-based push consumer interface for receiving messages from specified topics of tree
 * model.
 */
public interface ISubscriptionTreePushConsumer extends AutoCloseable {

  /**
   * Opens this consumer to begin receiving messages.
   *
   * @throws SubscriptionException if the consumer fails to open
   */
  void open() throws SubscriptionException;

  /**
   * Subscribes to the specified topic.
   *
   * @param topicName the name of the topic to subscribe to
   * @throws SubscriptionException if the subscription cannot be established
   */
  void subscribe(final String topicName) throws SubscriptionException;

  /**
   * Subscribes to multiple topics.
   *
   * @param topicNames one or more topic names to subscribe to
   * @throws SubscriptionException if the subscription cannot be established
   */
  void subscribe(final String... topicNames) throws SubscriptionException;

  /**
   * Subscribes to multiple topics.
   *
   * @param topicNames a set of topic names to subscribe to
   * @throws SubscriptionException if the subscription cannot be established
   */
  void subscribe(final Set<String> topicNames) throws SubscriptionException;

  /**
   * Unsubscribes from the specified topic.
   *
   * @param topicName the name of the topic to unsubscribe from
   * @throws SubscriptionException if the unsubscription fails
   */
  void unsubscribe(final String topicName) throws SubscriptionException;

  /**
   * Unsubscribes from multiple topics.
   *
   * @param topicNames one or more topic names to unsubscribe from
   * @throws SubscriptionException if the unsubscription fails
   */
  void unsubscribe(final String... topicNames) throws SubscriptionException;

  /**
   * Unsubscribes from multiple topics.
   *
   * @param topicNames a set of topic names to unsubscribe from
   * @throws SubscriptionException if the unsubscription fails
   */
  void unsubscribe(final Set<String> topicNames) throws SubscriptionException;

  /**
   * Retrieves the unique identifier of this consumer. If no consumer ID was provided at the time of
   * consumer construction, a random globally unique ID is automatically assigned after the consumer
   * is opened.
   *
   * @return the unique consumer identifier
   */
  String getConsumerId();

  /**
   * Retrieves the identifier of the consumer group to which this consumer belongs. If no consumer
   * group ID was specified at the time of consumer construction, a random globally unique ID is
   * automatically assigned after the consumer is opened.
   *
   * @return the consumer group's identifier
   */
  String getConsumerGroupId();
}
