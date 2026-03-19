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
import org.apache.iotdb.session.subscription.payload.SubscriptionMessage;

import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

/**
 * A subscription-based pull consumer interface for receiving messages from specified topics of
 * table model.
 */
public interface ISubscriptionTablePullConsumer extends AutoCloseable {

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
   * Retrieves messages from subscribed topics, waiting up to the specified timeout.
   *
   * @param timeout the maximum duration to wait for messages
   * @return a list of received messages, which can be empty if no messages are available
   * @throws SubscriptionException if polling fails
   */
  List<SubscriptionMessage> poll(final Duration timeout) throws SubscriptionException;

  /**
   * Retrieves messages from subscribed topics, waiting up to the specified timeout in milliseconds.
   *
   * @param timeoutMs the maximum time in milliseconds to wait for messages
   * @return a list of received messages, which can be empty if no messages are available
   * @throws SubscriptionException if polling fails
   */
  List<SubscriptionMessage> poll(final long timeoutMs) throws SubscriptionException;

  /**
   * Retrieves messages from the given set of topics, waiting up to the specified timeout.
   *
   * @param topicNames the set of topics to poll
   * @param timeout the maximum duration to wait for messages
   * @return a list of received messages, which can be empty if no messages are available
   * @throws SubscriptionException if polling fails
   */
  List<SubscriptionMessage> poll(final Set<String> topicNames, final Duration timeout)
      throws SubscriptionException;

  /**
   * Retrieves messages from the given set of topics, waiting up to the specified timeout in
   * milliseconds.
   *
   * @param topicNames the set of topics to poll
   * @param timeoutMs the maximum time in milliseconds to wait for messages
   * @return a list of received messages, which can be empty if no messages are available
   */
  List<SubscriptionMessage> poll(final Set<String> topicNames, final long timeoutMs);

  /**
   * Commits a single message synchronously, indicating that it has been successfully processed.
   *
   * @param message the message to commit
   * @throws SubscriptionException if the commit fails
   */
  void commitSync(final SubscriptionMessage message) throws SubscriptionException;

  /**
   * Commits multiple messages synchronously, indicating that they have been successfully processed.
   *
   * @param messages an iterable collection of messages to commit
   * @throws SubscriptionException if the commit fails
   */
  void commitSync(final Iterable<SubscriptionMessage> messages) throws SubscriptionException;

  /**
   * Commits a single message asynchronously, indicating that it has been successfully processed.
   *
   * @param message the message to commit
   * @return a CompletableFuture that completes when the commit operation finishes
   */
  CompletableFuture<Void> commitAsync(final SubscriptionMessage message);

  /**
   * Commits multiple messages asynchronously, indicating that they have been successfully
   * processed.
   *
   * @param messages an iterable collection of messages to commit
   * @return a CompletableFuture that completes when the commit operation finishes
   */
  CompletableFuture<Void> commitAsync(final Iterable<SubscriptionMessage> messages);

  /**
   * Commits a single message asynchronously, indicating that it has been successfully processed,
   * and notifies the provided callback upon completion.
   *
   * @param message the message to commit
   * @param callback a callback to receive the completion notification
   */
  void commitAsync(final SubscriptionMessage message, final AsyncCommitCallback callback);

  /**
   * Commits multiple messages asynchronously, indicating that they have been successfully
   * processed, and notifies the provided callback upon completion.
   *
   * @param messages an iterable collection of messages to commit
   * @param callback a callback to receive the completion notification
   */
  void commitAsync(
      final Iterable<SubscriptionMessage> messages, final AsyncCommitCallback callback);

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

  /**
   * Checks whether all topic messages have been consumed.
   *
   * <p>This method is used by the pull consumer in a loop that retrieves messages to determine if
   * all messages for the subscription have been processed. It ensures that the consumer can
   * correctly detect the termination signal for the subscription once all messages have been
   * consumed.
   *
   * @return true if all topic messages have been consumed, false otherwise.
   */
  boolean allTopicMessagesHaveBeenConsumed();
}
