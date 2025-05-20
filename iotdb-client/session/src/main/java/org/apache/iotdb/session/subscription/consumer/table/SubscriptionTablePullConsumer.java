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

package org.apache.iotdb.session.subscription.consumer.table;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.rpc.subscription.exception.SubscriptionException;
import org.apache.iotdb.session.subscription.consumer.AsyncCommitCallback;
import org.apache.iotdb.session.subscription.consumer.ISubscriptionTablePullConsumer;
import org.apache.iotdb.session.subscription.consumer.base.AbstractSubscriptionProvider;
import org.apache.iotdb.session.subscription.consumer.base.AbstractSubscriptionPullConsumer;
import org.apache.iotdb.session.subscription.payload.SubscriptionMessage;

import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

public class SubscriptionTablePullConsumer extends AbstractSubscriptionPullConsumer
    implements ISubscriptionTablePullConsumer {

  /////////////////////////////// provider ///////////////////////////////

  @Override
  protected AbstractSubscriptionProvider constructSubscriptionProvider(
      final TEndPoint endPoint,
      final String username,
      final String password,
      final String consumerId,
      final String consumerGroupId,
      final int thriftMaxFrameSize) {
    return new SubscriptionTableProvider(
        endPoint, username, password, consumerId, consumerGroupId, thriftMaxFrameSize);
  }

  /////////////////////////////// ctor ///////////////////////////////

  protected SubscriptionTablePullConsumer(final SubscriptionTablePullConsumerBuilder builder) {
    super(builder);
  }

  /////////////////////////////// interface ///////////////////////////////

  @Override
  public void open() throws SubscriptionException {
    super.open();
  }

  @Override
  public void close() throws SubscriptionException {
    super.close();
  }

  @Override
  public List<SubscriptionMessage> poll(final Duration timeout) throws SubscriptionException {
    return super.poll(timeout);
  }

  @Override
  public List<SubscriptionMessage> poll(final long timeoutMs) throws SubscriptionException {
    return super.poll(timeoutMs);
  }

  @Override
  public List<SubscriptionMessage> poll(final Set<String> topicNames, final Duration timeout)
      throws SubscriptionException {
    return super.poll(topicNames, timeout);
  }

  @Override
  public List<SubscriptionMessage> poll(final Set<String> topicNames, final long timeoutMs) {
    return super.poll(topicNames, timeoutMs);
  }

  @Override
  public void subscribe(final String topicName) throws SubscriptionException {
    super.subscribe(topicName);
  }

  @Override
  public void subscribe(final String... topicNames) throws SubscriptionException {
    super.subscribe(topicNames);
  }

  @Override
  public void subscribe(final Set<String> topicNames) throws SubscriptionException {
    super.subscribe(topicNames);
  }

  @Override
  public void unsubscribe(final String topicName) throws SubscriptionException {
    super.unsubscribe(topicName);
  }

  @Override
  public void unsubscribe(final String... topicNames) throws SubscriptionException {
    super.unsubscribe(topicNames);
  }

  @Override
  public void unsubscribe(final Set<String> topicNames) throws SubscriptionException {
    super.unsubscribe(topicNames);
  }

  @Override
  public void commitSync(final SubscriptionMessage message) throws SubscriptionException {
    super.commitSync(message);
  }

  @Override
  public void commitSync(final Iterable<SubscriptionMessage> messages)
      throws SubscriptionException {
    super.commitSync(messages);
  }

  @Override
  public CompletableFuture<Void> commitAsync(final SubscriptionMessage message) {
    return super.commitAsync(message);
  }

  @Override
  public CompletableFuture<Void> commitAsync(final Iterable<SubscriptionMessage> messages) {
    return super.commitAsync(messages);
  }

  @Override
  public void commitAsync(final SubscriptionMessage message, final AsyncCommitCallback callback) {
    super.commitAsync(message, callback);
  }

  @Override
  public void commitAsync(
      final Iterable<SubscriptionMessage> messages, final AsyncCommitCallback callback) {
    super.commitAsync(messages, callback);
  }

  @Override
  public String getConsumerId() {
    return super.getConsumerId();
  }

  @Override
  public String getConsumerGroupId() {
    return super.getConsumerGroupId();
  }

  @Override
  public boolean allTopicMessagesHaveBeenConsumed() {
    return super.allTopicMessagesHaveBeenConsumed();
  }
}
