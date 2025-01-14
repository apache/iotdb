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

public interface ISubscriptionTablePullConsumer {

  void open() throws SubscriptionException;

  void close() throws SubscriptionException;

  List<SubscriptionMessage> poll(final Duration timeout) throws SubscriptionException;

  List<SubscriptionMessage> poll(final long timeoutMs) throws SubscriptionException;

  List<SubscriptionMessage> poll(final Set<String> topicNames, final Duration timeout)
      throws SubscriptionException;

  List<SubscriptionMessage> poll(final Set<String> topicNames, final long timeoutMs);

  void commitSync(final SubscriptionMessage message) throws SubscriptionException;

  void commitSync(final Iterable<SubscriptionMessage> messages) throws SubscriptionException;

  CompletableFuture<Void> commitAsync(final SubscriptionMessage message);

  CompletableFuture<Void> commitAsync(final Iterable<SubscriptionMessage> messages);

  void commitAsync(final SubscriptionMessage message, final AsyncCommitCallback callback);

  void commitAsync(
      final Iterable<SubscriptionMessage> messages, final AsyncCommitCallback callback);
}
