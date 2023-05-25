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

package org.apache.iotdb.subscription.api.consumer.push;

import org.apache.iotdb.subscription.api.consumer.Consumer;
import org.apache.iotdb.subscription.api.exception.SubscriptionException;

public interface PushConsumer extends Consumer {

  /**
   * Register a listener to listen to the data arrival. The method should be called before the
   * consumer is started. The DataArrivalListener can NOT be changed once the consumer is started.
   *
   * @param listener the listener to listen to the data arrival.
   * @throws SubscriptionException if the listener cannot be registered. Mainly because the
   *     PushConsumer is running.
   */
  void registerDataArrivalListener(DataArrivalListener listener) throws SubscriptionException;

  /**
   * Register a listener to listen to the exception. The method should be called before the consumer
   * is started. The ExceptionListener can NOT be changed once the consumer is started.
   *
   * @param listener the listener to listen to the exception.
   * @throws SubscriptionException if the listener cannot be registered. Mainly because the
   *     PushConsumer is running.
   */
  void registerExceptionListener(ExceptionListener listener) throws SubscriptionException;

  /**
   * Start the consumer to listen to the data. If the consumer is already listening, do nothing.
   *
   * @throws SubscriptionException if the consumer cannot start, e.g. the DataArrivalListener is not
   *     registered or the ExceptionListener is not registered.
   */
  void start() throws SubscriptionException;

  /**
   * Stop the consumer to listen to the data. If the consumer is not listening, do nothing.
   *
   * @throws SubscriptionException if the consumer cannot stop.
   */
  void stop() throws SubscriptionException;

  /**
   * Check if the consumer is listening to the data.
   *
   * @return true if the consumer is listening to the data, false otherwise.
   */
  boolean isListening();
}
