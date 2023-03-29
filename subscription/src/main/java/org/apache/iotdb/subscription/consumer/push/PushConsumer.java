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

package org.apache.iotdb.subscription.consumer.push;

import org.apache.iotdb.subscription.api.SubscriptionConfiguration;
import org.apache.iotdb.subscription.api.consumer.push.DataArrivalListener;
import org.apache.iotdb.subscription.api.consumer.push.ExceptionListener;
import org.apache.iotdb.subscription.api.consumer.push.IPushConsumer;
import org.apache.iotdb.subscription.api.exception.SubscriptionException;
import org.apache.iotdb.subscription.consumer.Consumer;
import org.apache.iotdb.subscription.rpc.thrift.TSubscriptionDataSet;
import org.apache.iotdb.subscription.service.thrift.impl.PushSubscriptionService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class PushConsumer extends Consumer implements IPushConsumer {

  private static final Logger logger = LoggerFactory.getLogger(PushConsumer.class);
  private DataArrivalListener dataArrivalListener;
  private ExceptionListener exceptionListener;

  public PushConsumer(SubscriptionConfiguration subscriptionConfiguration) {
    super(subscriptionConfiguration);
    subscriptionService =
        new PushSubscriptionService(
            subscriptionConfiguration.getLocalHost(), subscriptionConfiguration.getLocalPort());
    subscriptionService.registerConsumer(this);
  }

  /**
   * Register a listener to listen to the data arrival. The method should be called before the
   * consumer is started. The DataArrivalListener can NOT be changed once the consumer is started.
   *
   * @param listener the listener to listen to the data arrival.
   * @throws SubscriptionException if the listener cannot be registered. Mainly because the
   *     PushConsumer is running.
   */
  @Override
  public void registerDataArrivalListener(DataArrivalListener listener)
      throws SubscriptionException {
    if (!this.isClosed()) {
      throw new SubscriptionException(
          "the listener cannot be registered.because th PushConsumer is running.");
    }
    if (this.dataArrivalListener != null) {
      throw new SubscriptionException("the listener cannot be repeat registered");
    }
    this.dataArrivalListener = listener;
  }

  /**
   * Register a listener to listen to the exception. The method should be called before the consumer
   * is started. The ExceptionListener can NOT be changed once the consumer is started.
   *
   * @param listener the listener to listen to the exception.
   * @throws SubscriptionException if the listener cannot be registered. Mainly because the
   *     PushConsumer is running.
   */
  @Override
  public void registerExceptionListener(ExceptionListener listener) throws SubscriptionException {
    if (!this.isClosed()) {
      throw new SubscriptionException(
          "the listener cannot be registered.because th PushConsumer is running.");
    }
    if (this.exceptionListener != null) {
      throw new SubscriptionException("the listener cannot be repeat registered");
    }
    this.exceptionListener = listener;
  }

  /**
   * Start the consumer to listen to the data. If the consumer is already listening, do nothing.
   *
   * @throws SubscriptionException if the consumer cannot start, e.g. the DataArrivalListener is not
   *     registered or the ExceptionListener is not registered.
   */
  @Override
  public void start() throws SubscriptionException {
    if (this.dataArrivalListener == null) {
      throw new SubscriptionException(
          "the consumer cannot start, because the DataArrivalListener is not registered.");
    }
    if (this.exceptionListener == null) {
      logger.warn("the ExceptionListener is not registered.");
    }
  }

  /**
   * Stop the consumer to listen to the data. If the consumer is not listening, do nothing.
   *
   * @throws SubscriptionException if the consumer cannot stop.
   */
  @Override
  public void stop() throws SubscriptionException {}

  /**
   * Check if the consumer is listening to the data.
   *
   * @return true if the consumer is listening to the data, false otherwise.
   */
  @Override
  public boolean isListening() {
    return this.dataArrivalListener != null;
  }

  /**
   * Process received data, Filter data based on watermark
   *
   * @param subscriptionDataSets Received source data
   */
  public void handleDataArrival(List<TSubscriptionDataSet> subscriptionDataSets) {
    if (subscriptionDataSets != null && subscriptionDataSets.size() != 0) {
      if (this.dataArrivalListener != null) {
        try {
          this.dataArrivalListener.onDataArrival(super.handleDataOnWatermark(subscriptionDataSets));
        } catch (SubscriptionException e) {
          handleException(e);
        }
      }
    }
  }

  /**
   * Exception occurred while processing data. 1. print log, 2. push to exceptionListener, if unset
   * exceptionListener, do nothing.
   *
   * @param e Exception for handleDataArrival
   */
  public void handleException(SubscriptionException e) {
    logger.error("error occurred while processing data.", e);
    if (this.exceptionListener == null) {
      logger.warn("the ExceptionListener is not registered.");
      return;
    }
    exceptionListener.onException(e);
  }
}
