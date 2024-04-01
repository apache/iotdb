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

package org.apache.iotdb.session.subscription;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.rpc.IoTDBConnectionException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class SubscriptionEndpointsSyncer implements Runnable {

  private static final Logger LOGGER = LoggerFactory.getLogger(SubscriptionEndpointsSyncer.class);

  private final SubscriptionConsumer consumer;

  public SubscriptionEndpointsSyncer(SubscriptionConsumer consumer) {
    this.consumer = consumer;
  }

  @Override
  public void run() {
    if (consumer.isClosed()) {
      return;
    }

    consumer.acquireWriteLock();
    try {
      syncInternal();
    } finally {
      consumer.releaseWriteLock();
    }
  }

  private void syncInternal() {
    if (consumer.hasNoProviders()) {
      try {
        consumer.openProviders();
      } catch (final IoTDBConnectionException e) {
        LOGGER.warn("something unexpected happened when syncing subscription endpoints...", e);
        return;
      }
    }

    final SubscriptionSessionConnection sessionConnection;
    try {
      sessionConnection = consumer.getDefaultSessionConnection();
    } catch (final IoTDBConnectionException e) {
      LOGGER.warn("something unexpected happened when syncing subscription endpoints...", e);
      return;
    }

    final Map<Integer, TEndPoint> allEndPoints;
    try {
      allEndPoints = sessionConnection.fetchAllEndPoints();
    } catch (final Exception e) {
      LOGGER.warn(
          "Failed to fetch all endpoints, exception: {}, will retry later...", e.getMessage());
      return; // retry later
    }

    // open new providers
    for (final Map.Entry<Integer, TEndPoint> entry : allEndPoints.entrySet()) {
      if (!consumer.containsProvider(entry.getKey())) {
        final SubscriptionProvider subscriptionProvider;
        try {
          subscriptionProvider = consumer.constructProvider(entry.getValue());
          subscriptionProvider.handshake();
        } catch (final Exception e) {
          LOGGER.warn(
              "Failed to create connection with {}, exception: {}, will retry later...",
              entry.getValue(),
              e.getMessage());
          continue; // retry later
        }
        consumer.addProvider(entry.getKey(), subscriptionProvider);
      }
    }

    // remove stale providers
    for (final Integer dataNodeId : consumer.getAvailableDataNodeIds()) {
      if (!allEndPoints.containsKey(dataNodeId)) {
        try {
          consumer.closeAndRemoveProvider(dataNodeId);
        } catch (final IoTDBConnectionException e) {
          LOGGER.warn(
              "Failed to close and remove subscription provider with data node id {}, exception: {}",
              dataNodeId,
              e.getMessage());
        }
      }
    }
  }
}
