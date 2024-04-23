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
import java.util.Objects;

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

    final Map<Integer, TEndPoint> allEndPoints;
    try {
      allEndPoints = consumer.fetchAllEndPointsWithRedirection();
    } catch (final Exception e) {
      LOGGER.warn("Failed to fetch all endpoints, will retry later...", e);
      return; // retry later
    }

    // add new providers or handshake existing providers
    for (final Map.Entry<Integer, TEndPoint> entry : allEndPoints.entrySet()) {
      final SubscriptionProvider provider = consumer.getProvider(entry.getKey());
      if (Objects.isNull(provider)) {
        // new provider
        final SubscriptionProvider newProvider = consumer.constructProvider(entry.getValue());
        try {
          newProvider.handshake();
        } catch (final Exception e) {
          LOGGER.warn(
              "Failed to create connection with subscription provider {}, will retry later...",
              newProvider,
              e);
          continue; // retry later
        }
        consumer.addProvider(entry.getKey(), newProvider);
      } else {
        // existing provider
        try {
          provider.heartbeat();
          provider.setAvailable();
        } catch (final Exception e) {
          LOGGER.warn(
              "something unexpected happened when sending heartbeat to subscription provider {}, set subscription provider unavailable",
              provider,
              e);
          provider.setUnavailable();
        }
        // close and remove unavailable provider (reset the connection as much as possible)
        if (!provider.isAvailable()) {
          try {
            consumer.closeAndRemoveProvider(entry.getKey());
          } catch (final IoTDBConnectionException e) {
            LOGGER.warn(
                "Exception occurred when closing and removing subscription provider with data node id {}",
                entry.getKey(),
                e);
          }
        }
      }
    }

    // close and remove stale providers
    for (final SubscriptionProvider provider : consumer.getAllProviders()) {
      final int dataNodeId = provider.getDataNodeId();
      if (!allEndPoints.containsKey(dataNodeId)) {
        try {
          consumer.closeAndRemoveProvider(dataNodeId);
        } catch (final IoTDBConnectionException e) {
          LOGGER.warn(
              "Exception occurred when closing and removing subscription provider with data node id {}",
              dataNodeId,
              e);
        }
      }
    }
  }
}
