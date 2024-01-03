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

package org.apache.iotdb.db.pipe.connector.client;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.pipe.connector.client.IoTDBThriftSyncClientManager;
import org.apache.iotdb.commons.pipe.connector.client.IoTDBThriftSyncConnectorClient;
import org.apache.iotdb.tsfile.utils.Pair;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public abstract class IoTDBThriftSyncLeaderCacheClientManager extends IoTDBThriftSyncClientManager {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(IoTDBThriftSyncLeaderCacheClientManager.class);
  private final LeaderCacheManager leaderCacheManager = new LeaderCacheManager();
  private final boolean useLeaderCache;

  protected IoTDBThriftSyncLeaderCacheClientManager(
      List<TEndPoint> endPoints,
      boolean useSSL,
      String trustStorePath,
      String trustStorePwd,
      boolean useLeaderCache) {
    super(endPoints, useSSL, trustStorePath, trustStorePwd);
    this.useLeaderCache = useLeaderCache;
  }

  public Pair<IoTDBThriftSyncConnectorClient, Boolean> getClient(String deviceId) {
    final TEndPoint endPoint = leaderCacheManager.getLeaderEndPoint(deviceId);
    return useLeaderCache
            && endPoint != null
            && endPoint2ClientAndStatus.containsKey(endPoint)
            && Boolean.TRUE.equals(endPoint2ClientAndStatus.get(endPoint).getRight())
        ? endPoint2ClientAndStatus.get(endPoint)
        : getClient();
  }

  public void updateLeaderCache(String deviceId, TEndPoint endPoint) {
    if (!useLeaderCache) {
      return;
    }

    try {
      if (!endPoint2ClientAndStatus.containsKey(endPoint)) {
        endPointList.add(endPoint);
        endPoint2ClientAndStatus.put(endPoint, new Pair<>(null, false));
        reconstructClient(endPoint);
      }

      leaderCacheManager.updateLeaderEndPoint(deviceId, endPoint);
    } catch (Exception e) {
      LOGGER.warn(
          "Failed to update leader cache for device {} with endpoint {}:{}.",
          deviceId,
          endPoint.getIp(),
          endPoint.getPort(),
          e);
    }
  }
}
