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
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.pipe.connector.client.IoTDBSyncClient;
import org.apache.iotdb.commons.pipe.connector.client.IoTDBSyncClientManager;
import org.apache.iotdb.commons.pipe.connector.payload.thrift.request.PipeTransferHandshakeV2Req;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.pipe.connector.payload.evolvable.request.PipeTransferDataNodeHandshakeV1Req;
import org.apache.iotdb.db.pipe.connector.payload.evolvable.request.PipeTransferDataNodeHandshakeV2Req;

import org.apache.tsfile.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class IoTDBDataNodeSyncClientManager extends IoTDBSyncClientManager
    implements IoTDBDataNodeCacheLeaderClientManager {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(IoTDBDataNodeSyncClientManager.class);

  public IoTDBDataNodeSyncClientManager(
      final List<TEndPoint> endPoints,
      final boolean useSSL,
      final String trustStorePath,
      final String trustStorePwd,
      /* The following parameters are used locally. */
      final boolean useLeaderCache,
      final String loadBalanceStrategy,
      /* The following parameters are used to handshake with the receiver. */
      final String username,
      final String password,
      final boolean shouldReceiverConvertOnTypeMismatch,
      final String loadTsFileStrategy) {
    super(
        endPoints,
        useSSL,
        trustStorePath,
        trustStorePwd,
        useLeaderCache,
        loadBalanceStrategy,
        username,
        password,
        shouldReceiverConvertOnTypeMismatch,
        loadTsFileStrategy);
  }

  @Override
  protected PipeTransferDataNodeHandshakeV1Req buildHandshakeV1Req() throws IOException {
    return PipeTransferDataNodeHandshakeV1Req.toTPipeTransferReq(
        CommonDescriptor.getInstance().getConfig().getTimestampPrecision());
  }

  @Override
  protected PipeTransferHandshakeV2Req buildHandshakeV2Req(final Map<String, String> params)
      throws IOException {
    return PipeTransferDataNodeHandshakeV2Req.toTPipeTransferReq(params);
  }

  @Override
  protected String getClusterId() {
    return IoTDBDescriptor.getInstance().getConfig().getClusterId();
  }

  public Pair<IoTDBSyncClient, Boolean> getClient(final String deviceId) {
    final TEndPoint endPoint = LEADER_CACHE_MANAGER.getLeaderEndPoint(deviceId);
    return useLeaderCache
            && endPoint != null
            && endPoint2ClientAndStatus.containsKey(endPoint)
            && Boolean.TRUE.equals(endPoint2ClientAndStatus.get(endPoint).getRight())
        ? endPoint2ClientAndStatus.get(endPoint)
        : getClient();
  }

  public Pair<IoTDBSyncClient, Boolean> getClient(final TEndPoint endPoint) {
    return useLeaderCache
            && endPoint != null
            && endPoint2ClientAndStatus.containsKey(endPoint)
            && Boolean.TRUE.equals(endPoint2ClientAndStatus.get(endPoint).getRight())
        ? endPoint2ClientAndStatus.get(endPoint)
        : getClient();
  }

  public void updateLeaderCache(final String deviceId, final TEndPoint endPoint) {
    if (!useLeaderCache || deviceId == null || endPoint == null) {
      return;
    }

    try {
      if (!endPoint2ClientAndStatus.containsKey(endPoint)) {
        endPointList.add(endPoint);
        endPoint2ClientAndStatus.put(endPoint, new Pair<>(null, false));
        reconstructClient(endPoint);
      }

      LEADER_CACHE_MANAGER.updateLeaderEndPoint(deviceId, endPoint);
    } catch (final Exception e) {
      LOGGER.warn(
          "Failed to update leader cache for device {} with endpoint {}:{}.",
          deviceId,
          endPoint.getIp(),
          endPoint.getPort(),
          e);
    }
  }
}
