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

package org.apache.iotdb.db.queryengine.plan.relational.metadata.spill;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.client.exception.ClientManagerException;
import org.apache.iotdb.commons.client.sync.SyncDataNodeMPPDataExchangeServiceClient;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.exception.query.DeviceEntrySpillNotFoundException;
import org.apache.iotdb.db.queryengine.execution.exchange.MPPDataExchangeService;
import org.apache.iotdb.mpp.rpc.thrift.TFetchDeviceEntrySegmentReq;
import org.apache.iotdb.mpp.rpc.thrift.TFetchDeviceEntrySegmentResp;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.thrift.TException;

import java.io.IOException;

public final class DeviceEntryRpcSegmentFetcher implements DeviceEntrySegmentFetcher {

  private static final int MAX_ATTEMPTS = 3;

  private final IClientManager<TEndPoint, SyncDataNodeMPPDataExchangeServiceClient> clientManager;

  private DeviceEntryRpcSegmentFetcher() {
    this(
        MPPDataExchangeService.getInstance()
            .getMPPDataExchangeManager()
            .getMppDataExchangeServiceClientManager());
  }

  @TestOnly
  public DeviceEntryRpcSegmentFetcher(
      IClientManager<TEndPoint, SyncDataNodeMPPDataExchangeServiceClient> clientManager) {
    this.clientManager = clientManager;
  }

  public static DeviceEntryRpcSegmentFetcher getInstance() {
    return DeviceEntryRpcSegmentFetcherHolder.INSTANCE;
  }

  @Override
  public byte[] fetch(DeviceEntryDataSetHandle handle, int segmentId) throws IOException {
    IOException failure = null;
    for (int attempt = 0; attempt < MAX_ATTEMPTS; attempt++) {
      TFetchDeviceEntrySegmentResp response;
      try {
        try (SyncDataNodeMPPDataExchangeServiceClient client =
            clientManager.borrowClient(handle.getCoordinatorMppDataExchangeEndPoint())) {
          response = client.fetchDeviceEntrySegment(createFetchRequest(handle, segmentId));
        }
      } catch (ClientManagerException | TException e) {
        failure = new IOException(e);
        continue;
      }
      if (response.getStatus().getCode() == TSStatusCode.QUERY_WAS_KILLED.getStatusCode()) {
        throw new DeviceEntrySpillNotFoundException(response.getStatus());
      }
      if (response.getStatus().getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        throw new IOException(response.getStatus().getMessage());
      }
      return response.getPayload();
    }
    throw failure;
  }

  @Override
  public void finish(DeviceEntryDataSetHandle handle) {
    for (int attempt = 0; attempt < MAX_ATTEMPTS; attempt++) {
      try {
        try (SyncDataNodeMPPDataExchangeServiceClient client =
            clientManager.borrowClient(handle.getCoordinatorMppDataExchangeEndPoint())) {
          org.apache.iotdb.common.rpc.thrift.TSStatus status =
              client.finishDeviceEntrySegment(handle.getQueryId(), handle.getPlanNodeId().getId());
          if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
            return;
          }
        }
        return;
      } catch (ClientManagerException | TException e) {
        // Cleanup notification is best effort and must not fail the query.
      }
    }
  }

  private TFetchDeviceEntrySegmentReq createFetchRequest(
      DeviceEntryDataSetHandle handle, int segmentId) {
    return new TFetchDeviceEntrySegmentReq(
        handle.getQueryId(), handle.getPlanNodeId().getId(), segmentId);
  }

  private static class DeviceEntryRpcSegmentFetcherHolder {
    private static final DeviceEntryRpcSegmentFetcher INSTANCE = new DeviceEntryRpcSegmentFetcher();

    private DeviceEntryRpcSegmentFetcherHolder() {}
  }
}
