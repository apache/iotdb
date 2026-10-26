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
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.client.sync.SyncDataNodeMPPDataExchangeServiceClient;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.mpp.rpc.thrift.TFetchDeviceEntrySegmentResp;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.thrift.TException;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class DeviceEntryRpcSegmentFetcherTest {

  private IClientManager<TEndPoint, SyncDataNodeMPPDataExchangeServiceClient> clientManager;
  private SyncDataNodeMPPDataExchangeServiceClient client;
  private DeviceEntryRpcSegmentFetcher fetcher;
  private DeviceEntryDataSetHandle handle;

  @Before
  @SuppressWarnings("unchecked")
  public void setUp() throws Exception {
    clientManager = Mockito.mock(IClientManager.class);
    client = Mockito.mock(SyncDataNodeMPPDataExchangeServiceClient.class);
    when(clientManager.borrowClient(any())).thenReturn(client);
    fetcher = new DeviceEntryRpcSegmentFetcher(clientManager);
    handle =
        new DeviceEntryDataSetHandle(
            "query", new PlanNodeId("scan"), new TEndPoint("127.0.0.1", 10740), 1, 1, false);
  }

  @Test
  public void testFetchRetriesNetworkFailure() throws Exception {
    byte[] payload = new byte[] {1, 2, 3};
    when(client.fetchDeviceEntrySegment(any()))
        .thenThrow(new TException())
        .thenThrow(new TException())
        .thenReturn(
            new TFetchDeviceEntrySegmentResp(
                    new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()))
                .setPayload(payload));

    assertArrayEquals(payload, fetcher.fetch(handle, 0));
    verify(client, times(3)).fetchDeviceEntrySegment(any());
  }

  @Test
  public void testFetchDoesNotRetryServerFailure() throws Exception {
    when(client.fetchDeviceEntrySegment(any()))
        .thenReturn(
            new TFetchDeviceEntrySegmentResp(
                new TSStatus(TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode())));

    assertThrows(IOException.class, () -> fetcher.fetch(handle, 0));
    verify(client, times(1)).fetchDeviceEntrySegment(any());
  }

  @Test
  public void testFinishRetriesNetworkFailureAtMostThreeTimes() throws Exception {
    when(client.finishDeviceEntrySegment(any(), any())).thenThrow(new TException());

    fetcher.finish(handle);

    verify(client, times(3)).finishDeviceEntrySegment(any(), any());
  }

  @Test
  public void testFinishDoesNotRetryServerFailure() throws Exception {
    when(client.finishDeviceEntrySegment(any(), any()))
        .thenReturn(new TSStatus(TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode()));

    fetcher.finish(handle);

    verify(client, times(1)).finishDeviceEntrySegment(any(), any());
  }
}
