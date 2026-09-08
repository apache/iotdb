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

package org.apache.iotdb.confignode.procedure.env;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.confignode.client.sync.CnToDnSyncRequestType;
import org.apache.iotdb.confignode.manager.ConfigManager;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.Assert;
import org.junit.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

public class RegionMaintainHandlerTest {

  private static final int DATA_NODE_ID = 1;

  private final ConfigManager configManager = mock(ConfigManager.class);
  private final RegionMaintainHandler handler = spy(new RegionMaintainHandler(configManager));

  private final TDataNodeLocation dataNodeLocation =
      new TDataNodeLocation(
          DATA_NODE_ID,
          new TEndPoint("127.0.0.1", 2000),
          new TEndPoint("127.0.0.1", 2001),
          new TEndPoint("127.0.0.1", 2002),
          new TEndPoint("127.0.0.1", 2003),
          new TEndPoint("127.0.0.1", 2004));

  private final TConsensusGroupId regionId =
      new TConsensusGroupId(TConsensusGroupType.DataRegion, 1);

  private void stubStatusAndSubmit(final NodeStatus status, final TSStatus submitResult) {
    doReturn(status).when(handler).getDataNodeStatus(DATA_NODE_ID);
    doReturn(submitResult)
        .when(handler)
        .submitDataNodeSyncRequest(
            any(TEndPoint.class),
            any(Object.class),
            any(CnToDnSyncRequestType.class),
            any(Boolean.class));
  }

  @Test
  public void testDeleteOldRegionPeerUsesSingleRetryForStoppedDataNode() {
    final TSStatus success = RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS);
    stubStatusAndSubmit(NodeStatus.Stopped, success);

    final TSStatus status = handler.submitDeleteOldRegionPeerTask(1L, dataNodeLocation, regionId);

    Assert.assertEquals(success.getCode(), status.getCode());
    // A Stopped node does not respond to requests either: like Unknown, it gets a single retry.
    verify(handler)
        .submitDataNodeSyncRequest(
            any(TEndPoint.class),
            any(Object.class),
            eq(CnToDnSyncRequestType.DELETE_OLD_REGION_PEER),
            eq(false));
  }

  @Test
  public void testDeleteOldRegionPeerUsesFullRetryForRunningDataNode() {
    final TSStatus success = RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS);
    stubStatusAndSubmit(NodeStatus.Running, success);

    handler.submitDeleteOldRegionPeerTask(1L, dataNodeLocation, regionId);

    verify(handler)
        .submitDataNodeSyncRequest(
            any(TEndPoint.class),
            any(Object.class),
            eq(CnToDnSyncRequestType.DELETE_OLD_REGION_PEER),
            eq(true));
  }

  @Test
  public void testDeleteOldRegionPeerUsesFullRetryForReadOnlyDataNode() {
    final TSStatus success = RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS);
    stubStatusAndSubmit(NodeStatus.ReadOnly, success);

    handler.submitDeleteOldRegionPeerTask(1L, dataNodeLocation, regionId);

    // Only Unknown and Stopped are treated as down; ReadOnly still responds.
    verify(handler)
        .submitDataNodeSyncRequest(
            any(TEndPoint.class),
            any(Object.class),
            eq(CnToDnSyncRequestType.DELETE_OLD_REGION_PEER),
            eq(true));
  }
}
