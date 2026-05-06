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

package org.apache.iotdb.confignode.manager.node;

import org.apache.iotdb.common.rpc.thrift.TConfigNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TDataNodeConfiguration;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.cluster.NodeType;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlan;
import org.apache.iotdb.confignode.consensus.request.write.confignode.ApplyConfigNodePlan;
import org.apache.iotdb.confignode.consensus.request.write.confignode.RemoveConfigNodePlan;
import org.apache.iotdb.confignode.consensus.request.write.confignode.UpdateVersionInfoPlan;
import org.apache.iotdb.confignode.consensus.request.write.datanode.RegisterDataNodePlan;
import org.apache.iotdb.confignode.consensus.request.write.datanode.RemoveDataNodePlan;
import org.apache.iotdb.confignode.consensus.response.datanode.DataNodeRegisterResp;
import org.apache.iotdb.confignode.manager.ClusterManager;
import org.apache.iotdb.confignode.manager.IManager;
import org.apache.iotdb.confignode.manager.consensus.ConsensusManager;
import org.apache.iotdb.confignode.manager.load.LoadManager;
import org.apache.iotdb.confignode.manager.load.cache.LoadCache;
import org.apache.iotdb.confignode.manager.schema.ClusterSchemaManager;
import org.apache.iotdb.confignode.persistence.node.NodeInfo;
import org.apache.iotdb.confignode.rpc.thrift.TDataNodeRegisterReq;
import org.apache.iotdb.confignode.rpc.thrift.TDataNodeRestartReq;
import org.apache.iotdb.confignode.rpc.thrift.TDataNodeRestartResp;
import org.apache.iotdb.confignode.rpc.thrift.TNodeVersionInfo;
import org.apache.iotdb.consensus.exception.ConsensusException;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class NodeManagerTest {

  private IManager configManager;
  private ConsensusManager consensusManager;
  private LoadManager loadManager;
  private LoadCache loadCache;
  private ClusterSchemaManager clusterSchemaManager;
  private ClusterManager clusterManager;
  private NodeInfo nodeInfo;
  private NodeManager nodeManager;

  @Before
  public void setUp() {
    configManager = Mockito.mock(IManager.class);
    consensusManager = Mockito.mock(ConsensusManager.class);
    loadManager = Mockito.mock(LoadManager.class);
    loadCache = Mockito.mock(LoadCache.class);
    clusterSchemaManager = Mockito.mock(ClusterSchemaManager.class);
    clusterManager = Mockito.mock(ClusterManager.class);
    nodeInfo = new NodeInfo();
    nodeManager = new NodeManager(configManager, nodeInfo);

    when(configManager.getConsensusManager()).thenReturn(consensusManager);
    when(configManager.getLoadManager()).thenReturn(loadManager);
    when(loadManager.getLoadCache()).thenReturn(loadCache);
    when(configManager.getClusterSchemaManager()).thenReturn(clusterSchemaManager);
    when(configManager.getClusterManager()).thenReturn(clusterManager);
  }

  @Test
  public void testRegisterDataNodeStopsWhenRegisterWriteFails() throws ConsensusException {
    TSStatus failureStatus =
        new TSStatus(TSStatusCode.REDIRECTION_RECOMMEND.getStatusCode()).setMessage("redirect");
    when(consensusManager.write(any())).thenReturn(failureStatus);

    DataNodeRegisterResp resp =
        (DataNodeRegisterResp) nodeManager.registerDataNode(generateDataNodeRegisterReq(1));

    Assert.assertEquals(failureStatus, resp.getStatus());
    verify(loadCache, never()).createNodeHeartbeatCache(eq(NodeType.DataNode), anyInt());
    verify(clusterSchemaManager, never()).adjustMaxRegionGroupNum();
  }

  @Test
  public void testRegisterDataNodeRollsBackWhenVersionWriteFails() throws ConsensusException {
    TSStatus successStatus = new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    TSStatus failureStatus =
        new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode())
            .setMessage("update failed");
    when(consensusManager.write(any())).thenReturn(successStatus, failureStatus, successStatus);

    DataNodeRegisterResp resp =
        (DataNodeRegisterResp) nodeManager.registerDataNode(generateDataNodeRegisterReq(1));

    Assert.assertEquals(
        TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode(), resp.getStatus().getCode());
    Assert.assertTrue(resp.getStatus().getMessage().contains("rolled back"));
    verify(loadCache, never()).createNodeHeartbeatCache(eq(NodeType.DataNode), anyInt());
    verify(clusterSchemaManager, never()).adjustMaxRegionGroupNum();

    ArgumentCaptor<ConfigPhysicalPlan> planCaptor =
        ArgumentCaptor.forClass(ConfigPhysicalPlan.class);
    verify(consensusManager, Mockito.times(3)).write(planCaptor.capture());
    Assert.assertTrue(planCaptor.getAllValues().get(0) instanceof RegisterDataNodePlan);
    Assert.assertTrue(planCaptor.getAllValues().get(1) instanceof UpdateVersionInfoPlan);
    Assert.assertTrue(planCaptor.getAllValues().get(2) instanceof RemoveDataNodePlan);
  }

  @Test
  public void testRestartDataNodeReturnsFailureWhenUpdateWriteFails() throws ConsensusException {
    final TDataNodeConfiguration registeredConfig = generateDataNodeConfiguration(1, "127.0.0.1");
    nodeInfo.registerDataNode(new RegisterDataNodePlan(registeredConfig));
    when(clusterManager.getClusterIdWithRetry(anyLong())).thenReturn("cluster");

    TSStatus failureStatus =
        new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode())
            .setMessage("update failed");
    when(consensusManager.write(any())).thenReturn(failureStatus);

    final TDataNodeRestartReq req = new TDataNodeRestartReq();
    req.setDataNodeConfiguration(generateDataNodeConfiguration(1, "127.0.0.2"));
    req.setVersionInfo(new TNodeVersionInfo("version", "build"));

    final TDataNodeRestartResp resp = nodeManager.updateDataNodeIfNecessary(req);

    Assert.assertEquals(failureStatus, resp.getStatus());
  }

  @Test
  public void testApplyConfigNodeRollsBackWhenVersionWriteFails() throws ConsensusException {
    TSStatus successStatus = new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    TSStatus failureStatus =
        new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode())
            .setMessage("apply failed");
    when(consensusManager.write(any())).thenReturn(successStatus, failureStatus, successStatus);

    try {
      nodeManager.applyConfigNode(
          new TConfigNodeLocation(
              1, new TEndPoint("127.0.0.1", 10710), new TEndPoint("127.0.0.1", 10720)),
          new TNodeVersionInfo("version", "build"));
      Assert.fail("Expected applyConfigNode to fail fast");
    } catch (IllegalStateException e) {
      Assert.assertTrue(e.getMessage().contains("rolled back"));
    }

    ArgumentCaptor<ConfigPhysicalPlan> planCaptor =
        ArgumentCaptor.forClass(ConfigPhysicalPlan.class);
    verify(consensusManager, Mockito.times(3)).write(planCaptor.capture());
    Assert.assertTrue(planCaptor.getAllValues().get(0) instanceof ApplyConfigNodePlan);
    Assert.assertTrue(planCaptor.getAllValues().get(1) instanceof UpdateVersionInfoPlan);
    Assert.assertTrue(planCaptor.getAllValues().get(2) instanceof RemoveConfigNodePlan);
  }

  private TDataNodeRegisterReq generateDataNodeRegisterReq(int dataNodeId) {
    final TDataNodeRegisterReq req = new TDataNodeRegisterReq();
    req.setDataNodeConfiguration(generateDataNodeConfiguration(dataNodeId, "127.0.0.1"));
    req.setVersionInfo(new TNodeVersionInfo("version", "build"));
    return req;
  }

  private TDataNodeConfiguration generateDataNodeConfiguration(int dataNodeId, String ip) {
    final TDataNodeLocation dataNodeLocation = new TDataNodeLocation();
    dataNodeLocation.setDataNodeId(dataNodeId);
    dataNodeLocation.setClientRpcEndPoint(new TEndPoint(ip, 6667 + dataNodeId));
    dataNodeLocation.setInternalEndPoint(new TEndPoint(ip, 10730 + dataNodeId));
    dataNodeLocation.setMPPDataExchangeEndPoint(new TEndPoint(ip, 10740 + dataNodeId));
    dataNodeLocation.setDataRegionConsensusEndPoint(new TEndPoint(ip, 10760 + dataNodeId));
    dataNodeLocation.setSchemaRegionConsensusEndPoint(new TEndPoint(ip, 10750 + dataNodeId));

    final TDataNodeConfiguration dataNodeConfiguration = new TDataNodeConfiguration();
    dataNodeConfiguration.setLocation(dataNodeLocation);
    return dataNodeConfiguration;
  }
}
