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

package org.apache.iotdb.db.protocol.thrift.impl;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.service.DataNode.DataNodeContext;
import org.apache.iotdb.mpp.rpc.thrift.TCreateDataRegionReq;
import org.apache.iotdb.mpp.rpc.thrift.TCreateSchemaRegionReq;
import org.apache.iotdb.mpp.rpc.thrift.TRegionLeaderChangeReq;
import org.apache.iotdb.mpp.rpc.thrift.TRegionLeaderChangeResp;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Collections;

import static org.mockito.Mockito.when;

public class ConsensusWaitTest {

  @BeforeClass
  public static void setUp() {
    IoTDBDescriptor.getInstance().getConfig().setDataNodeId(0);
  }

  private DataNodeInternalRPCServiceImpl createServiceWithConsensusState(boolean started) {
    DataNodeContext context = Mockito.mock(DataNodeContext.class);
    when(context.isAllConsensusStarted()).thenReturn(started);
    DataNodeInternalRPCServiceImpl service = new DataNodeInternalRPCServiceImpl(context);
    service.setConsensusWaitTimeoutSeconds(1);
    return service;
  }

  private TCreateSchemaRegionReq createSchemaRegionReq() {
    TCreateSchemaRegionReq req = new TCreateSchemaRegionReq();
    req.setStorageGroup("root.test");
    TRegionReplicaSet replicaSet = new TRegionReplicaSet();
    replicaSet.setRegionId(new TConsensusGroupId(TConsensusGroupType.SchemaRegion, 0));
    TDataNodeLocation location = new TDataNodeLocation();
    location.setDataNodeId(0);
    location.setClientRpcEndPoint(new TEndPoint("0.0.0.0", 6667));
    location.setInternalEndPoint(new TEndPoint("0.0.0.0", 10730));
    location.setMPPDataExchangeEndPoint(new TEndPoint("0.0.0.0", 10740));
    location.setDataRegionConsensusEndPoint(new TEndPoint("0.0.0.0", 10760));
    location.setSchemaRegionConsensusEndPoint(new TEndPoint("0.0.0.0", 10750));
    replicaSet.setDataNodeLocations(Collections.singletonList(location));
    req.setRegionReplicaSet(replicaSet);
    return req;
  }

  private TCreateDataRegionReq createDataRegionReq() {
    TCreateDataRegionReq req = new TCreateDataRegionReq();
    req.setStorageGroup("root.test");
    TRegionReplicaSet replicaSet = new TRegionReplicaSet();
    replicaSet.setRegionId(new TConsensusGroupId(TConsensusGroupType.DataRegion, 0));
    TDataNodeLocation location = new TDataNodeLocation();
    location.setDataNodeId(0);
    location.setClientRpcEndPoint(new TEndPoint("0.0.0.0", 6667));
    location.setInternalEndPoint(new TEndPoint("0.0.0.0", 10730));
    location.setMPPDataExchangeEndPoint(new TEndPoint("0.0.0.0", 10740));
    location.setDataRegionConsensusEndPoint(new TEndPoint("0.0.0.0", 10760));
    location.setSchemaRegionConsensusEndPoint(new TEndPoint("0.0.0.0", 10750));
    replicaSet.setDataNodeLocations(Collections.singletonList(location));
    req.setRegionReplicaSet(replicaSet);
    return req;
  }

  @Test
  public void testCreateSchemaRegionRejectsWhenConsensusNotStarted() {
    DataNodeInternalRPCServiceImpl service = createServiceWithConsensusState(false);
    TSStatus status = service.createSchemaRegion(createSchemaRegionReq());
    Assert.assertEquals(TSStatusCode.CONSENSUS_NOT_INITIALIZED.getStatusCode(), status.getCode());
  }

  @Test
  public void testCreateDataRegionRejectsWhenConsensusNotStarted() {
    DataNodeInternalRPCServiceImpl service = createServiceWithConsensusState(false);
    TSStatus status = service.createDataRegion(createDataRegionReq());
    Assert.assertEquals(TSStatusCode.CONSENSUS_NOT_INITIALIZED.getStatusCode(), status.getCode());
  }

  @Test
  public void testDeleteRegionRejectsWhenConsensusNotStarted() {
    DataNodeInternalRPCServiceImpl service = createServiceWithConsensusState(false);
    TConsensusGroupId groupId = new TConsensusGroupId(TConsensusGroupType.DataRegion, 0);
    TSStatus status = service.deleteRegion(groupId);
    Assert.assertEquals(TSStatusCode.CONSENSUS_NOT_INITIALIZED.getStatusCode(), status.getCode());
  }

  @Test
  public void testChangeRegionLeaderRejectsWhenConsensusNotStarted() {
    DataNodeInternalRPCServiceImpl service = createServiceWithConsensusState(false);
    TRegionLeaderChangeReq req = new TRegionLeaderChangeReq();
    req.setRegionId(new TConsensusGroupId(TConsensusGroupType.DataRegion, 0));
    TDataNodeLocation newLeader = new TDataNodeLocation();
    newLeader.setDataNodeId(0);
    newLeader.setInternalEndPoint(new TEndPoint("0.0.0.0", 10730));
    newLeader.setDataRegionConsensusEndPoint(new TEndPoint("0.0.0.0", 10760));
    newLeader.setSchemaRegionConsensusEndPoint(new TEndPoint("0.0.0.0", 10750));
    req.setNewLeaderNode(newLeader);
    TRegionLeaderChangeResp resp = service.changeRegionLeader(req);
    Assert.assertEquals(
        TSStatusCode.CONSENSUS_NOT_INITIALIZED.getStatusCode(), resp.getStatus().getCode());
  }
}
