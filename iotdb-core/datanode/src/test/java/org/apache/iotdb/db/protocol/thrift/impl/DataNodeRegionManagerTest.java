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
import org.apache.iotdb.commons.consensus.ConsensusGroupId;
import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.commons.consensus.SchemaRegionId;
import org.apache.iotdb.consensus.IConsensus;
import org.apache.iotdb.consensus.exception.ConsensusException;
import org.apache.iotdb.db.consensus.DataRegionConsensusImpl;
import org.apache.iotdb.db.consensus.SchemaRegionConsensusImpl;
import org.apache.iotdb.db.schemaengine.SchemaEngine;
import org.apache.iotdb.db.storageengine.StorageEngine;
import org.apache.iotdb.db.storageengine.dataregion.DataRegion;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.List;

public class DataNodeRegionManagerTest {

  private IConsensus previousDataConsensus;
  private IConsensus previousSchemaConsensus;
  private IConsensus dataConsensus;
  private IConsensus schemaConsensus;
  private StorageEngine storageEngine;
  private SchemaEngine schemaEngine;
  private DataNodeRegionManager regionManager;

  @Before
  public void setUp() {
    previousDataConsensus = DataRegionConsensusImpl.getInstance();
    previousSchemaConsensus = SchemaRegionConsensusImpl.getInstance();
    dataConsensus = Mockito.mock(IConsensus.class);
    schemaConsensus = Mockito.mock(IConsensus.class);
    DataRegionConsensusImpl.setInstance(dataConsensus);
    SchemaRegionConsensusImpl.setInstance(schemaConsensus);
    storageEngine = Mockito.mock(StorageEngine.class);
    schemaEngine = Mockito.mock(SchemaEngine.class);
    regionManager = new DataNodeRegionManager(schemaEngine, storageEngine);
  }

  @After
  public void tearDown() {
    DataRegionConsensusImpl.setInstance(previousDataConsensus);
    SchemaRegionConsensusImpl.setInstance(previousSchemaConsensus);
  }

  @Test
  public void testDataRegionConsensusOomRollsBackNewLocalState() throws Exception {
    final DataRegionId regionId = new DataRegionId(1);
    final List<ConsensusGroupId> noConsensusGroups = Collections.emptyList();
    final List<ConsensusGroupId> partiallyCreatedGroup = Collections.singletonList(regionId);
    Mockito.when(dataConsensus.getAllConsensusGroupIds())
        .thenReturn(noConsensusGroups, partiallyCreatedGroup);
    Mockito.when(storageEngine.getDataRegion(regionId)).thenReturn(null);
    Mockito.when(storageEngine.createDataRegionIfAbsent(regionId, "root.sg")).thenReturn(true);
    Mockito.doThrow(new OutOfMemoryError("WAL direct memory exhausted"))
        .when(dataConsensus)
        .createLocalPeer(Mockito.eq(regionId), Mockito.anyList());

    final TSStatus status =
        regionManager.createDataRegion(
            createReplicaSet(TConsensusGroupType.DataRegion, 1), "root.sg");

    Assert.assertEquals(TSStatusCode.CREATE_REGION_ERROR.getStatusCode(), status.getCode());
    Mockito.verify(dataConsensus).deleteLocalPeer(regionId);
    Mockito.verify(storageEngine).deleteDataRegion(regionId);
    Assert.assertNull(regionManager.getRegionLock(regionId));
  }

  @Test
  public void testSchemaRegionConsensusFailureRollsBackNewLocalState() throws Exception {
    final SchemaRegionId regionId = new SchemaRegionId(2);
    final List<ConsensusGroupId> noConsensusGroups = Collections.emptyList();
    final List<ConsensusGroupId> partiallyCreatedGroup = Collections.singletonList(regionId);
    Mockito.when(schemaConsensus.getAllConsensusGroupIds())
        .thenReturn(noConsensusGroups, partiallyCreatedGroup);
    Mockito.when(schemaEngine.getSchemaRegion(regionId)).thenReturn(null);
    Mockito.when(schemaEngine.createSchemaRegionIfAbsent("root.sg", regionId)).thenReturn(true);
    Mockito.doThrow(new ConsensusException("Ratis create failed"))
        .when(schemaConsensus)
        .createLocalPeer(Mockito.eq(regionId), Mockito.anyList());

    final TSStatus status =
        regionManager.createSchemaRegion(
            createReplicaSet(TConsensusGroupType.SchemaRegion, 2), "root.sg");

    Assert.assertEquals(TSStatusCode.CREATE_REGION_ERROR.getStatusCode(), status.getCode());
    Mockito.verify(schemaConsensus).deleteLocalPeer(regionId);
    Mockito.verify(schemaEngine).deleteSchemaRegion(regionId);
    Assert.assertNull(regionManager.getRegionLock(regionId));
  }

  @Test
  public void testFailedIdempotentRetryDoesNotDeleteExistingDataRegion() throws Exception {
    final DataRegionId regionId = new DataRegionId(3);
    Mockito.when(dataConsensus.getAllConsensusGroupIds()).thenReturn(Collections.emptyList());
    Mockito.when(storageEngine.getDataRegion(regionId)).thenReturn(Mockito.mock(DataRegion.class));
    Mockito.when(storageEngine.createDataRegionIfAbsent(regionId, "root.sg")).thenReturn(false);
    Mockito.doThrow(new ConsensusException("consensus unavailable"))
        .when(dataConsensus)
        .createLocalPeer(Mockito.eq(regionId), Mockito.anyList());

    final TSStatus status =
        regionManager.createDataRegion(
            createReplicaSet(TConsensusGroupType.DataRegion, 3), "root.sg");

    Assert.assertEquals(TSStatusCode.CREATE_REGION_ERROR.getStatusCode(), status.getCode());
    Mockito.verify(storageEngine, Mockito.never()).deleteDataRegion(regionId);
  }

  @Test
  public void testLocalRollbackContinuesWhenConsensusRollbackFails() throws Exception {
    final DataRegionId regionId = new DataRegionId(4);
    Mockito.when(dataConsensus.getAllConsensusGroupIds())
        .thenReturn(Collections.emptyList(), Collections.singletonList(regionId));
    Mockito.when(storageEngine.getDataRegion(regionId)).thenReturn(null);
    Mockito.when(storageEngine.createDataRegionIfAbsent(regionId, "root.sg")).thenReturn(true);
    Mockito.doThrow(new ConsensusException("Ratis create failed"))
        .when(dataConsensus)
        .createLocalPeer(Mockito.eq(regionId), Mockito.anyList());
    Mockito.doThrow(new ConsensusException("Ratis rollback failed"))
        .when(dataConsensus)
        .deleteLocalPeer(regionId);

    final TSStatus status =
        regionManager.createDataRegion(
            createReplicaSet(TConsensusGroupType.DataRegion, 4), "root.sg");

    Assert.assertEquals(TSStatusCode.CREATE_REGION_ERROR.getStatusCode(), status.getCode());
    Mockito.verify(storageEngine).deleteDataRegion(regionId);
    Assert.assertNull(regionManager.getRegionLock(regionId));
  }

  @Test
  public void testDeletedRegionGroupRejectsLateCreation() throws Exception {
    final DataRegionId regionId = new DataRegionId(5);
    regionManager.markRegionGroupDeleted(regionId);

    final TSStatus status =
        regionManager.createDataRegion(
            createReplicaSet(TConsensusGroupType.DataRegion, 5), "root.sg");

    Assert.assertEquals(TSStatusCode.CREATE_REGION_ERROR.getStatusCode(), status.getCode());
    Mockito.verify(storageEngine, Mockito.never())
        .createDataRegionIfAbsent(Mockito.any(), Mockito.anyString());
    Mockito.verify(dataConsensus, Mockito.never())
        .createLocalPeer(Mockito.any(), Mockito.anyList());
  }

  private TRegionReplicaSet createReplicaSet(TConsensusGroupType type, int regionId) {
    final TDataNodeLocation location =
        new TDataNodeLocation()
            .setDataNodeId(0)
            .setInternalEndPoint(new TEndPoint("127.0.0.1", 10730))
            .setDataRegionConsensusEndPoint(new TEndPoint("127.0.0.1", 10760))
            .setSchemaRegionConsensusEndPoint(new TEndPoint("127.0.0.1", 10750));
    return new TRegionReplicaSet(
        new TConsensusGroupId(type, regionId), Collections.singletonList(location));
  }
}
