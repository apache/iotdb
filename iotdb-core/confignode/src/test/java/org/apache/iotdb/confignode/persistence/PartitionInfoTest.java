/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.confignode.persistence;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.common.rpc.thrift.TSeriesPartitionSlot;
import org.apache.iotdb.common.rpc.thrift.TTimePartitionSlot;
import org.apache.iotdb.commons.partition.DataPartitionTable;
import org.apache.iotdb.commons.partition.SchemaPartitionTable;
import org.apache.iotdb.commons.partition.SeriesPartitionTable;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlanType;
import org.apache.iotdb.confignode.consensus.request.read.region.GetRegionInfoListPlan;
import org.apache.iotdb.confignode.consensus.request.write.database.DatabaseSchemaPlan;
import org.apache.iotdb.confignode.consensus.request.write.database.DeleteDatabasePlan;
import org.apache.iotdb.confignode.consensus.request.write.database.PreDeleteDatabasePlan;
import org.apache.iotdb.confignode.consensus.request.write.partition.CreateDataPartitionPlan;
import org.apache.iotdb.confignode.consensus.request.write.partition.CreateSchemaPartitionPlan;
import org.apache.iotdb.confignode.consensus.request.write.region.BatchRemoveRegionCreateTasksPlan;
import org.apache.iotdb.confignode.consensus.request.write.region.CreateRegionGroupsPlan;
import org.apache.iotdb.confignode.consensus.request.write.region.OfferRegionMaintainTasksPlan;
import org.apache.iotdb.confignode.consensus.response.partition.RegionInfoListResp;
import org.apache.iotdb.confignode.exception.DatabaseNotExistsException;
import org.apache.iotdb.confignode.persistence.partition.PartitionInfo;
import org.apache.iotdb.confignode.persistence.partition.maintainer.RegionCreateTask;
import org.apache.iotdb.confignode.persistence.partition.maintainer.RegionDeleteTask;
import org.apache.iotdb.confignode.persistence.partition.maintainer.RegionMaintainTask;
import org.apache.iotdb.confignode.persistence.partition.maintainer.RegionMaintainType;
import org.apache.iotdb.confignode.rpc.thrift.TDatabaseSchema;
import org.apache.iotdb.confignode.rpc.thrift.TShowRegionReq;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.thrift.TException;
import org.apache.tsfile.external.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.iotdb.db.utils.constant.TestConstant.BASE_OUTPUT_PATH;

public class PartitionInfoTest {

  private static PartitionInfo partitionInfo;
  private static final File snapshotDir = new File(BASE_OUTPUT_PATH, "snapshot");

  public enum testFlag {
    DataPartition(20),
    SchemaPartition(30);

    private final int flag;

    testFlag(int flag) {
      this.flag = flag;
    }

    public int getFlag() {
      return flag;
    }
  }

  @Before
  public void setup() {
    partitionInfo = new PartitionInfo();
    if (!snapshotDir.exists()) {
      snapshotDir.mkdirs();
    }
  }

  @After
  public void cleanup() throws IOException {
    partitionInfo.clear();
    if (snapshotDir.exists()) {
      FileUtils.deleteDirectory(snapshotDir);
    }
  }

  @Test
  public void testSnapshot() throws TException, IOException {

    partitionInfo.generateNextRegionGroupId();

    // Set StorageGroup
    partitionInfo.createDatabase(
        new DatabaseSchemaPlan(
            ConfigPhysicalPlanType.CreateDatabase, new TDatabaseSchema("root.test")));

    // Create a SchemaRegion
    CreateRegionGroupsPlan createRegionGroupsReq = new CreateRegionGroupsPlan();
    TRegionReplicaSet schemaRegionReplicaSet =
        generateTRegionReplicaSet(
            testFlag.SchemaPartition.getFlag(),
            generateTConsensusGroupId(
                testFlag.SchemaPartition.getFlag(), TConsensusGroupType.SchemaRegion));
    createRegionGroupsReq.addRegionGroup("root.test", schemaRegionReplicaSet);
    partitionInfo.createRegionGroups(createRegionGroupsReq);

    // Create a DataRegion
    createRegionGroupsReq = new CreateRegionGroupsPlan();
    TRegionReplicaSet dataRegionReplicaSet =
        generateTRegionReplicaSet(
            testFlag.DataPartition.getFlag(),
            generateTConsensusGroupId(
                testFlag.DataPartition.getFlag(), TConsensusGroupType.DataRegion));
    createRegionGroupsReq.addRegionGroup("root.test", dataRegionReplicaSet);
    partitionInfo.createRegionGroups(createRegionGroupsReq);

    // Create a SchemaPartition
    CreateSchemaPartitionPlan createSchemaPartitionPlan =
        generateCreateSchemaPartitionReq(
            testFlag.SchemaPartition.getFlag(),
            generateTConsensusGroupId(
                testFlag.SchemaPartition.getFlag(), TConsensusGroupType.SchemaRegion));
    partitionInfo.createSchemaPartition(createSchemaPartitionPlan);

    // Create a DataPartition
    CreateDataPartitionPlan createDataPartitionPlan =
        generateCreateDataPartitionReq(
            testFlag.DataPartition.getFlag(),
            generateTConsensusGroupId(
                testFlag.DataPartition.getFlag(), TConsensusGroupType.DataRegion));
    partitionInfo.createDataPartition(createDataPartitionPlan);

    partitionInfo.offerRegionMaintainTasks(generateOfferRegionMaintainTasksPlan());

    Assert.assertTrue(partitionInfo.processTakeSnapshot(snapshotDir));

    PartitionInfo partitionInfo1 = new PartitionInfo();
    partitionInfo1.processLoadSnapshot(snapshotDir);
    Assert.assertEquals(partitionInfo, partitionInfo1);
  }

  @Test
  public void testLegacyRegionDeleteTasksAreFiltered() throws TException, IOException {
    // Region deletion is owned by RemoveRegionGroupProcedure; the RegionMaintainer queue only
    // recreates failed replicas. A legacy RegionDeleteTask (offered by an old version and replayed
    // from a consensus log, or carried over in a snapshot) must be dropped rather than queued, so
    // it cannot block the recreation of that region's other replicas.

    // The offer plan mixes two RegionCreateTasks with one legacy RegionDeleteTask.
    final OfferRegionMaintainTasksPlan offerPlan = generateOfferRegionMaintainTasksPlan();
    final RegionCreateTask createTask =
        (RegionCreateTask) offerPlan.getRegionMaintainTaskList().get(0);
    partitionInfo.createDatabase(
        new DatabaseSchemaPlan(
            ConfigPhysicalPlanType.CreateDatabase, new TDatabaseSchema("root.sg")));
    final CreateRegionGroupsPlan createRegionGroupsPlan = new CreateRegionGroupsPlan();
    createRegionGroupsPlan.addRegionGroup("root.sg", createTask.getRegionReplicaSet());
    partitionInfo.createRegionGroups(createRegionGroupsPlan);
    partitionInfo.offerRegionMaintainTasks(offerPlan);

    // The DELETE task is filtered out at offer time; only the two CREATE tasks remain queued.
    List<RegionMaintainTask> queuedTasks = partitionInfo.getRegionMaintainEntryList();
    Assert.assertEquals(2, queuedTasks.size());
    for (RegionMaintainTask task : queuedTasks) {
      Assert.assertEquals(RegionMaintainType.CREATE, task.getType());
    }

    // A snapshot round-trip keeps the CREATE tasks and never resurrects a DELETE task.
    Assert.assertTrue(partitionInfo.processTakeSnapshot(snapshotDir));
    PartitionInfo loaded = new PartitionInfo();
    loaded.processLoadSnapshot(snapshotDir);
    Assert.assertEquals(partitionInfo, loaded);
    Assert.assertEquals(2, loaded.getRegionMaintainEntryList().size());
  }

  @Test
  public void testBatchRemoveAllRegionCreateTasksAndSnapshot() throws TException, IOException {
    final String database = "root.sg";
    final String otherDatabase = "root.other";
    partitionInfo.createDatabase(
        new DatabaseSchemaPlan(
            ConfigPhysicalPlanType.CreateDatabase, new TDatabaseSchema(database)));
    partitionInfo.createDatabase(
        new DatabaseSchemaPlan(
            ConfigPhysicalPlanType.CreateDatabase, new TDatabaseSchema(otherDatabase)));

    final TRegionReplicaSet region0 =
        generateTRegionReplicaSet(0, new TConsensusGroupId(TConsensusGroupType.DataRegion, 0));
    final TRegionReplicaSet region1 =
        generateTRegionReplicaSet(10, new TConsensusGroupId(TConsensusGroupType.DataRegion, 1));
    final TRegionReplicaSet otherRegion =
        generateTRegionReplicaSet(20, new TConsensusGroupId(TConsensusGroupType.DataRegion, 2));
    final CreateRegionGroupsPlan createRegionGroupsPlan = new CreateRegionGroupsPlan();
    createRegionGroupsPlan.addRegionGroup(database, region0);
    createRegionGroupsPlan.addRegionGroup(database, region1);
    createRegionGroupsPlan.addRegionGroup(otherDatabase, otherRegion);
    partitionInfo.createRegionGroups(createRegionGroupsPlan);

    final OfferRegionMaintainTasksPlan offerPlan = new OfferRegionMaintainTasksPlan();
    offerPlan.appendRegionMaintainTask(
        new RegionCreateTask(region0.getDataNodeLocations().get(0), database, region0));
    offerPlan.appendRegionMaintainTask(
        new RegionCreateTask(region0.getDataNodeLocations().get(1), database, region0));
    offerPlan.appendRegionMaintainTask(
        new RegionCreateTask(region1.getDataNodeLocations().get(0), database, region1));
    offerPlan.appendRegionMaintainTask(
        new RegionCreateTask(
            otherRegion.getDataNodeLocations().get(0), otherDatabase, otherRegion));
    partitionInfo.offerRegionMaintainTasks(offerPlan);
    Assert.assertEquals(4, partitionInfo.getRegionMaintainEntryList().size());

    partitionInfo.preDeleteDatabase(
        new PreDeleteDatabasePlan(database, PreDeleteDatabasePlan.PreDeleteType.EXECUTE));
    partitionInfo.batchRemoveRegionCreateTasks(new BatchRemoveRegionCreateTasksPlan(database));
    Assert.assertEquals(1, partitionInfo.getRegionMaintainEntryList().size());
    Assert.assertEquals(
        otherRegion.getRegionId(), partitionInfo.getRegionMaintainEntryList().get(0).getRegionId());

    // Replaying the same consensus plan is idempotent, and a snapshot cannot revive removed tasks.
    partitionInfo.batchRemoveRegionCreateTasks(new BatchRemoveRegionCreateTasksPlan(database));
    Assert.assertTrue(partitionInfo.processTakeSnapshot(snapshotDir));
    final PartitionInfo loaded = new PartitionInfo();
    loaded.processLoadSnapshot(snapshotDir);
    Assert.assertEquals(1, loaded.getRegionMaintainEntryList().size());
    Assert.assertEquals(
        otherRegion.getRegionId(), loaded.getRegionMaintainEntryList().get(0).getRegionId());
  }

  @Test
  public void testCancelledTasksCannotAffectRecreatedDatabase() {
    final String database = "root.sg";
    partitionInfo.createDatabase(
        new DatabaseSchemaPlan(
            ConfigPhysicalPlanType.CreateDatabase, new TDatabaseSchema(database)));
    final TRegionReplicaSet oldRegion =
        generateTRegionReplicaSet(0, new TConsensusGroupId(TConsensusGroupType.DataRegion, 0));
    final CreateRegionGroupsPlan oldCreatePlan = new CreateRegionGroupsPlan();
    oldCreatePlan.addRegionGroup(database, oldRegion);
    partitionInfo.createRegionGroups(oldCreatePlan);

    final OfferRegionMaintainTasksPlan oldOfferPlan = new OfferRegionMaintainTasksPlan();
    oldOfferPlan.appendRegionMaintainTask(
        new RegionCreateTask(oldRegion.getDataNodeLocations().get(0), database, oldRegion));
    partitionInfo.offerRegionMaintainTasks(oldOfferPlan);
    Assert.assertEquals(1, partitionInfo.getRegionMaintainEntryList().size());

    partitionInfo.preDeleteDatabase(
        new PreDeleteDatabasePlan(database, PreDeleteDatabasePlan.PreDeleteType.EXECUTE));
    final BatchRemoveRegionCreateTasksPlan oldCancellation =
        new BatchRemoveRegionCreateTasksPlan(database);
    partitionInfo.batchRemoveRegionCreateTasks(oldCancellation);
    Assert.assertTrue(partitionInfo.getRegionMaintainEntryList().isEmpty());

    // A late offer from the old create procedure is rejected after PRE_DELETE.
    partitionInfo.offerRegionMaintainTasks(oldOfferPlan);
    Assert.assertTrue(partitionInfo.getRegionMaintainEntryList().isEmpty());

    partitionInfo.deleteDatabase(new DeleteDatabasePlan(database));
    partitionInfo.createDatabase(
        new DatabaseSchemaPlan(
            ConfigPhysicalPlanType.CreateDatabase, new TDatabaseSchema(database)));
    final TRegionReplicaSet newRegion =
        generateTRegionReplicaSet(10, new TConsensusGroupId(TConsensusGroupType.DataRegion, 1));
    final CreateRegionGroupsPlan newCreatePlan = new CreateRegionGroupsPlan();
    newCreatePlan.addRegionGroup(database, newRegion);
    partitionInfo.createRegionGroups(newCreatePlan);
    final OfferRegionMaintainTasksPlan newOfferPlan = new OfferRegionMaintainTasksPlan();
    newOfferPlan.appendRegionMaintainTask(
        new RegionCreateTask(newRegion.getDataNodeLocations().get(0), database, newRegion));
    partitionInfo.offerRegionMaintainTasks(newOfferPlan);

    // Replaying the old cancellation is a no-op because the same-name database is not pre-deleted.
    // A late task offer from the old RegionGroup is rejected by Region ownership validation.
    partitionInfo.batchRemoveRegionCreateTasks(oldCancellation);
    partitionInfo.offerRegionMaintainTasks(oldOfferPlan);
    Assert.assertEquals(1, partitionInfo.getRegionMaintainEntryList().size());
    Assert.assertEquals(
        newRegion.getRegionId(), partitionInfo.getRegionMaintainEntryList().get(0).getRegionId());
  }

  @Test
  public void testCreateRegionGroupsRejectsPreDeletedAndMissingDatabase()
      throws DatabaseNotExistsException {
    final String database = "root.lifecycle";
    partitionInfo.createDatabase(
        new DatabaseSchemaPlan(
            ConfigPhysicalPlanType.CreateDatabase, new TDatabaseSchema(database)));

    final CreateRegionGroupsPlan preDeletedPlan = new CreateRegionGroupsPlan();
    preDeletedPlan.setDatabaseGeneration(database, partitionInfo.getDatabaseGeneration(database));
    preDeletedPlan.addRegionGroup(
        database,
        generateTRegionReplicaSet(0, new TConsensusGroupId(TConsensusGroupType.DataRegion, 1)));
    partitionInfo.preDeleteDatabase(
        new PreDeleteDatabasePlan(database, PreDeleteDatabasePlan.PreDeleteType.EXECUTE));

    TSStatus status = partitionInfo.createRegionGroups(preDeletedPlan);
    Assert.assertEquals(TSStatusCode.DATABASE_NOT_EXIST.getStatusCode(), status.getCode());
    Assert.assertTrue(
        partitionInfo.getAllReplicaSets(database, TConsensusGroupType.DataRegion).isEmpty());

    final CreateRegionGroupsPlan missingPlan = new CreateRegionGroupsPlan();
    missingPlan.addRegionGroup(
        "root.missing",
        generateTRegionReplicaSet(10, new TConsensusGroupId(TConsensusGroupType.SchemaRegion, 2)));
    status = partitionInfo.createRegionGroups(missingPlan);
    Assert.assertEquals(TSStatusCode.DATABASE_NOT_EXIST.getStatusCode(), status.getCode());
  }

  @Test
  public void testOldCreateRegionGroupsPlanCannotPolluteRecreatedDatabase()
      throws DatabaseNotExistsException {
    final String database = "root.recreated";
    final DatabaseSchemaPlan createDatabasePlan =
        new DatabaseSchemaPlan(
            ConfigPhysicalPlanType.CreateDatabase, new TDatabaseSchema(database));
    partitionInfo.createDatabase(createDatabasePlan);

    final long oldGeneration = partitionInfo.getDatabaseGeneration(database);
    final CreateRegionGroupsPlan oldPlan = new CreateRegionGroupsPlan();
    oldPlan.setDatabaseGeneration(database, oldGeneration);
    oldPlan.addRegionGroup(
        database,
        generateTRegionReplicaSet(0, new TConsensusGroupId(TConsensusGroupType.DataRegion, 3)));

    partitionInfo.deleteDatabase(new DeleteDatabasePlan(database));
    partitionInfo.createDatabase(createDatabasePlan);
    Assert.assertNotEquals(oldGeneration, partitionInfo.getDatabaseGeneration(database));

    final TSStatus status = partitionInfo.createRegionGroups(oldPlan);
    Assert.assertEquals(TSStatusCode.DATABASE_CONFIG_ERROR.getStatusCode(), status.getCode());
    Assert.assertEquals(
        0, partitionInfo.getRegionGroupCount(database, TConsensusGroupType.DataRegion));
  }

  @Test
  public void testBatchedCreateRegionGroupsPlanIsValidatedAtomically()
      throws DatabaseNotExistsException {
    final String existingDatabase = "root.existing";
    partitionInfo.createDatabase(
        new DatabaseSchemaPlan(
            ConfigPhysicalPlanType.CreateDatabase, new TDatabaseSchema(existingDatabase)));

    final CreateRegionGroupsPlan batchedPlan = new CreateRegionGroupsPlan();
    batchedPlan.setDatabaseGeneration(
        existingDatabase, partitionInfo.getDatabaseGeneration(existingDatabase));
    batchedPlan.addRegionGroup(
        existingDatabase,
        generateTRegionReplicaSet(0, new TConsensusGroupId(TConsensusGroupType.DataRegion, 40)));
    batchedPlan.addRegionGroup(
        "root.missing",
        generateTRegionReplicaSet(10, new TConsensusGroupId(TConsensusGroupType.DataRegion, 41)));

    final TSStatus status = partitionInfo.createRegionGroups(batchedPlan);
    Assert.assertEquals(TSStatusCode.DATABASE_NOT_EXIST.getStatusCode(), status.getCode());
    Assert.assertEquals(
        0, partitionInfo.getRegionGroupCount(existingDatabase, TConsensusGroupType.DataRegion));
    Assert.assertEquals(42, partitionInfo.generateNextRegionGroupId());
  }

  @Test
  public void testGetRegionType() {

    partitionInfo.generateNextRegionGroupId();

    // Set StorageGroup
    partitionInfo.createDatabase(
        new DatabaseSchemaPlan(
            ConfigPhysicalPlanType.CreateDatabase, new TDatabaseSchema("root.test")));

    // Create a SchemaRegion
    CreateRegionGroupsPlan createRegionGroupsPlan = new CreateRegionGroupsPlan();
    TConsensusGroupId schemaRegionId =
        generateTConsensusGroupId(
            testFlag.SchemaPartition.getFlag(), TConsensusGroupType.SchemaRegion);

    TRegionReplicaSet schemaRegionReplicaSet =
        generateTRegionReplicaSet(testFlag.SchemaPartition.getFlag(), schemaRegionId);
    createRegionGroupsPlan.addRegionGroup("root.test", schemaRegionReplicaSet);
    partitionInfo.createRegionGroups(createRegionGroupsPlan);

    // Create a DataRegion
    createRegionGroupsPlan = new CreateRegionGroupsPlan();
    TConsensusGroupId dataRegionId =
        generateTConsensusGroupId(testFlag.DataPartition.getFlag(), TConsensusGroupType.DataRegion);
    TRegionReplicaSet dataRegionReplicaSet =
        generateTRegionReplicaSet(testFlag.DataPartition.getFlag(), dataRegionId);
    createRegionGroupsPlan.addRegionGroup("root.test", dataRegionReplicaSet);
    partitionInfo.createRegionGroups(createRegionGroupsPlan);

    Assert.assertEquals(
        Optional.of(TConsensusGroupType.SchemaRegion),
        partitionInfo.getRegionType(schemaRegionId.getId()));
    Assert.assertEquals(
        Optional.of(TConsensusGroupType.DataRegion),
        partitionInfo.getRegionType(dataRegionId.getId()));
    Assert.assertEquals(Optional.empty(), partitionInfo.getRegionType(-1));
  }

  @Test
  public void testShowRegion() {
    for (int i = 0; i < 2; i++) {
      partitionInfo.generateNextRegionGroupId();

      // Set StorageGroup
      partitionInfo.createDatabase(
          new DatabaseSchemaPlan(
              ConfigPhysicalPlanType.CreateDatabase, new TDatabaseSchema("root.test" + i)));

      // Create a SchemaRegion
      CreateRegionGroupsPlan createRegionGroupsPlan = new CreateRegionGroupsPlan();
      final TRegionReplicaSet schemaRegionReplicaSet =
          generateTRegionReplicaSet(
              testFlag.SchemaPartition.getFlag(),
              generateTConsensusGroupId(
                  testFlag.SchemaPartition.getFlag(), TConsensusGroupType.SchemaRegion));
      createRegionGroupsPlan.addRegionGroup("root.test" + i, schemaRegionReplicaSet);
      partitionInfo.createRegionGroups(createRegionGroupsPlan);

      // Create a DataRegion
      createRegionGroupsPlan = new CreateRegionGroupsPlan();
      final TRegionReplicaSet dataRegionReplicaSet =
          generateTRegionReplicaSet(
              testFlag.DataPartition.getFlag(),
              generateTConsensusGroupId(
                  testFlag.DataPartition.getFlag(), TConsensusGroupType.DataRegion));
      createRegionGroupsPlan.addRegionGroup("root.test" + i, dataRegionReplicaSet);
      partitionInfo.createRegionGroups(createRegionGroupsPlan);
    }
    final GetRegionInfoListPlan regionReq = new GetRegionInfoListPlan();
    final TShowRegionReq showRegionReq = new TShowRegionReq();
    showRegionReq.setConsensusGroupType(null);
    regionReq.setShowRegionReq(showRegionReq);
    final RegionInfoListResp regionInfoList1 =
        (RegionInfoListResp) partitionInfo.getRegionInfoList(regionReq);
    Assert.assertEquals(20, regionInfoList1.getRegionInfoList().size());
    regionInfoList1
        .getRegionInfoList()
        .forEach((regionInfo) -> Assert.assertEquals("127.0.0.1", regionInfo.getClientRpcIp()));

    showRegionReq.setConsensusGroupType(TConsensusGroupType.SchemaRegion);
    final RegionInfoListResp regionInfoList2 =
        (RegionInfoListResp) partitionInfo.getRegionInfoList(regionReq);
    Assert.assertEquals(10, regionInfoList2.getRegionInfoList().size());
    regionInfoList2
        .getRegionInfoList()
        .forEach(
            (regionInfo) ->
                Assert.assertEquals(
                    TConsensusGroupType.SchemaRegion, regionInfo.getConsensusGroupId().getType()));

    showRegionReq.setConsensusGroupType(TConsensusGroupType.DataRegion);
    final RegionInfoListResp regionInfoList3 =
        (RegionInfoListResp) partitionInfo.getRegionInfoList(regionReq);
    Assert.assertEquals(10, regionInfoList3.getRegionInfoList().size());
    regionInfoList3
        .getRegionInfoList()
        .forEach(
            (regionInfo) ->
                Assert.assertEquals(
                    TConsensusGroupType.DataRegion, regionInfo.getConsensusGroupId().getType()));
    showRegionReq.setConsensusGroupType(null);
    showRegionReq.setDatabases(Collections.singletonList("root.test1"));
    final RegionInfoListResp regionInfoList4 =
        (RegionInfoListResp) partitionInfo.getRegionInfoList(regionReq);
    Assert.assertEquals(10, regionInfoList4.getRegionInfoList().size());
    regionInfoList4
        .getRegionInfoList()
        .forEach(
            (regionInfo) -> {
              Assert.assertEquals("127.0.0.1", regionInfo.getClientRpcIp());
              Assert.assertEquals("root.test1", regionInfo.getDatabase());
            });
  }

  private TRegionReplicaSet generateTRegionReplicaSet(
      int startFlag, TConsensusGroupId tConsensusGroupId) {
    TRegionReplicaSet tRegionReplicaSet = new TRegionReplicaSet();
    tRegionReplicaSet.setRegionId(tConsensusGroupId);
    List<TDataNodeLocation> dataNodeLocations = new ArrayList<>();
    int locationNum = 5;
    for (int i = startFlag; i < locationNum + startFlag; i++) {
      TDataNodeLocation tDataNodeLocation = new TDataNodeLocation();
      tDataNodeLocation.setDataNodeId(i);
      tDataNodeLocation.setClientRpcEndPoint(new TEndPoint("127.0.0.1", 6000 + i));
      tDataNodeLocation.setInternalEndPoint(new TEndPoint("127.0.0.1", 7000 + i));
      tDataNodeLocation.setMPPDataExchangeEndPoint(new TEndPoint("127.0.0.1", 8000 + i));
      tDataNodeLocation.setDataRegionConsensusEndPoint(new TEndPoint("127.0.0.1", 9000 + i));
      tDataNodeLocation.setSchemaRegionConsensusEndPoint(new TEndPoint("127.0.0.1", 10000 + i));
      dataNodeLocations.add(tDataNodeLocation);
    }
    tRegionReplicaSet.setDataNodeLocations(dataNodeLocations);
    return tRegionReplicaSet;
  }

  private OfferRegionMaintainTasksPlan generateOfferRegionMaintainTasksPlan() {
    TDataNodeLocation dataNodeLocation = new TDataNodeLocation();
    dataNodeLocation.setDataNodeId(0);
    dataNodeLocation.setClientRpcEndPoint(new TEndPoint("0.0.0.0", 6667));
    dataNodeLocation.setInternalEndPoint(new TEndPoint("0.0.0.0", 10730));
    dataNodeLocation.setMPPDataExchangeEndPoint(new TEndPoint("0.0.0.0", 10740));
    dataNodeLocation.setDataRegionConsensusEndPoint(new TEndPoint("0.0.0.0", 10760));
    dataNodeLocation.setSchemaRegionConsensusEndPoint(new TEndPoint("0.0.0.0", 10750));

    TRegionReplicaSet regionReplicaSet = new TRegionReplicaSet();
    regionReplicaSet.setRegionId(new TConsensusGroupId(TConsensusGroupType.DataRegion, 0));
    regionReplicaSet.setDataNodeLocations(Collections.singletonList(dataNodeLocation));

    OfferRegionMaintainTasksPlan offerPlan = new OfferRegionMaintainTasksPlan();
    offerPlan.appendRegionMaintainTask(
        new RegionCreateTask(dataNodeLocation, "root.sg", regionReplicaSet));
    offerPlan.appendRegionMaintainTask(
        new RegionCreateTask(dataNodeLocation, "root.sg", regionReplicaSet));
    offerPlan.appendRegionMaintainTask(
        new RegionDeleteTask(
            dataNodeLocation, new TConsensusGroupId(TConsensusGroupType.SchemaRegion, 2)));

    return offerPlan;
  }

  private CreateSchemaPartitionPlan generateCreateSchemaPartitionReq(
      int startFlag, TConsensusGroupId tConsensusGroupId) {
    CreateSchemaPartitionPlan createSchemaPartitionPlan = new CreateSchemaPartitionPlan();
    // Map<StorageGroup, Map<TSeriesPartitionSlot, TSchemaRegionPlaceInfo>>
    Map<String, SchemaPartitionTable> assignedSchemaPartition = new HashMap<>();
    Map<TSeriesPartitionSlot, TConsensusGroupId> relationInfo = new HashMap<>();
    relationInfo.put(new TSeriesPartitionSlot(startFlag), tConsensusGroupId);
    assignedSchemaPartition.put("root.test", new SchemaPartitionTable(relationInfo));
    createSchemaPartitionPlan.setAssignedSchemaPartition(assignedSchemaPartition);
    return createSchemaPartitionPlan;
  }

  private CreateDataPartitionPlan generateCreateDataPartitionReq(
      int startFlag, TConsensusGroupId tConsensusGroupId) {
    startFlag = startFlag / 10;
    CreateDataPartitionPlan createSchemaPartitionReq = new CreateDataPartitionPlan();
    // Map<StorageGroup, Map<TSeriesPartitionSlot, Map<TTimePartitionSlot, List<TRegionMessage>>>>
    Map<String, DataPartitionTable> dataPartitionMap = new HashMap<>();

    Map<TTimePartitionSlot, List<TConsensusGroupId>> relationInfo = new HashMap<>();

    for (int i = 0; i <= startFlag; i++) {
      relationInfo.put(
          new TTimePartitionSlot((System.currentTimeMillis() / 1000) + i),
          Collections.singletonList(tConsensusGroupId));
    }

    Map<TSeriesPartitionSlot, SeriesPartitionTable> slotInfo = new HashMap<>();

    for (int i = 0; i <= startFlag; i++) {
      slotInfo.put(new TSeriesPartitionSlot(startFlag + i), new SeriesPartitionTable(relationInfo));
    }

    dataPartitionMap.put("root.test", new DataPartitionTable(slotInfo));
    createSchemaPartitionReq.setAssignedDataPartition(dataPartitionMap);
    return createSchemaPartitionReq;
  }

  private TConsensusGroupId generateTConsensusGroupId(
      int startFlag, TConsensusGroupType consensusGroupType) {
    return new TConsensusGroupId(consensusGroupType, 111000 + startFlag);
  }
}
