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

package org.apache.iotdb.confignode.procedure.impl.consistency;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.commons.consensus.iotv2.consistency.RepairProgressTable;
import org.apache.iotdb.confignode.procedure.Procedure;
import org.apache.iotdb.confignode.procedure.store.ProcedureFactory;

import org.apache.tsfile.utils.PublicBAOS;
import org.junit.Assert;
import org.junit.Test;

import java.io.DataOutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BooleanSupplier;

public class RepairRegionProcedureTest {

  @Test
  public void serDeTest() throws Exception {
    TConsensusGroupId groupId = new TConsensusGroupId(TConsensusGroupType.DataRegion, 10);
    RepairRegionProcedure procedure =
        new RepairRegionProcedure(groupId, new TestExecutionContext(Collections.emptyMap()));
    try (PublicBAOS byteArrayOutputStream = new PublicBAOS();
        DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream)) {
      procedure.serialize(outputStream);
      ByteBuffer buffer =
          ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size());
      Assert.assertEquals(procedure, ProcedureFactory.getInstance().create(buffer));
    } finally {
      RepairRegionProcedure.unregisterExecutionContext(procedure.getExecutionContextId());
    }
  }

  @Test
  public void executeRepairOperationFlowTest() throws Exception {
    TConsensusGroupId groupId = new TConsensusGroupId(TConsensusGroupType.DataRegion, 1);
    String repairEpoch = "leader:1:1000:2000:2000";
    List<String> operationIds = Arrays.asList("live@leaf:1:0", "tombstone@leaf:1:0");

    TestExecutionContext executionContext = new TestExecutionContext(new LinkedHashMap<>());
    executionContext.lastPersistedTable.markMismatch(
        0L,
        100L,
        1000L,
        2000L,
        2000L,
        RepairProgressTable.SnapshotState.READY,
        "LIVE@leaf:1:0,TOMBSTONE@leaf:1:0",
        2,
        repairEpoch);
    executionContext.partitionContexts.put(
        0L,
        new TestPartitionRepairContext(
            0L,
            false,
            operationIds,
            repairEpoch,
            () -> executionContext.executedOperationIds.equals(operationIds)));

    ExposedRepairRegionProcedure procedure =
        new ExposedRepairRegionProcedure(groupId, executionContext);

    executeProcedureToCompletion(procedure, 16);

    Assert.assertEquals(operationIds, executionContext.executedOperationIds);
    Assert.assertEquals(Collections.singletonList(0L), executionContext.committedPartitions);
    Assert.assertTrue(executionContext.closed);
    Assert.assertFalse(executionContext.rolledBack);

    RepairProgressTable.PartitionProgress progress =
        executionContext.lastPersistedTable.getPartition(0L);
    Assert.assertNotNull(progress);
    Assert.assertEquals(RepairProgressTable.CheckState.VERIFIED, progress.getCheckState());
    Assert.assertEquals(RepairProgressTable.RepairState.SUCCEEDED, progress.getRepairState());
    Assert.assertEquals(repairEpoch, progress.getRepairEpoch());
    Assert.assertNull(progress.getLastErrorCode());
  }

  @Test
  public void executeRepairOperationFailureRollsBackTest() throws Exception {
    TConsensusGroupId groupId = new TConsensusGroupId(TConsensusGroupType.DataRegion, 2);
    String repairEpoch = "leader:2:1000:3000:3000";

    TestExecutionContext executionContext = new TestExecutionContext(new LinkedHashMap<>());
    executionContext.lastPersistedTable.markMismatch(
        0L,
        100L,
        1000L,
        3000L,
        3000L,
        RepairProgressTable.SnapshotState.READY,
        "LIVE@leaf:2:0",
        1,
        repairEpoch);
    executionContext.operationToFail = "live@leaf:2:0";
    executionContext.partitionContexts.put(
        0L,
        new TestPartitionRepairContext(
            0L, false, Collections.singletonList("live@leaf:2:0"), repairEpoch, () -> false));

    ExposedRepairRegionProcedure procedure =
        new ExposedRepairRegionProcedure(groupId, executionContext);

    executeProcedureToCompletion(procedure, 16);

    Assert.assertEquals(
        Collections.singletonList("live@leaf:2:0"), executionContext.executedOperationIds);
    Assert.assertTrue(executionContext.closed);
    Assert.assertTrue(executionContext.rolledBack);
    Assert.assertTrue(executionContext.committedPartitions.isEmpty());

    RepairProgressTable.PartitionProgress progress =
        executionContext.lastPersistedTable.getPartition(0L);
    Assert.assertNotNull(progress);
    Assert.assertEquals(RepairProgressTable.CheckState.FAILED, progress.getCheckState());
    Assert.assertEquals(RepairProgressTable.RepairState.FAILED, progress.getRepairState());
    Assert.assertEquals("REPAIR_FAILED", progress.getLastErrorCode());
    Assert.assertEquals(repairEpoch, progress.getRepairEpoch());
  }

  @Test
  public void blockingReasonShouldFailFastWithoutRepairOperations() throws Exception {
    TConsensusGroupId groupId = new TConsensusGroupId(TConsensusGroupType.DataRegion, 3);
    String repairEpoch = "leader:3:1000:4000:4000";
    String blockingReason =
        "Partition 0 contains follower-only tombstone mismatches that cannot be rolled back safely yet";

    TestExecutionContext executionContext = new TestExecutionContext(new LinkedHashMap<>());
    executionContext.lastPersistedTable.markMismatch(
        0L,
        100L,
        1000L,
        4000L,
        4000L,
        RepairProgressTable.SnapshotState.READY,
        "TOMBSTONE@leaf:3:0",
        1,
        repairEpoch);
    executionContext.partitionContexts.put(
        0L,
        new TestPartitionRepairContext(
            0L, false, Collections.emptyList(), repairEpoch, blockingReason, () -> false));

    ExposedRepairRegionProcedure procedure =
        new ExposedRepairRegionProcedure(groupId, executionContext);

    executeProcedureToCompletion(procedure, 16);

    Assert.assertTrue(executionContext.executedOperationIds.isEmpty());
    Assert.assertTrue(executionContext.closed);
    Assert.assertTrue(executionContext.rolledBack);

    RepairProgressTable.PartitionProgress progress =
        executionContext.lastPersistedTable.getPartition(0L);
    Assert.assertNotNull(progress);
    Assert.assertEquals(RepairProgressTable.CheckState.FAILED, progress.getCheckState());
    Assert.assertEquals(RepairProgressTable.RepairState.FAILED, progress.getRepairState());
    Assert.assertEquals("REPAIR_FAILED", progress.getLastErrorCode());
    Assert.assertTrue(progress.getLastErrorMessage().contains(blockingReason));
  }

  private static void executeProcedureToCompletion(
      ExposedRepairRegionProcedure procedure, int maxSteps) throws Exception {
    int steps = 0;
    Procedure<?>[] next;
    do {
      next = procedure.executeOnce();
      steps++;
    } while (next != null && steps < maxSteps);
    Assert.assertTrue("procedure should finish within " + maxSteps + " steps", steps < maxSteps);
  }

  private static final class ExposedRepairRegionProcedure extends RepairRegionProcedure {

    private ExposedRepairRegionProcedure(
        TConsensusGroupId consensusGroupId, RepairExecutionContext executionContext) {
      super(consensusGroupId, executionContext);
    }

    private Procedure<?>[] executeOnce() throws InterruptedException {
      return doExecute(null);
    }
  }

  private static final class TestExecutionContext
      implements RepairRegionProcedure.RepairExecutionContext {

    private final Map<Long, RepairRegionProcedure.PartitionRepairContext> partitionContexts;
    private final List<String> executedOperationIds = new ArrayList<>();
    private final List<Long> committedPartitions = new ArrayList<>();
    private RepairProgressTable lastPersistedTable = new RepairProgressTable("DataRegion-unknown");
    private String operationToFail;
    private boolean closed;
    private boolean rolledBack;

    private TestExecutionContext(
        Map<Long, RepairRegionProcedure.PartitionRepairContext> partitionContexts) {
      this.partitionContexts = partitionContexts;
    }

    @Override
    public boolean isReplicationComplete() {
      return true;
    }

    @Override
    public long computeSafeWatermark() {
      return 1000L;
    }

    @Override
    public List<Long> collectPendingPartitions(
        long safeWatermark, RepairProgressTable repairProgressTable) {
      return new ArrayList<>(partitionContexts.keySet());
    }

    @Override
    public RepairRegionProcedure.PartitionRepairContext getPartitionContext(long partitionId) {
      return partitionContexts.get(partitionId);
    }

    @Override
    public void executeRepairOperation(String operationId) {
      executedOperationIds.add(operationId);
      if (operationId.equals(operationToFail)) {
        throw new IllegalStateException("Injected repair failure for " + operationId);
      }
    }

    @Override
    public RepairProgressTable loadRepairProgressTable(String consensusGroupKey) {
      return lastPersistedTable.copy();
    }

    @Override
    public void persistRepairProgressTable(RepairProgressTable repairProgressTable) {
      this.lastPersistedTable = repairProgressTable.copy();
    }

    @Override
    public void onPartitionCommitted(
        long partitionId, long committedAt, RepairProgressTable repairProgressTable) {
      committedPartitions.add(partitionId);
    }

    @Override
    public void rollbackPartition(long partitionId, RepairProgressTable repairProgressTable) {
      rolledBack = true;
    }

    @Override
    public void close() {
      closed = true;
    }
  }

  private static final class TestPartitionRepairContext
      implements RepairRegionProcedure.PartitionRepairContext {

    private final long partitionId;
    private final boolean rootHashMatched;
    private final List<String> repairOperationIds;
    private final String repairEpoch;
    private final String blockingReason;
    private final BooleanSupplier verifier;

    private TestPartitionRepairContext(
        long partitionId,
        boolean rootHashMatched,
        List<String> repairOperationIds,
        String repairEpoch,
        String blockingReason,
        BooleanSupplier verifier) {
      this.partitionId = partitionId;
      this.rootHashMatched = rootHashMatched;
      this.repairOperationIds = repairOperationIds;
      this.repairEpoch = repairEpoch;
      this.blockingReason = blockingReason;
      this.verifier = verifier;
    }

    private TestPartitionRepairContext(
        long partitionId,
        boolean rootHashMatched,
        List<String> repairOperationIds,
        String repairEpoch,
        BooleanSupplier verifier) {
      this(partitionId, rootHashMatched, repairOperationIds, repairEpoch, null, verifier);
    }

    @Override
    public long getPartitionId() {
      return partitionId;
    }

    @Override
    public boolean isRootHashMatched() {
      return rootHashMatched;
    }

    @Override
    public List<String> getRepairOperationIds() {
      return repairOperationIds;
    }

    @Override
    public String getRepairEpoch() {
      return repairEpoch;
    }

    @Override
    public String getBlockingReason() {
      return blockingReason;
    }

    @Override
    public boolean verify() {
      return verifier.getAsBoolean();
    }
  }
}
