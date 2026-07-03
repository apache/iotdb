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

package org.apache.iotdb.confignode.procedure.impl.region;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.confignode.procedure.store.ProcedureFactory;

import org.apache.tsfile.utils.PublicBAOS;
import org.junit.Assert;
import org.junit.Test;

import java.io.DataOutputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;

public class RemoveRegionGroupProcedureTest {
  @Test
  public void serDeTest() throws Exception {
    final TRegionReplicaSet regionReplicaSet =
        new TRegionReplicaSet(
            new TConsensusGroupId(TConsensusGroupType.DataRegion, 10),
            Arrays.asList(
                new TDataNodeLocation(
                    1,
                    new TEndPoint("127.0.0.1", 0),
                    new TEndPoint("127.0.0.1", 1),
                    new TEndPoint("127.0.0.1", 2),
                    new TEndPoint("127.0.0.1", 3),
                    new TEndPoint("127.0.0.1", 4)),
                new TDataNodeLocation(
                    2,
                    new TEndPoint("127.0.0.1", 10),
                    new TEndPoint("127.0.0.1", 11),
                    new TEndPoint("127.0.0.1", 12),
                    new TEndPoint("127.0.0.1", 13),
                    new TEndPoint("127.0.0.1", 14))));
    final RemoveRegionGroupProcedure procedure = new RemoveRegionGroupProcedure(regionReplicaSet);
    // A non-zero cursor so the round-trip actually exercises currentReplicaIndex (de)serialization;
    // equals/hashCode include it, so a dropped/garbled cursor would fail the assertion.
    procedure.setCurrentReplicaIndex(1);
    // Non-default delete-task cursor so the round-trip exercises deleteTaskSeq/deleteTaskSubmitted
    // too; equals/hashCode include them, so a dropped/garbled value would fail the assertion.
    procedure.setDeleteTaskState(42L, true);
    try (PublicBAOS byteArrayOutputStream = new PublicBAOS();
        DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream)) {
      procedure.serialize(outputStream);
      ByteBuffer buffer =
          ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size());
      // Exercises ProcedureType.REMOVE_REGION_GROUP_PROCEDURE + ProcedureFactory registration as
      // well as the procedure's own serialize/deserialize.
      Assert.assertEquals(procedure, ProcedureFactory.getInstance().create(buffer));
    }
  }

  @Test
  public void deleteTaskIdIsNegativeAndUnique() {
    // The DataNode taskResultMap is keyed only by taskId and is shared with add/remove-peer tasks,
    // which use a procedure's (non-negative) procId directly as the taskId. So a delete taskId must
    // be strictly negative (disjoint from every procId) and distinct per (procId, deleteTaskSeq),
    // otherwise a later peer op could be silently deduped against a lingering delete-task entry.
    final TRegionReplicaSet regionReplicaSet =
        new TRegionReplicaSet(
            new TConsensusGroupId(TConsensusGroupType.DataRegion, 1),
            Arrays.asList(new TDataNodeLocation()));
    final java.util.Set<Long> seen = new java.util.HashSet<>();
    for (long procId : new long[] {0L, 1L, 100L, 1L << 20, (1L << 43) - 1}) {
      for (long seq : new long[] {1L, 2L, 100L, (1L << 20) - 1}) {
        final RemoveRegionGroupProcedure procedure =
            new RemoveRegionGroupProcedure(regionReplicaSet);
        procedure.setProcId(procId);
        procedure.setDeleteTaskState(seq, true);
        final long taskId = procedure.deleteTaskIdForTest();
        Assert.assertTrue("taskId must be negative: " + taskId, taskId < 0);
        Assert.assertTrue(
            "taskId must be unique for (" + procId + "," + seq + ")", seen.add(taskId));
      }
    }
  }
}
