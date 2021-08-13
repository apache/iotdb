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
package org.apache.iotdb.cluster.server.member;

import org.apache.iotdb.cluster.common.TestAsyncDataClient;
import org.apache.iotdb.cluster.common.TestDataGroupMember;
import org.apache.iotdb.cluster.common.TestPartitionedLogManager;
import org.apache.iotdb.cluster.common.TestUtils;
import org.apache.iotdb.cluster.config.ClusterDescriptor;
import org.apache.iotdb.cluster.config.ConsistencyLevel;
import org.apache.iotdb.cluster.exception.CheckConsistencyException;
import org.apache.iotdb.cluster.log.manage.PartitionedSnapshotLogManager;
import org.apache.iotdb.cluster.rpc.thrift.AppendEntryRequest;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.rpc.thrift.RaftService;
import org.apache.iotdb.cluster.rpc.thrift.RequestCommitIndexResponse;
import org.apache.iotdb.cluster.server.NodeCharacter;
import org.apache.iotdb.cluster.server.Response;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;

public class RaftMemberTest extends BaseMember {
  @Test
  public void testsyncLeaderStrongConsistencyCheckFalse() {
    // 1. write request : Strong consistency level with syncLeader false
    DataGroupMember dataGroupMemberWithWriteStrongConsistencyFalse =
        newDataGroupMemberWithSyncLeaderFalse(TestUtils.getNode(0), false);
    ClusterDescriptor.getInstance()
        .getConfig()
        .setConsistencyLevel(ConsistencyLevel.STRONG_CONSISTENCY);
    try {
      dataGroupMemberWithWriteStrongConsistencyFalse.waitUntilCatchUp(
          new RaftMember.StrongCheckConsistency());
    } catch (CheckConsistencyException e) {
      Assert.assertNotNull(e);
      Assert.assertEquals(CheckConsistencyException.CHECK_STRONG_CONSISTENCY_EXCEPTION, e);
    }
  }

  @Test
  public void testsyncLeaderStrongConsistencyCheckTrue() {
    // 1. write request : Strong consistency level with syncLeader false
    DataGroupMember dataGroupMemberWithWriteStrongConsistencyTrue =
        newDataGroupMemberWithSyncLeaderTrue(TestUtils.getNode(0), false);
    ClusterDescriptor.getInstance()
        .getConfig()
        .setConsistencyLevel(ConsistencyLevel.STRONG_CONSISTENCY);
    try {

      PartitionedSnapshotLogManager partitionedSnapshotLogManager =
          Mockito.mock(PartitionedSnapshotLogManager.class);
      Mockito.when(partitionedSnapshotLogManager.getMaxHaveAppliedCommitIndex()).thenReturn(1000L);
      dataGroupMemberWithWriteStrongConsistencyTrue.setLogManager(partitionedSnapshotLogManager);

      dataGroupMemberWithWriteStrongConsistencyTrue.waitUntilCatchUp(
          new RaftMember.StrongCheckConsistency());
    } catch (CheckConsistencyException e) {
      Assert.fail();
    }
  }

  @Test
  public void testsyncLeaderMidConsistencyCheckFalse() {
    // 1. write request : Strong consistency level with syncLeader false
    DataGroupMember dataGroupMemberWithWriteStrongConsistencyFalse =
        newDataGroupMemberWithSyncLeaderFalse(TestUtils.getNode(0), false);
    ClusterDescriptor.getInstance()
        .getConfig()
        .setConsistencyLevel(ConsistencyLevel.MID_CONSISTENCY);
    ClusterDescriptor.getInstance().getConfig().setMaxReadLogLag(1);
    try {

      PartitionedSnapshotLogManager partitionedSnapshotLogManager =
          Mockito.mock(PartitionedSnapshotLogManager.class);
      Mockito.when(partitionedSnapshotLogManager.getMaxHaveAppliedCommitIndex()).thenReturn(-2L);
      dataGroupMemberWithWriteStrongConsistencyFalse.setLogManager(partitionedSnapshotLogManager);

      dataGroupMemberWithWriteStrongConsistencyFalse.waitUntilCatchUp(
          new RaftMember.MidCheckConsistency());
    } catch (CheckConsistencyException e) {
      Assert.assertEquals(CheckConsistencyException.CHECK_MID_CONSISTENCY_EXCEPTION, e);
    }
  }

  @Test
  public void testsyncLeaderMidConsistencyCheckTrue() {
    // 1. write request : Strong consistency level with syncLeader false
    DataGroupMember dataGroupMemberWithWriteStrongConsistencyTrue =
        newDataGroupMemberWithSyncLeaderTrue(TestUtils.getNode(0), false);
    ClusterDescriptor.getInstance()
        .getConfig()
        .setConsistencyLevel(ConsistencyLevel.MID_CONSISTENCY);
    ClusterDescriptor.getInstance().getConfig().setMaxReadLogLag(500);
    try {

      PartitionedSnapshotLogManager partitionedSnapshotLogManager =
          Mockito.mock(PartitionedSnapshotLogManager.class);
      Mockito.when(partitionedSnapshotLogManager.getMaxHaveAppliedCommitIndex()).thenReturn(600L);
      dataGroupMemberWithWriteStrongConsistencyTrue.setLogManager(partitionedSnapshotLogManager);

      dataGroupMemberWithWriteStrongConsistencyTrue.waitUntilCatchUp(
          new RaftMember.MidCheckConsistency());
    } catch (CheckConsistencyException e) {
      Assert.fail();
    }
  }

  @Test
  public void testsyncLeaderWeakConsistencyCheckFalse() {
    // 1. write request : Strong consistency level with syncLeader false
    DataGroupMember dataGroupMemberWithWriteStrongConsistencyFalse =
        newDataGroupMemberWithSyncLeaderFalse(TestUtils.getNode(0), false);
    ClusterDescriptor.getInstance()
        .getConfig()
        .setConsistencyLevel(ConsistencyLevel.WEAK_CONSISTENCY);
    ClusterDescriptor.getInstance().getConfig().setMaxReadLogLag(1);
    try {

      PartitionedSnapshotLogManager partitionedSnapshotLogManager =
          Mockito.mock(PartitionedSnapshotLogManager.class);
      Mockito.when(partitionedSnapshotLogManager.getMaxHaveAppliedCommitIndex()).thenReturn(-2L);
      dataGroupMemberWithWriteStrongConsistencyFalse.setLogManager(partitionedSnapshotLogManager);

      dataGroupMemberWithWriteStrongConsistencyFalse.waitUntilCatchUp(null);
    } catch (CheckConsistencyException e) {
      Assert.fail();
    }
  }

  @Test
  public void testsyncLeaderWeakConsistencyCheckTrue() {
    // 1. write request : Strong consistency level with syncLeader false
    DataGroupMember dataGroupMemberWithWriteStrongConsistencyTrue =
        newDataGroupMemberWithSyncLeaderTrue(TestUtils.getNode(0), false);
    ClusterDescriptor.getInstance()
        .getConfig()
        .setConsistencyLevel(ConsistencyLevel.WEAK_CONSISTENCY);
    ClusterDescriptor.getInstance().getConfig().setMaxReadLogLag(500);
    try {

      PartitionedSnapshotLogManager partitionedSnapshotLogManager =
          Mockito.mock(PartitionedSnapshotLogManager.class);
      Mockito.when(partitionedSnapshotLogManager.getMaxHaveAppliedCommitIndex()).thenReturn(600L);
      dataGroupMemberWithWriteStrongConsistencyTrue.setLogManager(partitionedSnapshotLogManager);

      dataGroupMemberWithWriteStrongConsistencyTrue.waitUntilCatchUp(null);
    } catch (CheckConsistencyException e) {
      Assert.fail();
    }
  }

  private DataGroupMember newDataGroupMemberWithSyncLeaderFalse(Node node, boolean syncLeader) {
    DataGroupMember newMember =
        new TestDataGroupMember(node, partitionTable.getHeaderGroup(node)) {

          @Override
          public boolean syncLeader(RaftMember.CheckConsistency checkConsistency) {
            return syncLeader;
          }

          @Override
          protected RequestCommitIndexResponse requestCommitIdAsync() {
            return new RequestCommitIndexResponse(5, 5, 5);
          }

          @Override
          public long appendEntry(AppendEntryRequest request) {
            return Response.RESPONSE_AGREE;
          }

          @Override
          public RaftService.AsyncClient getAsyncClient(Node node) {
            try {
              return new TestAsyncDataClient(node, dataGroupMemberMap);
            } catch (IOException e) {
              return null;
            }
          }
        };
    newMember.setThisNode(node);
    newMember.setMetaGroupMember(testMetaMember);
    newMember.setLeader(node);
    newMember.setCharacter(NodeCharacter.LEADER);
    newMember.setLogManager(new TestPartitionedLogManager());
    newMember.setAppendLogThreadPool(testThreadPool);
    return newMember;
  }

  private DataGroupMember newDataGroupMemberWithSyncLeaderTrue(Node node, boolean syncLeader) {
    DataGroupMember newMember =
        new TestDataGroupMember(node, partitionTable.getHeaderGroup(node)) {

          @Override
          public boolean syncLeader(RaftMember.CheckConsistency checkConsistency) {
            return syncLeader;
          }

          @Override
          protected RequestCommitIndexResponse requestCommitIdAsync() {
            return new RequestCommitIndexResponse(1000, 1000, 1000);
          }

          @Override
          public long appendEntry(AppendEntryRequest request) {
            return Response.RESPONSE_AGREE;
          }

          @Override
          public RaftService.AsyncClient getAsyncClient(Node node) {
            try {
              return new TestAsyncDataClient(node, dataGroupMemberMap);
            } catch (IOException e) {
              return null;
            }
          }
        };
    newMember.setThisNode(node);
    newMember.setMetaGroupMember(testMetaMember);
    newMember.setLeader(node);
    newMember.setCharacter(NodeCharacter.LEADER);
    newMember.setLogManager(new TestPartitionedLogManager());
    newMember.setAppendLogThreadPool(testThreadPool);
    return newMember;
  }
}
