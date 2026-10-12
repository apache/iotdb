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

package org.apache.iotdb.consensus.iot.service;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.consensus.EmptyStateMachine;
import org.apache.iotdb.consensus.config.ConsensusConfig;
import org.apache.iotdb.consensus.i18n.ConsensusMessages;
import org.apache.iotdb.consensus.iot.IoTConsensus;
import org.apache.iotdb.consensus.iot.thrift.TActivatePeerReq;
import org.apache.iotdb.consensus.iot.thrift.TBuildSyncLogChannelReq;
import org.apache.iotdb.consensus.iot.thrift.TCleanupTransferredSnapshotReq;
import org.apache.iotdb.consensus.iot.thrift.TInactivatePeerReq;
import org.apache.iotdb.consensus.iot.thrift.TRemoveSyncLogChannelReq;
import org.apache.iotdb.consensus.iot.thrift.TSendSnapshotFragmentReq;
import org.apache.iotdb.consensus.iot.thrift.TSyncLogEntriesReq;
import org.apache.iotdb.consensus.iot.thrift.TSyncWriterSafeTimeBarrierReq;
import org.apache.iotdb.consensus.iot.thrift.TTriggerSnapshotLoadReq;
import org.apache.iotdb.consensus.iot.thrift.TWaitReleaseAllRegionRelatedResourceReq;
import org.apache.iotdb.consensus.iot.thrift.TWaitSyncLogCompleteReq;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.Collections;

public class IoTConsensusRPCServiceProcessorTest {

  private static final TConsensusGroupId MISSING_GROUP_ID =
      new TConsensusGroupId(TConsensusGroupType.DataRegion, 1);

  @Rule public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  private IoTConsensus consensus;
  private IoTConsensusRPCServiceProcessor processor;

  @Before
  public void setUp() throws IOException {
    consensus =
        new IoTConsensus(
            ConsensusConfig.newBuilder()
                .setThisNodeId(1)
                .setThisNode(new TEndPoint("127.0.0.1", 0))
                .setStorageDir(temporaryFolder.newFolder("storage").getAbsolutePath())
                .setRecvSnapshotDirs(
                    Collections.singletonList(
                        temporaryFolder.newFolder("snapshot").getAbsolutePath()))
                .setConsensusGroupType(TConsensusGroupType.DataRegion)
                .build(),
            groupId -> new EmptyStateMachine());
    processor = new IoTConsensusRPCServiceProcessor(consensus);
  }

  @After
  public void tearDown() {
    consensus.stop();
  }

  @Test
  public void testMissingConsensusGroupPreservesStatusAndMessage() throws Exception {
    assertMissingGroupStatus(
        processor
            .syncLogEntries(
                new TSyncLogEntriesReq()
                    .setConsensusGroupId(MISSING_GROUP_ID)
                    .setLogEntries(Collections.emptyList()))
            .getStatuses()
            .get(0),
        String.format(
            ConsensusMessages.UNEXPECTED_CONSENSUS_GROUP_ID_FOR_SYNC_LOG, "DataRegion[1]", 0));
    assertMissingGroupStatus(
        processor
            .syncWriterSafeTimeBarrier(
                new TSyncWriterSafeTimeBarrierReq().setConsensusGroupId(MISSING_GROUP_ID))
            .getStatus(),
        String.format(
            ConsensusMessages.UNEXPECTED_CONSENSUS_GROUP_ID_FOR_REQUEST,
            "DataRegion[1]",
            "TSyncWriterSafeTimeBarrierReq"));
    assertMissingGroupStatus(
        processor
            .inactivatePeer(new TInactivatePeerReq().setConsensusGroupId(MISSING_GROUP_ID))
            .getStatus(),
        String.format(
            ConsensusMessages.UNEXPECTED_CONSENSUS_GROUP_ID_FOR_REQUEST,
            "DataRegion[1]",
            "inactivatePeer"));
    assertMissingGroupStatus(
        processor
            .activatePeer(new TActivatePeerReq().setConsensusGroupId(MISSING_GROUP_ID))
            .getStatus(),
        String.format(
            ConsensusMessages.UNEXPECTED_CONSENSUS_GROUP_ID_FOR_REQUEST,
            "DataRegion[1]",
            "inactivatePeer"));
    assertMissingGroupStatus(
        processor
            .buildSyncLogChannel(
                new TBuildSyncLogChannelReq().setConsensusGroupId(MISSING_GROUP_ID))
            .getStatus(),
        String.format(
            ConsensusMessages.UNEXPECTED_CONSENSUS_GROUP_ID_FOR_REQUEST,
            "DataRegion[1]",
            "buildSyncLogChannel"));
    assertMissingGroupStatus(
        processor
            .removeSyncLogChannel(
                new TRemoveSyncLogChannelReq().setConsensusGroupId(MISSING_GROUP_ID))
            .getStatus(),
        String.format(
            ConsensusMessages.UNEXPECTED_CONSENSUS_GROUP_ID_FOR_REQUEST,
            "DataRegion[1]",
            "buildSyncLogChannel"));
    assertMissingGroupStatus(
        processor
            .sendSnapshotFragment(
                new TSendSnapshotFragmentReq().setConsensusGroupId(MISSING_GROUP_ID))
            .getStatus(),
        String.format(
            ConsensusMessages.UNEXPECTED_CONSENSUS_GROUP_ID_FOR_REQUEST,
            "DataRegion[1]",
            "buildSyncLogChannel"));
    assertMissingGroupStatus(
        processor
            .triggerSnapshotLoad(
                new TTriggerSnapshotLoadReq().setConsensusGroupId(MISSING_GROUP_ID))
            .getStatus(),
        String.format(
            ConsensusMessages.UNEXPECTED_CONSENSUS_GROUP_ID_FOR_REQUEST,
            "DataRegion[1]",
            "buildSyncLogChannel"));
    assertMissingGroupStatus(
        processor
            .cleanupTransferredSnapshot(
                new TCleanupTransferredSnapshotReq().setConsensusGroupId(MISSING_GROUP_ID))
            .getStatus(),
        String.format(
            ConsensusMessages.UNEXPECTED_CONSENSUS_GROUP_ID_FOR_REQUEST,
            "DataRegion[1]",
            "buildSyncLogChannel"));
  }

  @Test
  public void testMissingConsensusGroupCompletesIdempotentWaits() throws Exception {
    Assert.assertTrue(
        processor.waitSyncLogComplete(
                new TWaitSyncLogCompleteReq().setConsensusGroupId(MISSING_GROUP_ID))
            .complete);
    Assert.assertTrue(
        processor.waitReleaseAllRegionRelatedResource(
                new TWaitReleaseAllRegionRelatedResourceReq().setConsensusGroupId(MISSING_GROUP_ID))
            .releaseAllResource);
  }

  private static void assertMissingGroupStatus(TSStatus status, String expectedMessage) {
    Assert.assertEquals(TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode(), status.getCode());
    Assert.assertEquals(expectedMessage, status.getMessage());
  }
}
