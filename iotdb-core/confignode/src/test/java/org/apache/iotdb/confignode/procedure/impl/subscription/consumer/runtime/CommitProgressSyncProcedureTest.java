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

package org.apache.iotdb.confignode.procedure.impl.subscription.consumer.runtime;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.confignode.consensus.request.write.subscription.consumer.runtime.CommitProgressHandleMetaChangePlan;
import org.apache.iotdb.confignode.manager.ConfigManager;
import org.apache.iotdb.confignode.manager.consensus.ConsensusManager;
import org.apache.iotdb.confignode.persistence.subscription.SubscriptionInfo;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.mpp.rpc.thrift.TPullCommitProgressResp;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.rpc.subscription.exception.SubscriptionException;
import org.apache.iotdb.rpc.subscription.payload.poll.RegionProgress;
import org.apache.iotdb.rpc.subscription.payload.poll.WriterId;
import org.apache.iotdb.rpc.subscription.payload.poll.WriterProgress;

import org.junit.Test;
import org.mockito.Mockito;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class CommitProgressSyncProcedureTest {

  @Test
  public void requiredSyncShouldRejectFailedResponseBeforeConsensusWrite() throws Exception {
    final ConfigNodeProcedureEnv env = Mockito.mock(ConfigNodeProcedureEnv.class);
    final Map<Integer, TPullCommitProgressResp> responses = new LinkedHashMap<>();
    responses.put(
        2,
        new TPullCommitProgressResp(
            new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode())));
    Mockito.when(env.pullCommitProgressFromDataNodes()).thenReturn(responses);

    try {
      CommitProgressSyncProcedure.syncCommitProgressFromDataNodesRequired(
          env, new SubscriptionInfo());
      fail();
    } catch (final SubscriptionException e) {
      assertTrue(e.getMessage().contains("DataNode 2"));
    }

    Mockito.verify(env, Mockito.never()).getConfigManager();
  }

  @Test
  public void requiredSyncShouldPersistEmptyProgressAndMergeByMaximum() throws Exception {
    final String emptyProgressKey = "empty_progress";
    final String mergedProgressKey = "merged_progress";
    final WriterId firstWriter = new WriterId("DataRegion[1]", 1);
    final WriterId secondWriter = new WriterId("DataRegion[1]", 2);

    final Map<WriterId, WriterProgress> existingWriterProgress = new LinkedHashMap<>();
    existingWriterProgress.put(firstWriter, new WriterProgress(200, 1));
    final SubscriptionInfo subscriptionInfo = new SubscriptionInfo();
    subscriptionInfo
        .getCommitProgressKeeper()
        .updateRegionProgress(
            mergedProgressKey, serialize(new RegionProgress(existingWriterProgress)));

    final Map<WriterId, WriterProgress> incomingWriterProgress = new LinkedHashMap<>();
    incomingWriterProgress.put(firstWriter, new WriterProgress(100, 2));
    incomingWriterProgress.put(secondWriter, new WriterProgress(300, 3));
    final Map<String, ByteBuffer> dataNodeProgress = new LinkedHashMap<>();
    dataNodeProgress.put(emptyProgressKey, serialize(new RegionProgress(Collections.emptyMap())));
    dataNodeProgress.put(mergedProgressKey, serialize(new RegionProgress(incomingWriterProgress)));

    final ConfigNodeProcedureEnv env = Mockito.mock(ConfigNodeProcedureEnv.class);
    Mockito.when(env.pullCommitProgressFromDataNodes())
        .thenReturn(
            Collections.singletonMap(
                1,
                new TPullCommitProgressResp(
                        new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()))
                    .setCommitRegionProgress(dataNodeProgress)));
    final ConfigManager configManager = Mockito.mock(ConfigManager.class);
    final ConsensusManager consensusManager = Mockito.mock(ConsensusManager.class);
    Mockito.when(env.getConfigManager()).thenReturn(configManager);
    Mockito.when(configManager.getConsensusManager()).thenReturn(consensusManager);
    final AtomicReference<CommitProgressHandleMetaChangePlan> writtenPlan = new AtomicReference<>();
    Mockito.when(consensusManager.write(Mockito.any()))
        .thenAnswer(
            invocation -> {
              writtenPlan.set((CommitProgressHandleMetaChangePlan) invocation.getArgument(0));
              return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
            });

    CommitProgressSyncProcedure.syncCommitProgressFromDataNodesRequired(env, subscriptionInfo);

    final Map<String, ByteBuffer> persistedProgress = writtenPlan.get().getRegionProgressMap();
    assertTrue(persistedProgress.containsKey(emptyProgressKey));
    assertEquals(
        new RegionProgress(Collections.emptyMap()),
        RegionProgress.deserialize(persistedProgress.get(emptyProgressKey).slice()));

    final Map<WriterId, WriterProgress> expectedWriterProgress = new LinkedHashMap<>();
    expectedWriterProgress.put(firstWriter, new WriterProgress(200, 1));
    expectedWriterProgress.put(secondWriter, new WriterProgress(300, 3));
    assertEquals(
        new RegionProgress(expectedWriterProgress),
        RegionProgress.deserialize(persistedProgress.get(mergedProgressKey).slice()));
    Mockito.verify(env, Mockito.never()).pullCommitProgressFromDataNodesBestEffort();
  }

  private static ByteBuffer serialize(final RegionProgress regionProgress) throws Exception {
    try (final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final DataOutputStream dos = new DataOutputStream(baos)) {
      regionProgress.serialize(dos);
      dos.flush();
      return ByteBuffer.wrap(baos.toByteArray()).asReadOnlyBuffer();
    }
  }
}
