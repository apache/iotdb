/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * License); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an AS IS basis,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iotdb.confignode.procedure.impl.schema;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.mpp.rpc.thrift.TDropTableModProgress;
import org.apache.iotdb.mpp.rpc.thrift.TGetDropTableModProgressResp;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class ModFileDeletionProgressTest {
  @Test
  public void finalRefreshTargetsEveryReplicaButLiveRefreshUsesExecutorTarget() {
    final TConsensusGroupId region = new TConsensusGroupId(TConsensusGroupType.DataRegion, 1);
    final TDataNodeLocation firstReplica = dataNodeLocation(1);
    final TDataNodeLocation secondReplica = dataNodeLocation(2);
    final TRegionReplicaSet replicaSet =
        new TRegionReplicaSet(region, Arrays.asList(firstReplica, secondReplica));
    final ModFileDeletionProgress progress = new ModFileDeletionProgress(() -> 1, () -> true);
    progress.setRegionCount(1);
    progress.trackRegionReplicas(Map.of(region, replicaSet));
    progress.trackRegions(firstReplica, Collections.singletonList(region));

    assertEquals(
        Map.of(firstReplica, Collections.singletonList(region)), progress.getRefreshTargets(false));
    assertEquals(
        Map.of(
            firstReplica,
            Collections.singletonList(region),
            secondReplica,
            Collections.singletonList(region)),
        progress.getRefreshTargets(true));
  }

  @Test
  public void failedFinalRefreshRetainsTargetsAcrossSerializationAndRetries() throws Exception {
    final TConsensusGroupId region = new TConsensusGroupId(TConsensusGroupType.DataRegion, 1);
    final TDataNodeLocation firstReplica = dataNodeLocation(1);
    final TDataNodeLocation secondReplica = dataNodeLocation(2);
    final TRegionReplicaSet replicaSet =
        new TRegionReplicaSet(region, Arrays.asList(firstReplica, secondReplica));
    final AtomicBoolean omitSecondResponse = new AtomicBoolean(true);
    final List<Set<Integer>> requestNodeIds = new ArrayList<>();
    final ModFileDeletionProgress progress =
        new ModFileDeletionProgress(
            () -> 9,
            () -> true,
            requestContext -> {
              final Set<Integer> nodeIds = new HashSet<>(requestContext.getRequestIndices());
              requestNodeIds.add(nodeIds);
              for (final int nodeId : nodeIds) {
                assertTrue(requestContext.getRequest(nodeId).isClearProgress());
                if (nodeId == 2 && omitSecondResponse.get()) {
                  continue;
                }
                requestContext
                    .getResponseMap()
                    .put(
                        nodeId,
                        new TGetDropTableModProgressResp(
                                new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()))
                            .setRegionProgress(
                                Map.of(
                                    region.getId(),
                                    new TDropTableModProgress(5).setRemainingModFileCount(0))));
              }
            });
    progress.setRegionCount(1);
    progress.trackRegionReplicas(Map.of(region, replicaSet));
    progress.trackRegions(firstReplica, Collections.singletonList(region));

    final AtomicBoolean advanced = new AtomicBoolean();
    progress.finish(() -> advanced.set(true));

    assertTrue(advanced.get());
    assertTrue(progress.hasPendingCleanup());
    assertEquals(Collections.singletonList(Set.of(1, 2)), requestNodeIds);
    assertEquals(Collections.emptyMap(), progress.getRefreshTargets(false));
    assertEquals(
        Map.of(
            firstReplica,
            Collections.singletonList(region),
            secondReplica,
            Collections.singletonList(region)),
        progress.getRefreshTargets(true));

    final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    progress.serialize(new DataOutputStream(bytes));
    omitSecondResponse.set(false);
    final List<Integer> retryNodeIds = new ArrayList<>();
    final ModFileDeletionProgress recovered =
        new ModFileDeletionProgress(
            () -> 9,
            () -> false,
            requestContext -> {
              for (final int nodeId : requestContext.getRequestIndices()) {
                retryNodeIds.add(nodeId);
                assertTrue(requestContext.getRequest(nodeId).isClearProgress());
                requestContext
                    .getResponseMap()
                    .put(
                        nodeId,
                        new TGetDropTableModProgressResp(
                            new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode())));
              }
            });
    recovered.deserialize(ByteBuffer.wrap(bytes.toByteArray()));

    assertTrue(recovered.hasPendingCleanup());
    assertEquals(new TDropTableModProgress(5).setRemainingModFileCount(0), recovered.snapshot());
    recovered.update();

    assertFalse(recovered.hasPendingCleanup());
    assertEquals(Set.of(1, 2), new HashSet<>(retryNodeIds));
    assertEquals(Collections.emptyMap(), recovered.getRefreshTargets(true));
  }

  @Test
  public void unsuccessfulFinalRefreshResponseKeepsCleanupPending() {
    final TConsensusGroupId region = new TConsensusGroupId(TConsensusGroupType.DataRegion, 5);
    final TDataNodeLocation firstReplica = dataNodeLocation(5);
    final TDataNodeLocation secondReplica = dataNodeLocation(6);
    final TRegionReplicaSet replicaSet =
        new TRegionReplicaSet(region, Arrays.asList(firstReplica, secondReplica));
    final ModFileDeletionProgress progress =
        new ModFileDeletionProgress(
            () -> 12,
            () -> true,
            requestContext -> {
              for (final int nodeId : requestContext.getRequestIndices()) {
                requestContext
                    .getResponseMap()
                    .put(
                        nodeId,
                        new TGetDropTableModProgressResp(
                            new TSStatus(
                                nodeId == secondReplica.getDataNodeId()
                                    ? TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode()
                                    : TSStatusCode.SUCCESS_STATUS.getStatusCode())));
              }
            });
    progress.setRegionCount(1);
    progress.trackRegionReplicas(Map.of(region, replicaSet));
    progress.trackRegions(firstReplica, Collections.singletonList(region));
    final AtomicBoolean advanced = new AtomicBoolean();

    progress.finish(() -> advanced.set(true));

    assertTrue(advanced.get());
    assertTrue(progress.hasPendingCleanup());
    assertEquals(2, progress.getRefreshTargets(true).values().stream().mapToInt(List::size).sum());
  }

  @Test
  public void serializationKeepsCleanupTargetsWhenConcurrentRetryCompletes() throws Exception {
    final TConsensusGroupId region = new TConsensusGroupId(TConsensusGroupType.DataRegion, 3);
    final TDataNodeLocation firstReplica = dataNodeLocation(3);
    final TDataNodeLocation secondReplica = dataNodeLocation(4);
    final TRegionReplicaSet replicaSet =
        new TRegionReplicaSet(region, Arrays.asList(firstReplica, secondReplica));
    final AtomicBoolean omitCleanupResponse = new AtomicBoolean(true);
    final ModFileDeletionProgress progress =
        new ModFileDeletionProgress(
            () -> 11,
            () -> true,
            requestContext -> {
              for (final int nodeId : requestContext.getRequestIndices()) {
                final boolean clearProgress = requestContext.getRequest(nodeId).isClearProgress();
                if (clearProgress
                    && nodeId == secondReplica.getDataNodeId()
                    && omitCleanupResponse.get()) {
                  continue;
                }
                final TGetDropTableModProgressResp response =
                    new TGetDropTableModProgressResp(
                        new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()));
                if (!clearProgress) {
                  response.setRegionProgress(
                      Map.of(
                          region.getId(),
                          new TDropTableModProgress(2).setRemainingModFileCount(0)));
                }
                requestContext.getResponseMap().put(nodeId, response);
              }
            });
    progress.setRegionCount(1);
    progress.trackRegionReplicas(Map.of(region, replicaSet));
    progress.trackRegions(firstReplica, Collections.singletonList(region));
    progress.update();
    progress.finish(() -> {});
    assertTrue(progress.hasPendingCleanup());

    final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    final AtomicBoolean retryCleanup = new AtomicBoolean();
    final OutputStream outputStream =
        new OutputStream() {
          private int writtenBytes;

          @Override
          public void write(final int value) {
            bytes.write(value);
            triggerCleanup(1);
          }

          @Override
          public void write(final byte[] buffer, final int offset, final int length) {
            bytes.write(buffer, offset, length);
            triggerCleanup(length);
          }

          private void triggerCleanup(final int length) {
            writtenBytes += length;
            if (writtenBytes >= 18 && retryCleanup.compareAndSet(false, true)) {
              omitCleanupResponse.set(false);
              progress.update();
            }
          }
        };
    progress.serialize(new DataOutputStream(outputStream));

    assertFalse(progress.hasPendingCleanup());
    final ModFileDeletionProgress recovered =
        new ModFileDeletionProgress(() -> 11, () -> false, requestContext -> {});
    recovered.deserialize(ByteBuffer.wrap(bytes.toByteArray()));

    assertTrue(recovered.hasPendingCleanup());
    assertEquals(
        Map.of(
            firstReplica,
            Collections.singletonList(region),
            secondReplica,
            Collections.singletonList(region)),
        recovered.getRefreshTargets(true));
  }

  @Test
  public void staleRefreshFromPreviousReplicaIsIgnoredAfterTargetSwitch() {
    final TConsensusGroupId region = new TConsensusGroupId(TConsensusGroupType.DataRegion, 2);
    final TDataNodeLocation firstReplica = dataNodeLocation(1);
    final TDataNodeLocation secondReplica = dataNodeLocation(2);
    final AtomicReference<ModFileDeletionProgress> progressReference = new AtomicReference<>();
    final AtomicBoolean switchReplica = new AtomicBoolean(true);
    final ModFileDeletionProgress progress =
        new ModFileDeletionProgress(
            () -> 10,
            () -> true,
            requestContext -> {
              final int nodeId = requestContext.getRequestIndices().get(0);
              if (switchReplica.getAndSet(false)) {
                progressReference
                    .get()
                    .trackRegions(secondReplica, Collections.singletonList(region));
              }
              requestContext
                  .getResponseMap()
                  .put(
                      nodeId,
                      new TGetDropTableModProgressResp(
                              new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()))
                          .setRegionProgress(
                              Map.of(region.getId(), new TDropTableModProgress(nodeId))));
            });
    progressReference.set(progress);
    progress.setRegionCount(1);
    progress.trackRegions(firstReplica, Collections.singletonList(region));

    progress.update();
    assertNull(progress.snapshot());

    progress.update();
    assertEquals(new TDropTableModProgress(2), progress.snapshot());
  }

  private static TDataNodeLocation dataNodeLocation(final int nodeId) {
    return new TDataNodeLocation()
        .setDataNodeId(nodeId)
        .setClientRpcEndPoint(new TEndPoint("127.0.0.1", 6660 + nodeId))
        .setInternalEndPoint(new TEndPoint("127.0.0.1", 10730 + nodeId))
        .setMPPDataExchangeEndPoint(new TEndPoint("127.0.0.1", 10740 + nodeId))
        .setDataRegionConsensusEndPoint(new TEndPoint("127.0.0.1", 10750 + nodeId))
        .setSchemaRegionConsensusEndPoint(new TEndPoint("127.0.0.1", 10760 + nodeId));
  }
}
