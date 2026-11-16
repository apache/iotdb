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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iotdb.pipe.it.single;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.consensus.ConsensusGroupId;
import org.apache.iotdb.commons.consensus.SchemaRegionId;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.consensus.IConsensus;
import org.apache.iotdb.consensus.common.Peer;
import org.apache.iotdb.consensus.config.ConsensusConfig;
import org.apache.iotdb.consensus.config.RatisConfig;
import org.apache.iotdb.db.consensus.statemachine.schemaregion.SchemaRegionStateMachine;
import org.apache.iotdb.db.pipe.agent.PipeDataNodeAgent;
import org.apache.iotdb.db.pipe.source.schemaregion.SchemaRegionListeningQueue;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.write.CreateTimeSeriesNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.pipe.PipeOperateSchemaQueueNode;
import org.apache.iotdb.db.schemaengine.schemaregion.ISchemaRegion;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;

import org.apache.ratis.util.ExitUtils;
import org.apache.ratis.util.TimeDuration;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.awaitility.Awaitility;
import org.junit.After;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Proxy;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

@Category({LocalStandaloneIT.class})
public class PipeSchemaRegionSnapshotRecoveryIT {

  @Rule public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  @After
  public void clearRatisExitState() {
    ExitUtils.clear();
  }

  @Test
  public void testInstallingSnapshotWithoutListenerClosesStaleQueue() throws Exception {
    ExitUtils.disableSystemExit();

    final SchemaRegionId schemaRegionId = new SchemaRegionId(Integer.MAX_VALUE - 17666);
    final int leaderPort = freePort();
    int followerPort = freePort();
    while (followerPort == leaderPort) {
      followerPort = freePort();
    }
    final Peer leaderPeer = new Peer(schemaRegionId, 0, new TEndPoint("127.0.0.1", leaderPort));
    final Peer followerPeer = new Peer(schemaRegionId, 1, new TEndPoint("127.0.0.1", followerPort));
    final CountDownLatch leaderSnapshotCreated = new CountDownLatch(1);
    final CountDownLatch followerSnapshotLoaded = new CountDownLatch(1);
    final AtomicReference<File> leaderSnapshotDir = new AtomicReference<>();
    final AtomicBoolean followerSnapshotLoadSucceeded = new AtomicBoolean();
    final AtomicReference<SchemaRegionListeningQueue> staleQueueRef = new AtomicReference<>();
    final ISchemaRegion schemaRegion =
        newSchemaRegion(schemaRegionId, leaderSnapshotDir, leaderSnapshotCreated);

    final RatisConfig ratisConfig =
        RatisConfig.newBuilder()
            .setRpc(
                RatisConfig.Rpc.newBuilder()
                    .setFirstElectionTimeoutMin(TimeDuration.valueOf(1, TimeUnit.SECONDS))
                    .setFirstElectionTimeoutMax(TimeDuration.valueOf(4, TimeUnit.SECONDS))
                    .setTimeoutMin(TimeDuration.valueOf(1, TimeUnit.SECONDS))
                    .setTimeoutMax(TimeDuration.valueOf(4, TimeUnit.SECONDS))
                    .build())
            .setLog(RatisConfig.Log.newBuilder().setPurgeUptoSnapshotIndex(true).build())
            .build();
    final IConsensus leader =
        newConsensus(
            leaderPeer,
            ratisConfig,
            schemaRegion,
            new File(temporaryFolder.getRoot(), "leader"),
            null,
            null);
    final IConsensus follower =
        newConsensus(
            followerPeer,
            ratisConfig,
            schemaRegion,
            new File(temporaryFolder.getRoot(), "follower"),
            followerSnapshotLoaded,
            followerSnapshotLoadSucceeded);
    boolean leaderStarted = false;
    boolean followerStarted = false;

    try {
      leader.start();
      leaderStarted = true;
      follower.start();
      followerStarted = true;

      leader.createLocalPeer(schemaRegionId, Collections.singletonList(leaderPeer));
      Awaitility.await()
          .atMost(30, TimeUnit.SECONDS)
          .until(() -> leader.isLeaderReady(schemaRegionId));

      for (int i = 0; i < 10; i++) {
        Assert.assertEquals(
            200,
            leader
                .write(
                    schemaRegionId,
                    new PipeOperateSchemaQueueNode(new PlanNodeId("close" + i), false))
                .getCode());
      }
      leader.triggerSnapshot(schemaRegionId, true);
      Assert.assertTrue(leaderSnapshotCreated.await(30, TimeUnit.SECONDS));
      Assert.assertFalse(SchemaRegionListeningQueue.hasSnapshot(leaderSnapshotDir.get()));

      final SchemaRegionListeningQueue staleQueue =
          PipeDataNodeAgent.runtime().schemaListener(schemaRegionId);
      staleQueueRef.set(staleQueue);
      PipeDataNodeAgent.runtime().increaseAndGetSchemaListenerReferenceCount(schemaRegionId);
      staleQueue.open();
      staleQueue.tryListenToNode(
          new CreateTimeSeriesNode(
              new PlanNodeId("stale"),
              new MeasurementPath("root.db.d1.s1"),
              TSDataType.INT32,
              TSEncoding.PLAIN,
              CompressionType.GZIP,
              null,
              null,
              null,
              "alias"));
      Assert.assertEquals(1, staleQueue.getSize());

      follower.createLocalPeer(schemaRegionId, Collections.emptyList());
      leader.addRemotePeer(schemaRegionId, followerPeer);

      Assert.assertTrue(followerSnapshotLoaded.await(30, TimeUnit.SECONDS));
      Assert.assertTrue(followerSnapshotLoadSucceeded.get());
      Assert.assertFalse(staleQueue.isOpened());
      Assert.assertEquals(0, staleQueue.getSize());
      Assert.assertSame(
          staleQueue, PipeDataNodeAgent.runtime().schemaListenerIfPresent(schemaRegionId));
      Assert.assertEquals(
          0,
          PipeDataNodeAgent.runtime().decreaseAndGetSchemaListenerReferenceCount(schemaRegionId));
      Assert.assertNull(PipeDataNodeAgent.runtime().schemaListenerIfPresent(schemaRegionId));
    } finally {
      if (followerStarted) {
        follower.stop();
      }
      if (leaderStarted) {
        leader.stop();
      }
      if (staleQueueRef.get() != null) {
        staleQueueRef.get().close();
      }
      PipeDataNodeAgent.runtime().clearSchemaRegionState(schemaRegionId);
    }
  }

  private ISchemaRegion newSchemaRegion(
      final SchemaRegionId schemaRegionId,
      final AtomicReference<File> snapshotDir,
      final CountDownLatch snapshotCreated) {
    return (ISchemaRegion)
        Proxy.newProxyInstance(
            ISchemaRegion.class.getClassLoader(),
            new Class<?>[] {ISchemaRegion.class},
            (proxy, method, args) -> {
              switch (method.getName()) {
                case "getSchemaRegionId":
                  return schemaRegionId;
                case "createSnapshot":
                  final File dir = (File) args[0];
                  Files.createDirectories(dir.toPath());
                  Files.writeString(new File(dir, "schema.snapshot").toPath(), "schema");
                  snapshotDir.set(dir);
                  snapshotCreated.countDown();
                  return true;
                case "loadSnapshot":
                  return new File((File) args[0], "schema.snapshot").isFile();
                default:
                  throw new UnsupportedOperationException(method.toString());
              }
            });
  }

  private IConsensus newConsensus(
      final Peer peer,
      final RatisConfig ratisConfig,
      final ISchemaRegion schemaRegion,
      final File storageDir,
      final CountDownLatch snapshotLoaded,
      final AtomicBoolean snapshotLoadSucceeded) {
    return ConsensusFactory.getConsensusImpl(
            ConsensusFactory.RATIS_CONSENSUS,
            ConsensusConfig.newBuilder()
                .setThisNodeId(peer.getNodeId())
                .setThisNode(peer.getEndpoint())
                .setRatisConfig(ratisConfig)
                .setStorageDir(storageDir.getAbsolutePath())
                .setConsensusGroupType(TConsensusGroupType.SchemaRegion)
                .build(),
            ignored ->
                new QuietSchemaRegionStateMachine(
                    schemaRegion, snapshotLoaded, snapshotLoadSucceeded))
        .orElseThrow(IllegalStateException::new);
  }

  private int freePort() throws IOException {
    try (ServerSocket socket = new ServerSocket(0)) {
      return socket.getLocalPort();
    }
  }

  private static class QuietSchemaRegionStateMachine extends SchemaRegionStateMachine {

    private final CountDownLatch snapshotLoaded;
    private final AtomicBoolean snapshotLoadSucceeded;

    private QuietSchemaRegionStateMachine(
        final ISchemaRegion schemaRegion,
        final CountDownLatch snapshotLoaded,
        final AtomicBoolean snapshotLoadSucceeded) {
      super(schemaRegion);
      this.snapshotLoaded = snapshotLoaded;
      this.snapshotLoadSucceeded = snapshotLoadSucceeded;
    }

    @Override
    public boolean loadSnapshot(final File snapshotDir) {
      final boolean succeeded = super.loadSnapshot(snapshotDir);
      if (snapshotLoaded != null) {
        snapshotLoadSucceeded.set(succeeded);
        snapshotLoaded.countDown();
      }
      return succeeded;
    }

    @Override
    public void notifyLeaderChanged(final ConsensusGroupId groupId, final int newLeaderId) {}

    @Override
    public void notifyLeaderReady() {}

    @Override
    public void notifyNotLeader() {}

    @Override
    public void stop() {}
  }
}
