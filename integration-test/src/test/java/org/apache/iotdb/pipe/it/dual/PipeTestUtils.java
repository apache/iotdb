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

package org.apache.iotdb.pipe.it.dual;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.commons.client.sync.SyncConfigNodeIServiceClient;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeMeta;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeStaticMeta;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeStatus;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeTaskMeta;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.confignode.rpc.thrift.TGetAllPipeInfoResp;
import org.apache.iotdb.confignode.rpc.thrift.TRegionInfo;
import org.apache.iotdb.confignode.rpc.thrift.TShowRegionReq;
import org.apache.iotdb.confignode.rpc.thrift.TShowRegionResp;
import org.apache.iotdb.db.it.utils.TestUtils;
import org.apache.iotdb.isession.ISession;
import org.apache.iotdb.it.env.cluster.node.DataNodeWrapper;
import org.apache.iotdb.itbase.env.BaseEnv;
import org.apache.iotdb.metrics.core.utils.IoTDBMetricObjNameFactory;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.rpc.UrlUtils;

import org.junit.Assert;

import javax.management.InstanceNotFoundException;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static org.awaitility.Awaitility.await;

public class PipeTestUtils {

  private static final long COMPLETION_REFLUSH_INTERVAL_MILLIS = 1000;

  private PipeTestUtils() {}

  /**
   * Flush both environments after failed assertion polls, lazily retaining one Session per side.
   * Close this handler before restarting either cluster and create a new one after recovery.
   */
  public static final class RetryFlushHandler implements Consumer<String>, AutoCloseable {
    private final BaseEnv[] environments;
    private final ISession[] sessions = new ISession[2];

    @TestOnly
    public RetryFlushHandler(final BaseEnv firstEnv, final BaseEnv secondEnv) {
      environments = new BaseEnv[] {firstEnv, secondEnv};
    }

    @Override
    public void accept(final String ignored) {
      // A successful first poll opens no clients; failed polls reuse Sessions instead of
      // repeatedly creating multi-node JDBC connections at the assertion's 100 ms interval.
      for (int i = 0; i < environments.length; i++) {
        try {
          if (sessions[i] == null) {
            sessions[i] = environments[i].getSessionConnection();
          }
          sessions[i].executeNonQueryStatement("flush");
        } catch (final Exception e) {
          throw new AssertionError(e);
        }
      }
    }

    @Override
    public void close() throws Exception {
      // Always close the second side even if closing the first Session fails.
      try {
        if (sessions[0] != null) {
          sessions[0].close();
        }
      } finally {
        if (sessions[1] != null) {
          sessions[1].close();
        }
      }
    }
  }

  /** Check every supplied IoTConsensus replica using live JMX gauges; enable its JMX reporter. */
  public static void assertDataRegionReplicationComplete(
      final BaseEnv env, final List<TRegionInfo> replicas) throws Exception {
    Assert.assertFalse("No DataRegion replicas to check", replicas.isEmpty());
    // Node -> region ids: read all of a node's replicas through the same connection per sample.
    final Map<DataNodeWrapper, List<Integer>> regionsByNode = new HashMap<>();
    for (final TRegionInfo replica : replicas) {
      final DataNodeWrapper node =
          env.getDataNodeWrapperList().stream()
              .filter(wrapper -> wrapper.getPort() == replica.getClientRpcPort())
              .findFirst()
              .orElseThrow(() -> new AssertionError("Unknown DataRegion endpoint: " + replica));
      regionsByNode
          .computeIfAbsent(node, ignored -> new ArrayList<>())
          .add(replica.getConsensusGroupId().getId());
    }
    for (final Map.Entry<DataNodeWrapper, List<Integer>> entry : regionsByNode.entrySet()) {
      final DataNodeWrapper node = entry.getKey();
      final JMXServiceURL url =
          new JMXServiceURL(
              "service:jmx:rmi:///jndi/rmi://"
                  + UrlUtils.formatTEndPointIpv4AndIpv6Url(node.getIp(), node.getJmxPort())
                  + "/jmxrmi");
      // Prometheus snapshots can retain either an old lag or an old zero after new writes.
      // Read the AutoGauge directly before allowing a flush or restart of these replicas.
      try (final JMXConnector connector = JMXConnectorFactory.connect(url)) {
        for (final int regionId : entry.getValue()) {
          final ObjectName metric =
              IoTDBMetricObjNameFactory.getInstance()
                  .createName(
                      "IoTDBAutoGauge",
                      "org.apache.iotdb.metrics",
                      "iot_consensus",
                      Map.of(
                          "name", "ioTConsensusServerImpl",
                          "region", "DataRegion[" + regionId + "]",
                          "type", "syncLag"));
          final Object value = connector.getMBeanServerConnection().getAttribute(metric, "Value");
          if (!(value instanceof Number lag)) {
            throw new AssertionError("Non-numeric replication lag for " + metric + " at " + url);
          }
          Assert.assertEquals(
              "Unreplicated data for " + metric + " at " + url, 0.0, lag.doubleValue(), 0.001);
        }
      } catch (final InstanceNotFoundException | IOException e) {
        // A restarted replica may expose JMX before rebinding its region metrics. Retry within
        // the caller's deadline, retaining the endpoint and underlying failure for diagnosis.
        throw new AssertionError("Replication metrics unavailable at " + url, e);
      }
    }
  }

  /**
   * Wait for committed full-flush barriers, then let the caller verify the receiver's exact result.
   * The caller must stop writing and use a running, completion-capable pipe capturing every
   * existing DataRegion. Enable the sender's JMX reporter before cluster startup. This proves data
   * completion only; it cannot replace observation of paused pipes, rejected events, or schema/auth
   * synchronization.
   */
  public static void awaitDataPipeCompletion(final BaseEnv senderEnv, final String pipeName)
      throws Exception {
    final CompletionSnapshot initial = readCompletionSnapshot(senderEnv, pipeName);
    // RemainingEventCount can briefly be zero before capture. A full flush publishes an ordered
    // barrier behind all preceding data, and completion_ready requires its successful commit.
    TestUtils.executeNonQuery(senderEnv, "flush", null);
    final long[] lastFlushTimeNanos = {System.nanoTime()};
    await()
        .alias("Committed data completion of pipe " + pipeName)
        .pollDelay(0, TimeUnit.MILLISECONDS)
        .pollInterval(100, TimeUnit.MILLISECONDS)
        .atMost(120, TimeUnit.SECONDS)
        .untilAsserted(
            () -> {
              final CompletionSnapshot before = readCompletionSnapshot(senderEnv, pipeName);

              Assert.assertEquals(
                  "Pipe changed while awaiting completion", initial.staticMeta, before.staticMeta);
              AssertionError completionFailure = null;
              for (final DataNodeWrapper node : before.taskOwners.values()) {
                try {
                  assertCompletionMetric(node, pipeName, initial.staticMeta.getCreationTime());
                } catch (final AssertionError e) {
                  completionFailure = e;
                  break;
                }
              }

              final CompletionSnapshot after = readCompletionSnapshot(senderEnv, pipeName);
              Assert.assertEquals(
                  "Pipe changed while reading metrics", before.staticMeta, after.staticMeta);
              Assert.assertEquals(
                  "DataRegion leaders changed while reading metrics",
                  before.regionLeaders,
                  after.regionLeaders);
              if (completionFailure != null) {
                reflushIfNeeded(senderEnv, lastFlushTimeNanos);
                throw completionFailure;
              }
            });
  }

  private static void reflushIfNeeded(final BaseEnv senderEnv, final long[] lastFlushTimeNanos)
      throws Exception {
    final long now = System.nanoTime();
    if (now - lastFlushTimeNanos[0]
        >= TimeUnit.MILLISECONDS.toNanos(COMPLETION_REFLUSH_INTERVAL_MILLIS)) {
      // A new DataRegion can be assigned after the preceding flush. Re-publish the barrier only
      // after a short quiet period so asynchronous task startup can be covered without flooding
      // the sender with flush requests.
      TestUtils.executeNonQuery(senderEnv, "flush", null);
      lastFlushTimeNanos[0] = System.nanoTime();
    }
  }

  private static CompletionSnapshot readCompletionSnapshot(final BaseEnv env, final String pipeName)
      throws Exception {
    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) env.getLeaderConfigNodeConnection()) {
      final TGetAllPipeInfoResp response = client.getAllPipeInfo();
      Assert.assertEquals(
          response.toString(),
          TSStatusCode.SUCCESS_STATUS.getStatusCode(),
          response.getStatus().getCode());
      PipeMeta pipe = null;
      for (final ByteBuffer binary : response.getAllPipeInfo()) {
        final PipeMeta candidate = PipeMeta.deserialize4Coordinator(binary);
        if (pipeName.equals(candidate.getStaticMeta().getPipeName())) {
          pipe = candidate;
          break;
        }
      }
      Assert.assertNotNull("Missing pipe " + pipeName, pipe);
      Assert.assertTrue(
          "Pipe does not support completion barriers: " + pipeName,
          pipe.getStaticMeta().supportsCompletionMark());
      Assert.assertEquals(
          "Pipe is not running: " + pipeName,
          PipeStatus.RUNNING,
          pipe.getRuntimeMeta().getStatus().get());
      Assert.assertTrue(
          "Pipe node errors: " + pipe.getRuntimeMeta().getNodeId2PipeRuntimeExceptionMap(),
          pipe.getRuntimeMeta().getNodeId2PipeRuntimeExceptionMap().isEmpty());

      // Region -> pipe task leader, and leader node id -> endpoint. Derive the expected nodes from
      // metadata rather than present gauges, so a missing/failed source cannot count as completed.
      final Map<Integer, Integer> regionLeaders = new HashMap<>();
      final Map<Integer, DataNodeWrapper> taskOwners = new HashMap<>();
      for (final boolean tableModel : new boolean[] {false, true}) {
        final TShowRegionResp regions =
            client.showRegion(
                new TShowRegionReq()
                    .setConsensusGroupType(TConsensusGroupType.DataRegion)
                    .setIsTableModel(tableModel));
        Assert.assertEquals(
            regions.toString(),
            TSStatusCode.SUCCESS_STATUS.getStatusCode(),
            regions.getStatus().getCode());
        for (final TRegionInfo region : regions.getRegionInfoList()) {
          final int regionId = region.getConsensusGroupId().getId();
          final PipeTaskMeta task =
              pipe.getRuntimeMeta().getConsensusGroupId2TaskMetaMap().get(regionId);
          Assert.assertNotNull("Missing pipe task for DataRegion " + regionId, task);
          Assert.assertFalse(
              "Pipe task errors in DataRegion "
                  + regionId
                  + ": "
                  + task.getExceptionMessagesString(),
              task.hasExceptionMessages());
          regionLeaders.put(regionId, task.getLeaderNodeId());
          if (region.getDataNodeId() == task.getLeaderNodeId()) {
            final DataNodeWrapper node =
                env.getDataNodeWrapperList().stream()
                    .filter(wrapper -> wrapper.getPort() == region.getClientRpcPort())
                    .findFirst()
                    .orElseThrow(
                        () -> new AssertionError("Unknown DataRegion endpoint: " + region));
            taskOwners.put(region.getDataNodeId(), node);
          }
        }
      }
      Assert.assertFalse("No DataRegions to await for " + pipeName, regionLeaders.isEmpty());
      Assert.assertTrue(
          "Missing task owner endpoints: " + regionLeaders,
          taskOwners.keySet().containsAll(regionLeaders.values()));
      return new CompletionSnapshot(pipe.getStaticMeta(), regionLeaders, taskOwners);
    }
  }

  private static void assertCompletionMetric(
      final DataNodeWrapper node, final String pipeName, final long creationTime) throws Exception {
    final JMXServiceURL url =
        new JMXServiceURL(
            "service:jmx:rmi:///jndi/rmi://"
                + UrlUtils.formatTEndPointIpv4AndIpv6Url(node.getIp(), node.getJmxPort())
                + "/jmxrmi");
    // Prometheus serves a 15-second cached snapshot. Read the AutoGauge through JMX instead, so a
    // previous successful wait cannot mask newly published data on the same pipe incarnation.
    final ObjectName metric =
        IoTDBMetricObjNameFactory.getInstance()
            .createName(
                "IoTDBAutoGauge",
                "org.apache.iotdb.metrics",
                "pipe_datanode_completion_ready",
                Map.of("name", pipeName, "creation_time", String.valueOf(creationTime)));
    try (final JMXConnector connector = JMXConnectorFactory.connect(url)) {
      final Object value = connector.getMBeanServerConnection().getAttribute(metric, "Value");
      if (!(value instanceof Number completion)) {
        throw new AssertionError("Non-numeric completion metric at " + url + ": " + value);
      }
      Assert.assertEquals(
          "Uncommitted data for " + metric + " at " + url, 1.0, completion.doubleValue(), 0.0);
    } catch (final InstanceNotFoundException e) {
      throw new AssertionError("Missing completion metric " + metric + " at " + url, e);
    }
  }

  private static class CompletionSnapshot {
    private final PipeStaticMeta staticMeta;
    private final Map<Integer, Integer> regionLeaders;
    private final Map<Integer, DataNodeWrapper> taskOwners;

    private CompletionSnapshot(
        final PipeStaticMeta staticMeta,
        final Map<Integer, Integer> regionLeaders,
        final Map<Integer, DataNodeWrapper> taskOwners) {
      this.staticMeta = staticMeta;
      this.regionLeaders = regionLeaders;
      this.taskOwners = taskOwners;
    }
  }
}
