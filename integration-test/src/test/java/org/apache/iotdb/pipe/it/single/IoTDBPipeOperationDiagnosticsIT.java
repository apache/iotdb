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

package org.apache.iotdb.pipe.it.single;

import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.client.sync.SyncConfigNodeIServiceClient;
import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.confignode.rpc.thrift.TAlterPipeReq;
import org.apache.iotdb.confignode.rpc.thrift.TCreatePipeReq;
import org.apache.iotdb.confignode.rpc.thrift.TShowPipeReq;
import org.apache.iotdb.confignode.rpc.thrift.TShowPipeResp;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.env.cluster.node.ConfigNodeWrapper;
import org.apache.iotdb.it.env.cluster.node.DataNodeWrapper;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.rpc.TSStatusCode;

import org.awaitility.Awaitility;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Collections;
import java.util.HashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

@RunWith(IoTDBTestRunner.class)
@Category({ClusterIT.class})
public class IoTDBPipeOperationDiagnosticsIT {

  private static final String PIPE_NAME = "pipe_operation_diagnostics";
  private static final String RECOVER_PIPE_NAME = "pipe_drop_recover";

  @Before
  public void setUp() {
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setAutoCreateSchemaEnabled(true)
        .setDnConnectionTimeoutMs(2000)
        .setPipeMetaSyncerSyncIntervalMinutes(1)
        .setPipeMemoryManagementEnabled(false)
        .setIsPipeEnableMemoryCheck(false)
        .setPipeAutoSplitFullEnabled(false);
    EnvFactory.getEnv().initClusterEnvironment(1, 2);
  }

  @After
  public void tearDown() {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testDropPipeExposesPreDeleteAndRejectsConcurrentOperations() throws Exception {
    final DataNodeWrapper unavailableDataNode = EnvFactory.getEnv().getDataNodeWrapper(1);
    final ConfigNodeWrapper configNode = EnvFactory.getEnv().getConfigNodeWrapper(0);
    createPipe(PIPE_NAME);

    final int unavailableDataNodeId;
    try (final SyncConfigNodeIServiceClient client = getLeaderClient()) {
      unavailableDataNodeId = findDataNodeId(client, unavailableDataNode);
    }

    unavailableDataNode.stopForcibly();
    try (BlackholeServer ignored =
        new BlackholeServer(
            unavailableDataNode.getInternalAddress(), unavailableDataNode.getInternalPort())) {
      configNode.stopForcibly();
      configNode.start();
      awaitLeader();
      try (final SyncConfigNodeIServiceClient client = getLeaderClient()) {
        final TSStatus dropStatus = client.dropPipe(PIPE_NAME);
        assertDropTimeout(dropStatus, unavailableDataNodeId);
        Assert.assertTrue(ignored.getAcceptedConnectionCount() > 0);
        assertPipeState(client, PIPE_NAME, "PRE_DELETE");

        assertPipeBeingDropped(
            client.alterPipe(newAlterPipeRequest(PIPE_NAME)),
            String.format("Failed to alter pipe %s, the pipe is being dropped", PIPE_NAME));
        assertPipeBeingDropped(
            client.startPipe(PIPE_NAME),
            String.format("Failed to start pipe %s, the pipe is being dropped", PIPE_NAME));
        assertPipeBeingDropped(
            client.stopPipe(PIPE_NAME),
            String.format("Failed to stop pipe %s, the pipe is being dropped", PIPE_NAME));
      }
    } finally {
      if (!configNode.isAlive()) {
        configNode.start();
      }
      restartDataNode(unavailableDataNode);
    }

    awaitPipeAbsent(PIPE_NAME);
  }

  @Test
  public void testDropPipeProcedureRecoversWithPreDelete() throws Exception {
    final DataNodeWrapper unavailableDataNode = EnvFactory.getEnv().getDataNodeWrapper(1);
    final ConfigNodeWrapper configNode = EnvFactory.getEnv().getConfigNodeWrapper(0);
    createPipe(RECOVER_PIPE_NAME);

    final int unavailableDataNodeId;
    try (final SyncConfigNodeIServiceClient client = getLeaderClient()) {
      unavailableDataNodeId = findDataNodeId(client, unavailableDataNode);
    }

    unavailableDataNode.stopForcibly();
    try {
      try (BlackholeServer ignored =
          new BlackholeServer(
              unavailableDataNode.getInternalAddress(), unavailableDataNode.getInternalPort())) {
        configNode.stopForcibly();
        configNode.start();
        awaitLeader();
        try (final SyncConfigNodeIServiceClient client = getLeaderClient()) {
          final TSStatus dropStatus = client.dropPipe(RECOVER_PIPE_NAME);
          assertDropTimeout(dropStatus, unavailableDataNodeId);
          assertPipeState(client, RECOVER_PIPE_NAME, "PRE_DELETE");
        }

        configNode.stopForcibly();
        configNode.start();
        awaitLeader();
        try (final SyncConfigNodeIServiceClient recoveredClient = getLeaderClient()) {
          assertPipeState(recoveredClient, RECOVER_PIPE_NAME, "PRE_DELETE");
          assertPipeBeingDropped(
              recoveredClient.alterPipe(newAlterPipeRequest(RECOVER_PIPE_NAME)),
              String.format(
                  "Failed to alter pipe %s, the pipe is being dropped", RECOVER_PIPE_NAME));
          assertPipeBeingDropped(
              recoveredClient.startPipe(RECOVER_PIPE_NAME),
              String.format(
                  "Failed to start pipe %s, the pipe is being dropped", RECOVER_PIPE_NAME));
          assertPipeBeingDropped(
              recoveredClient.stopPipe(RECOVER_PIPE_NAME),
              String.format(
                  "Failed to stop pipe %s, the pipe is being dropped", RECOVER_PIPE_NAME));
        }
      }
    } finally {
      if (!configNode.isAlive()) {
        configNode.start();
      }
      restartDataNode(unavailableDataNode);
    }

    awaitPipeAbsent(RECOVER_PIPE_NAME);
  }

  private void createPipe(final String pipeName) throws Exception {
    final HashMap<String, String> sinkAttributes = new HashMap<>();
    sinkAttributes.put("sink", "write-back-sink");
    try (final SyncConfigNodeIServiceClient client = getLeaderClient()) {
      final TSStatus status =
          client.createPipe(
              new TCreatePipeReq(pipeName, sinkAttributes)
                  .setExtractorAttributes(new HashMap<>())
                  .setProcessorAttributes(new HashMap<>()));
      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
    }
  }

  private SyncConfigNodeIServiceClient getLeaderClient() throws Exception {
    return (SyncConfigNodeIServiceClient) EnvFactory.getEnv().getLeaderConfigNodeConnection();
  }

  private static TAlterPipeReq newAlterPipeRequest(final String pipeName) {
    return new TAlterPipeReq()
        .setPipeName(pipeName)
        .setExtractorAttributes(new HashMap<>())
        .setIsReplaceAllExtractorAttributes(false)
        .setProcessorAttributes(new HashMap<>())
        .setIsReplaceAllProcessorAttributes(false)
        .setConnectorAttributes(new HashMap<>())
        .setIsReplaceAllConnectorAttributes(false);
  }

  private static int findDataNodeId(
      final SyncConfigNodeIServiceClient client, final DataNodeWrapper dataNodeWrapper)
      throws Exception {
    for (final TDataNodeLocation dataNodeLocation : client.showCluster().getDataNodeList()) {
      if (dataNodeLocation.getInternalEndPoint().getPort() == dataNodeWrapper.getInternalPort()) {
        return dataNodeLocation.getDataNodeId();
      }
    }
    Assert.fail("The DataNode is not registered: " + dataNodeWrapper.getInternalPort());
    return -1;
  }

  private static void assertDropTimeout(final TSStatus status, final int unavailableDataNodeId) {
    Assert.assertEquals(TSStatusCode.PIPE_ERROR.getStatusCode(), status.getCode());
    Assert.assertNotNull(status.getMessage());
    Assert.assertTrue(status.getMessage(), status.getMessage().contains("DROP_PIPE"));
    Assert.assertTrue(status.getMessage(), status.getMessage().contains("procedureId="));
    Assert.assertTrue(status.getMessage(), status.getMessage().contains("OPERATE_ON_DATA_NODES"));
    Assert.assertTrue(
        status.getMessage(), status.getMessage().contains(String.valueOf(unavailableDataNodeId)));
  }

  private static void assertPipeBeingDropped(final TSStatus status, final String expectedMessage) {
    Assert.assertEquals(TSStatusCode.PIPE_ERROR.getStatusCode(), status.getCode());
    Assert.assertEquals(expectedMessage, status.getMessage());
  }

  private static void assertPipeState(
      final SyncConfigNodeIServiceClient client, final String pipeName, final String expectedState)
      throws Exception {
    final TShowPipeResp response = client.showPipe(new TShowPipeReq());
    Assert.assertEquals(
        TSStatusCode.SUCCESS_STATUS.getStatusCode(), response.getStatus().getCode());
    Assert.assertTrue(
        response.getPipeInfoList().stream()
            .anyMatch(
                pipeInfo ->
                    pipeName.equals(pipeInfo.getId())
                        && expectedState.equals(pipeInfo.getState())));
  }

  private static void awaitPipeAbsent(final String pipeName) {
    Awaitility.await()
        .pollInterval(1, TimeUnit.SECONDS)
        .atMost(1, TimeUnit.MINUTES)
        .until(
            () -> {
              try (final SyncConfigNodeIServiceClient client = getLeaderClientStatic()) {
                final TShowPipeResp response = client.showPipe(new TShowPipeReq());
                if (response.getStatus().getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
                  return false;
                }
                return response.getPipeInfoList().stream()
                    .noneMatch(pipeInfo -> pipeName.equals(pipeInfo.getId()));
              } catch (final Exception e) {
                return false;
              }
            });
  }

  private static SyncConfigNodeIServiceClient getLeaderClientStatic() throws Exception {
    return (SyncConfigNodeIServiceClient) EnvFactory.getEnv().getLeaderConfigNodeConnection();
  }

  private static void awaitLeader() {
    Awaitility.await()
        .pollInterval(1, TimeUnit.SECONDS)
        .atMost(45, TimeUnit.SECONDS)
        .until(
            () -> {
              try (final SyncConfigNodeIServiceClient ignored = getLeaderClientStatic()) {
                return true;
              } catch (final Exception e) {
                return false;
              }
            });
  }

  private static void restartDataNode(final DataNodeWrapper dataNodeWrapper) {
    if (!dataNodeWrapper.isAlive()) {
      dataNodeWrapper.start();
    }
    EnvFactory.getEnv()
        .ensureNodeStatus(
            Collections.singletonList(dataNodeWrapper),
            Collections.singletonList(NodeStatus.Running));
  }

  private static final class BlackholeServer implements AutoCloseable {
    private final ServerSocket serverSocket;
    private final CopyOnWriteArrayList<Socket> acceptedSockets = new CopyOnWriteArrayList<>();
    private final AtomicBoolean closed = new AtomicBoolean();
    private final AtomicInteger acceptedConnectionCount = new AtomicInteger();
    private final AtomicReference<IOException> acceptFailure = new AtomicReference<>();
    private final Thread acceptThread;

    private BlackholeServer(final String address, final int port) throws Exception {
      serverSocket = bind(address, port);
      acceptThread =
          new Thread(
              () -> {
                while (!closed.get()) {
                  try {
                    final Socket socket = serverSocket.accept();
                    acceptedSockets.add(socket);
                    acceptedConnectionCount.incrementAndGet();
                  } catch (final IOException e) {
                    if (!closed.get()) {
                      acceptFailure.set(e);
                    }
                    return;
                  }
                }
              },
              "iotdb-pipe-blackhole-" + port);
      acceptThread.setDaemon(true);
      acceptThread.start();
    }

    private static ServerSocket bind(final String address, final int port)
        throws IOException, InterruptedException {
      IOException lastException = null;
      final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
      while (System.nanoTime() < deadline) {
        final ServerSocket candidate = new ServerSocket();
        try {
          candidate.setReuseAddress(true);
          candidate.bind(new InetSocketAddress(address, port));
          return candidate;
        } catch (final IOException e) {
          lastException = e;
          candidate.close();
          TimeUnit.MILLISECONDS.sleep(100);
        }
      }
      throw lastException;
    }

    private int getAcceptedConnectionCount() {
      Assert.assertNull(acceptFailure.get());
      return acceptedConnectionCount.get();
    }

    @Override
    public void close() {
      if (!closed.compareAndSet(false, true)) {
        return;
      }
      try {
        serverSocket.close();
      } catch (final IOException ignored) {
        // The socket is already being closed.
      }
      for (final Socket socket : acceptedSockets) {
        try {
          socket.close();
        } catch (final IOException ignored) {
          // The socket is already being closed.
        }
      }
      try {
        acceptThread.join(TimeUnit.SECONDS.toMillis(1));
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
  }
}
