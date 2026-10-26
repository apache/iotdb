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

package org.apache.iotdb.consensus.iot;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.consensus.ConsensusGroupId;
import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.commons.exception.StartupException;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.consensus.common.ConsensusGroup;
import org.apache.iotdb.consensus.common.Peer;
import org.apache.iotdb.consensus.common.request.IConsensusRequest;
import org.apache.iotdb.consensus.config.ConsensusConfig;
import org.apache.iotdb.consensus.config.IoTConsensusConfig;
import org.apache.iotdb.consensus.exception.ConsensusException;
import org.apache.iotdb.consensus.iot.logdispatcher.IoTConsensusMemoryManager;
import org.apache.iotdb.consensus.iot.util.TestStateMachine;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.ratis.util.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class ConcurrentLargeWriteTest {

  private static final ConsensusGroupId GROUP_ID = new DataRegionId(1);
  private static final int FIRST_PORT = 10000;
  private static final int LAST_PORT = 20000;
  private static final int REQUEST_COUNT = 4;
  private static final int REQUEST_PAYLOAD_SIZE = 8 * 1024 * 1024;
  private static final long REPLICATION_TIMEOUT_SECONDS = 60;

  private List<Peer> peers;
  private final List<File> storageDirs =
      Arrays.asList(
          new File("target" + File.separator + "concurrent-large-write-1"),
          new File("target" + File.separator + "concurrent-large-write-2"));
  private final List<IoTConsensus> servers = new ArrayList<>();
  private final List<TestStateMachine> stateMachines = new ArrayList<>();
  private ConsensusGroup group;
  private final IoTConsensusMemoryManager memoryManager = IoTConsensusMemoryManager.getInstance();

  private long previousMaxMemory;
  private long previousMaxQueueMemory;

  @Before
  public void setUp() throws Exception {
    int basePort = findAvailablePortPair();
    Assume.assumeTrue(basePort > 0);
    peers =
        Arrays.asList(
            new Peer(GROUP_ID, 1, new TEndPoint("127.0.0.1", basePort)),
            new Peer(GROUP_ID, 2, new TEndPoint("127.0.0.1", basePort + 1)));
    group = new ConsensusGroup(GROUP_ID, peers);
    previousMaxMemory = memoryManager.getMaxMemorySizeInByte();
    previousMaxQueueMemory = memoryManager.getMaxMemorySizeForQueueInByte();
    for (File storageDir : storageDirs) {
      FileUtils.deleteFully(storageDir);
      FileUtils.createDirectories(storageDir);
    }

    IoTConsensusConfig consensusConfig =
        IoTConsensusConfig.newBuilder()
            .setReplication(
                IoTConsensusConfig.Replication.newBuilder()
                    .setMaxLogEntriesNumPerBatch(2)
                    .setMaxSizePerBatch(REQUEST_PAYLOAD_SIZE * 2)
                    .setMaxPendingBatchesNum(5)
                    .setBasicRetryWaitTimeMs(10)
                    // Only one large batch fits, forcing competing dispatchers to retry
                    // reservation.
                    .setAllocateMemoryForConsensus(REQUEST_PAYLOAD_SIZE * 2L)
                    .setMaxMemoryRatioForQueue(1.0)
                    .build())
            .build();
    for (int i = 0; i < peers.size(); i++) {
      int nodeIndex = i;
      stateMachines.add(new TestStateMachine());
      servers.add(
          (IoTConsensus)
              ConsensusFactory.getConsensusImpl(
                      ConsensusFactory.IOT_CONSENSUS,
                      ConsensusConfig.newBuilder()
                          .setThisNodeId(peers.get(i).getNodeId())
                          .setThisNode(peers.get(i).getEndpoint())
                          .setStorageDir(storageDirs.get(i).getAbsolutePath())
                          .setConsensusGroupType(TConsensusGroupType.DataRegion)
                          .setIoTConsensusConfig(consensusConfig)
                          .build(),
                      groupId -> stateMachines.get(nodeIndex))
                  .orElseThrow(
                      () ->
                          new IllegalArgumentException(
                              String.format(
                                  ConsensusFactory.CONSTRUCT_FAILED_MSG,
                                  ConsensusFactory.IOT_CONSENSUS))));
      servers.get(i).recordCorrectPeerListBeforeStarting(Collections.singletonMap(GROUP_ID, peers));
    }
    try {
      for (IoTConsensus server : servers) {
        server.start();
      }
    } catch (IOException e) {
      if (e.getCause() instanceof StartupException) {
        Assume.assumeTrue(false);
      }
      throw e;
    }
  }

  @After
  public void tearDown() throws Exception {
    servers.forEach(IoTConsensus::stop);
    servers.clear();
    memoryManager.init(previousMaxMemory, previousMaxQueueMemory);
    for (File storageDir : storageDirs) {
      FileUtils.deleteFully(storageDir);
    }
  }

  @Test
  public void testConcurrentLargeWritesWithTwoReplicas() throws Exception {
    for (IoTConsensus server : servers) {
      createLocalPeer(server);
    }

    CountDownLatch start = new CountDownLatch(1);
    ExecutorService executor = Executors.newFixedThreadPool(peers.size() * REQUEST_COUNT);
    List<Future<TSStatus>> futures = new ArrayList<>();
    try {
      for (int nodeIndex = 0; nodeIndex < peers.size(); nodeIndex++) {
        int finalNodeIndex = nodeIndex;
        for (int requestIndex = 0; requestIndex < REQUEST_COUNT; requestIndex++) {
          int finalRequestIndex = requestIndex;
          futures.add(
              executor.submit(
                  () -> {
                    start.await();
                    return servers
                        .get(finalNodeIndex)
                        .write(
                            GROUP_ID,
                            new LargeTestEntry(
                                finalRequestIndex,
                                peers.get(finalNodeIndex),
                                REQUEST_PAYLOAD_SIZE));
                  }));
        }
      }
      start.countDown();
      for (Future<TSStatus> future : futures) {
        Assert.assertEquals(
            TSStatusCode.SUCCESS_STATUS.getStatusCode(),
            future.get(30, TimeUnit.SECONDS).getCode());
      }
    } finally {
      executor.shutdownNow();
      executor.awaitTermination(30, TimeUnit.SECONDS);
    }

    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(REPLICATION_TIMEOUT_SECONDS);
    while (System.nanoTime() < deadline
        && (servers.get(0).getImpl(GROUP_ID).getMinSyncIndex() < REQUEST_COUNT
            || servers.get(1).getImpl(GROUP_ID).getMinSyncIndex() < REQUEST_COUNT)) {
      Thread.sleep(100);
    }

    Assert.assertEquals(REQUEST_COUNT, servers.get(0).getImpl(GROUP_ID).getMinSyncIndex());
    Assert.assertEquals(REQUEST_COUNT, servers.get(1).getImpl(GROUP_ID).getMinSyncIndex());
    Assert.assertEquals(REQUEST_COUNT * peers.size(), stateMachines.get(0).getRequestSet().size());
    Assert.assertEquals(REQUEST_COUNT * peers.size(), stateMachines.get(1).getRequestSet().size());
  }

  private void createLocalPeer(IoTConsensus server) throws ConsensusException {
    server.createLocalPeer(GROUP_ID, group.getPeers());
  }

  private int findAvailablePortPair() {
    for (int basePort = FIRST_PORT; basePort < LAST_PORT; basePort++) {
      try (ServerSocket first = new ServerSocket(basePort);
          ServerSocket second = new ServerSocket(basePort + 1)) {
        // Keep the test independent from services using a fixed development port.
        return basePort;
      } catch (IOException e) {
        // Try the next pair when either port is already in use.
      }
    }
    return -1;
  }

  private static class LargeTestEntry implements IConsensusRequest {
    private final ByteBuffer serialized;

    private LargeTestEntry(int num, Peer peer, int payloadSize) throws IOException {
      try (ByteArrayOutputStream output = new ByteArrayOutputStream(payloadSize + 64);
          DataOutputStream dataOutput = new DataOutputStream(output)) {
        dataOutput.writeInt(num);
        peer.serialize(dataOutput);
        dataOutput.write(new byte[payloadSize]);
        dataOutput.flush();
        serialized = ByteBuffer.wrap(output.toByteArray());
      }
    }

    @Override
    public ByteBuffer serializeToByteBuffer() {
      return serialized.duplicate();
    }
  }
}
