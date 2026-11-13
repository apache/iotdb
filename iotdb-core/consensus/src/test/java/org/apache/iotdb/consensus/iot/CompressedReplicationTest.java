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
import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.commons.request.IConsensusRequest;
import org.apache.iotdb.consensus.common.Peer;
import org.apache.iotdb.consensus.common.request.BatchIndexedConsensusRequest;
import org.apache.iotdb.consensus.common.request.IndexedConsensusRequest;
import org.apache.iotdb.consensus.config.ConsensusConfig;
import org.apache.iotdb.consensus.config.IoTConsensusConfig;
import org.apache.iotdb.consensus.iot.util.TestEntry;
import org.apache.iotdb.consensus.iot.util.TestStateMachine;

import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.net.ServerSocket;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/** Real async Thrift in both directions, using the existing state machine/reader fixture. */
@RunWith(Parameterized.class)
public class CompressedReplicationTest {
  @Rule public TemporaryFolder directory = new TemporaryFolder();

  @Parameterized.Parameters(name = "{0}")
  public static Object[] codecs() {
    return new Object[] {CompressionType.LZ4, CompressionType.SNAPPY, CompressionType.ZSTD};
  }

  @Parameterized.Parameter public CompressionType codec;
  private static final int COUNT = 32;
  private static final int BYTES = 65536;

  @Test
  public void testBidirectionalReplicationPreservesFullPayloadAndMetadata() throws Exception {
    DataRegionId group = new DataRegionId(1);
    int firstPort, secondPort;
    try (ServerSocket first = new ServerSocket(0);
        ServerSocket second = new ServerSocket(0)) {
      firstPort = first.getLocalPort();
      secondPort = second.getLocalPort();
    }
    List<Peer> peers =
        Arrays.asList(
            new Peer(group, 1, new TEndPoint("127.0.0.1", firstPort)),
            new Peer(group, 2, new TEndPoint("127.0.0.1", secondPort)));
    IoTConsensus[] servers = new IoTConsensus[2];
    ObservedStateMachine[] machines = {new ObservedStateMachine(), new ObservedStateMachine()};
    try {
      for (int i = 0; i < 2; i++) {
        final int node = i;
        ConsensusConfig config =
            ConsensusConfig.newBuilder()
                .setThisNodeId(i + 1)
                .setThisNode(peers.get(i).getEndpoint())
                .setConsensusGroupType(TConsensusGroupType.DataRegion)
                .setStorageDir(directory.newFolder().getAbsolutePath())
                .setRecvSnapshotDirs(
                    Collections.singletonList(directory.newFolder().getAbsolutePath()))
                .setIoTConsensusConfig(
                    IoTConsensusConfig.newBuilder()
                        .setReplication(
                            IoTConsensusConfig.Replication.newBuilder()
                                .setLogCompressionType(codec)
                                .setMaxWaitingTimeForWaitBatchInMs(100)
                                .build())
                        .build())
                .build();
        servers[i] = new IoTConsensus(config, ignored -> machines[node]);
        servers[i].recordCorrectPeerListBeforeStarting(Collections.singletonMap(group, peers));
        servers[i].start();
      }
      for (IoTConsensus server : servers) server.createLocalPeer(group, peers);
      for (int j = 0; j < COUNT; j++) {
        for (int i = 0; i < 2; i++) {
          final TestEntry entry =
              new TestEntry(j, peers.get(i)) {
                @Override
                public ByteBuffer serializeToByteBuffer() {
                  ByteBuffer result = ByteBuffer.allocate(BYTES);
                  result.put(super.serializeToByteBuffer());
                  // A repeated nonzero tail makes corruption visible beyond the parsed TestEntry.
                  while (result.hasRemaining()) result.put((byte) 42);
                  result.flip();
                  return result;
                }
              };
          assertEquals(200, servers[i].write(group, entry).getCode());
        }
      }
      for (int i = 0; i < 2; i++) {
        assertTrue(
            "remote requests must all be applied", machines[i].applied.await(30, TimeUnit.SECONDS));
        assertNull(machines[i].failure.get());
        assertEquals(COUNT * 2, machines[i].getData().size());
        for (Peer peer : peers)
          for (int j = 0; j < COUNT; j++)
            assertTrue(machines[i].getData().contains(new TestEntry(j, peer)));
      }
    } finally {
      for (IoTConsensus server : servers) if (server != null) server.stop();
    }
  }

  private static class ObservedStateMachine extends TestStateMachine {
    private final CountDownLatch applied = new CountDownLatch(COUNT);
    private final AtomicReference<AssertionError> failure = new AtomicReference<>();

    @Override
    public IConsensusRequest deserializeRequest(IConsensusRequest request) {
      if (request instanceof BatchIndexedConsensusRequest) {
        try {
          for (IndexedConsensusRequest indexed :
              ((BatchIndexedConsensusRequest) request).getRequests()) {
            assertTrue(indexed.getPhysicalTime() > 0);
            for (IConsensusRequest inner : indexed.getRequests()) {
              ByteBuffer buffer = inner.serializeToByteBuffer().duplicate();
              assertEquals(BYTES, buffer.remaining());
              buffer.getInt();
              Peer.deserialize(buffer);
              while (buffer.hasRemaining()) assertEquals(42, buffer.get());
            }
          }
        } catch (AssertionError e) {
          failure.compareAndSet(null, e);
        }
      }
      return super.deserializeRequest(request);
    }

    @Override
    public TSStatus write(IConsensusRequest request) {
      TSStatus result = super.write(request);
      if (request instanceof IndexedConsensusRequest
          && ((IndexedConsensusRequest) request).getSearchIndex() == -1) {
        applied.countDown();
      }
      return result;
    }
  }
}
