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

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.commons.disk.strategy.DirectoryStrategyType;
import org.apache.iotdb.consensus.common.Peer;
import org.apache.iotdb.consensus.config.IoTConsensusConfig;
import org.apache.iotdb.consensus.iot.util.TestStateMachine;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Spliterator;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class IoTConsensusServerImplTest {

  private static final int WRITER_COUNT = 4;
  private static final int READER_COUNT = 4;
  private static final int PEERS_PER_WRITER = 1_000;

  @Rule public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  /**
   * Verifies that configuration snapshots can be read while several writers concurrently add and
   * remove distinct peers. Every snapshot must remain duplicate-free and sorted, and the final
   * configuration must contain exactly the peers that were not removed.
   */
  @Test
  public void testConfigurationConcurrentReadAndWrite() throws Exception {
    final ScheduledExecutorService backgroundTaskService =
        Executors.newSingleThreadScheduledExecutor();
    final ExecutorService executorService =
        Executors.newFixedThreadPool(WRITER_COUNT + READER_COUNT);
    try {
      final Peer thisNode = new Peer(new DataRegionId(1), 0, new TEndPoint("127.0.0.1", 6667));
      final IoTConsensusServerImpl server =
          new IoTConsensusServerImpl(
              temporaryFolder.getRoot().getAbsolutePath(),
              null,
              DirectoryStrategyType.SEQUENCE_STRATEGY,
              thisNode,
              Collections.singletonList(thisNode),
              new TestStateMachine(),
              backgroundTaskService,
              null,
              null,
              IoTConsensusConfig.newBuilder().build());
      final Set<Peer> configuration = getConfiguration(server);
      assertTrue(configuration.spliterator().hasCharacteristics(Spliterator.CONCURRENT));

      final CountDownLatch startLatch = new CountDownLatch(1);
      final CountDownLatch writersDoneLatch = new CountDownLatch(WRITER_COUNT);
      final Set<Peer> expectedConfiguration = new HashSet<>();
      expectedConfiguration.add(thisNode);

      final Future<?>[] writerFutures = new Future<?>[WRITER_COUNT];
      for (int writerIndex = 0; writerIndex < WRITER_COUNT; writerIndex++) {
        final int currentWriterIndex = writerIndex;
        writerFutures[writerIndex] =
            executorService.submit(
                () -> {
                  startLatch.await();
                  try {
                    for (int peerIndex = 0; peerIndex < PEERS_PER_WRITER; peerIndex++) {
                      final Peer peer = createPeer(currentWriterIndex, peerIndex);
                      configuration.add(peer);
                      if ((peerIndex & 1) == 0) {
                        configuration.remove(peer);
                      }
                    }
                  } finally {
                    writersDoneLatch.countDown();
                  }
                  return null;
                });
        for (int peerIndex = 1; peerIndex < PEERS_PER_WRITER; peerIndex += 2) {
          expectedConfiguration.add(createPeer(writerIndex, peerIndex));
        }
      }

      final Future<?>[] readerFutures = new Future<?>[READER_COUNT];
      for (int readerIndex = 0; readerIndex < READER_COUNT; readerIndex++) {
        readerFutures[readerIndex] =
            executorService.submit(
                () -> {
                  startLatch.await();
                  do {
                    assertValidSnapshot(server.getConfiguration());
                  } while (writersDoneLatch.getCount() > 0);
                  assertValidSnapshot(server.getConfiguration());
                  return null;
                });
      }

      startLatch.countDown();
      for (Future<?> writerFuture : writerFutures) {
        writerFuture.get(30, TimeUnit.SECONDS);
      }
      for (Future<?> readerFuture : readerFutures) {
        readerFuture.get(30, TimeUnit.SECONDS);
      }

      assertEquals(expectedConfiguration, new HashSet<>(server.getConfiguration()));
    } finally {
      executorService.shutdownNow();
      backgroundTaskService.shutdownNow();
    }
  }

  @SuppressWarnings("unchecked")
  private static Set<Peer> getConfiguration(IoTConsensusServerImpl server) throws Exception {
    final Field configurationField = IoTConsensusServerImpl.class.getDeclaredField("configuration");
    configurationField.setAccessible(true);
    return (Set<Peer>) configurationField.get(server);
  }

  private static Peer createPeer(int writerIndex, int peerIndex) {
    final int nodeId = writerIndex * PEERS_PER_WRITER + peerIndex + 1;
    return new Peer(new DataRegionId(1), nodeId, new TEndPoint("127.0.0.1", 10_000 + nodeId));
  }

  private static void assertValidSnapshot(List<Peer> snapshot) {
    assertEquals(snapshot.size(), new HashSet<>(snapshot).size());
    for (int index = 1; index < snapshot.size(); index++) {
      assertTrue(snapshot.get(index - 1).compareTo(snapshot.get(index)) < 0);
    }
  }
}
