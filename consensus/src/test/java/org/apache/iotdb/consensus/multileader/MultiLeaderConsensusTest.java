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

package org.apache.iotdb.consensus.multileader;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.consensus.ConsensusGroupId;
import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.consensus.IStateMachine;
import org.apache.iotdb.consensus.common.ConsensusGroup;
import org.apache.iotdb.consensus.common.DataSet;
import org.apache.iotdb.consensus.common.Peer;
import org.apache.iotdb.consensus.common.request.ByteBufferConsensusRequest;
import org.apache.iotdb.consensus.common.request.IConsensusRequest;
import org.apache.iotdb.consensus.common.request.IndexedConsensusRequest;
import org.apache.iotdb.consensus.config.ConsensusConfig;
import org.apache.iotdb.consensus.multileader.logdispatcher.IndexController;
import org.apache.iotdb.consensus.multileader.wal.ConsensusReqReader;
import org.apache.iotdb.consensus.multileader.wal.GetConsensusReqReaderPlan;
import org.apache.iotdb.tsfile.utils.PublicBAOS;

import org.apache.ratis.util.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class MultiLeaderConsensusTest {

  private final Logger logger = LoggerFactory.getLogger(MultiLeaderConsensusTest.class);

  private final ConsensusGroupId gid = new DataRegionId(1);

  private final List<Peer> peers =
      Arrays.asList(
          new Peer(gid, new TEndPoint("127.0.0.1", 6000)),
          new Peer(gid, new TEndPoint("127.0.0.1", 6001)),
          new Peer(gid, new TEndPoint("127.0.0.1", 6002)));

  private final List<File> peersStorage =
      Arrays.asList(
          new File("target" + java.io.File.separator + "1"),
          new File("target" + java.io.File.separator + "2"),
          new File("target" + java.io.File.separator + "3"));

  private final ConsensusGroup group = new ConsensusGroup(gid, peers);
  private final List<MultiLeaderConsensus> servers = new ArrayList<>();
  private final List<TestStateMachine> stateMachines = new ArrayList<>();

  @Before
  public void setUp() throws Exception {
    for (int i = 0; i < 3; i++) {
      peersStorage.get(i).mkdirs();
      stateMachines.add(new TestStateMachine());
    }
    initServer();
  }

  @After
  public void tearDown() throws Exception {
    stopServer();
    for (File file : peersStorage) {
      FileUtils.deleteFully(file);
    }
  }

  private void initServer() throws IOException {
    for (int i = 0; i < 3; i++) {
      int finalI = i;
      servers.add(
          (MultiLeaderConsensus)
              ConsensusFactory.getConsensusImpl(
                      ConsensusFactory.MultiLeaderConsensus,
                      ConsensusConfig.newBuilder()
                          .setThisNode(peers.get(i).getEndpoint())
                          .setStorageDir(peersStorage.get(i).getAbsolutePath())
                          .build(),
                      groupId -> stateMachines.get(finalI))
                  .orElseThrow(
                      () ->
                          new IllegalArgumentException(
                              String.format(
                                  ConsensusFactory.CONSTRUCT_FAILED_MSG,
                                  ConsensusFactory.MultiLeaderConsensus))));
      servers.get(i).start();
    }
  }

  private void stopServer() {
    servers.parallelStream().forEach(MultiLeaderConsensus::stop);
    servers.clear();
  }

  /**
   * The three nodes use the requests in the queue to replicate the requests to the other two nodes
   */
  @Test
  public void ReplicateUsingQueueTest() throws IOException, InterruptedException {
    logger.info("Start ReplicateUsingQueueTest");
    servers.get(0).addConsensusGroup(group.getGroupId(), group.getPeers());
    servers.get(1).addConsensusGroup(group.getGroupId(), group.getPeers());
    servers.get(2).addConsensusGroup(group.getGroupId(), group.getPeers());

    Assert.assertEquals(0, servers.get(0).getImpl(gid).getIndex());
    Assert.assertEquals(0, servers.get(1).getImpl(gid).getIndex());
    Assert.assertEquals(0, servers.get(2).getImpl(gid).getIndex());

    for (int i = 0; i < IndexController.FLUSH_INTERVAL; i++) {
      servers.get(0).write(gid, new TestEntry(i, peers.get(0)));
      servers.get(1).write(gid, new TestEntry(i, peers.get(1)));
      servers.get(2).write(gid, new TestEntry(i, peers.get(2)));
      Assert.assertEquals(i + 1, servers.get(0).getImpl(gid).getIndex());
      Assert.assertEquals(i + 1, servers.get(1).getImpl(gid).getIndex());
      Assert.assertEquals(i + 1, servers.get(2).getImpl(gid).getIndex());
    }

    //    Assert.assertEquals(
    //        IndexController.FLUSH_INTERVAL,
    //        servers.get(0).getImpl(gid).getController().getLastFlushedIndex());
    //    Assert.assertEquals(
    //        IndexController.FLUSH_INTERVAL,
    //        servers.get(1).getImpl(gid).getController().getLastFlushedIndex());
    //    Assert.assertEquals(
    //        IndexController.FLUSH_INTERVAL,
    //        servers.get(2).getImpl(gid).getController().getLastFlushedIndex());

    for (int i = 0; i < 3; i++) {
      long start = System.currentTimeMillis();
      while (servers.get(i).getImpl(gid).getCurrentSafelyDeletedSearchIndex()
          < IndexController.FLUSH_INTERVAL) {
        long current = System.currentTimeMillis();
        if ((current - start) > 20 * 1000) {
          Assert.fail("Unable to replicate entries");
        }
        Thread.sleep(100);
      }
    }

    Assert.assertEquals(
        IndexController.FLUSH_INTERVAL,
        servers.get(0).getImpl(gid).getCurrentSafelyDeletedSearchIndex());
    Assert.assertEquals(
        IndexController.FLUSH_INTERVAL,
        servers.get(1).getImpl(gid).getCurrentSafelyDeletedSearchIndex());
    Assert.assertEquals(
        IndexController.FLUSH_INTERVAL,
        servers.get(2).getImpl(gid).getCurrentSafelyDeletedSearchIndex());
    Assert.assertEquals(
        IndexController.FLUSH_INTERVAL * 3, stateMachines.get(0).getRequestSet().size());
    Assert.assertEquals(
        IndexController.FLUSH_INTERVAL * 3, stateMachines.get(1).getRequestSet().size());
    Assert.assertEquals(
        IndexController.FLUSH_INTERVAL * 3, stateMachines.get(2).getRequestSet().size());
    Assert.assertEquals(stateMachines.get(0).getData(), stateMachines.get(1).getData());
    Assert.assertEquals(stateMachines.get(2).getData(), stateMachines.get(1).getData());

    stopServer();
    initServer();

    Assert.assertEquals(peers, servers.get(0).getImpl(gid).getConfiguration());
    Assert.assertEquals(peers, servers.get(1).getImpl(gid).getConfiguration());
    Assert.assertEquals(peers, servers.get(2).getImpl(gid).getConfiguration());

    Assert.assertEquals(IndexController.FLUSH_INTERVAL * 2, servers.get(0).getImpl(gid).getIndex());
    Assert.assertEquals(IndexController.FLUSH_INTERVAL * 2, servers.get(1).getImpl(gid).getIndex());
    Assert.assertEquals(IndexController.FLUSH_INTERVAL * 2, servers.get(2).getImpl(gid).getIndex());

    for (int i = 0; i < 3; i++) {
      long start = System.currentTimeMillis();
      while (servers.get(i).getImpl(gid).getCurrentSafelyDeletedSearchIndex()
          < IndexController.FLUSH_INTERVAL) {
        long current = System.currentTimeMillis();
        if ((current - start) > 20 * 1000) {
          Assert.fail("Unable to recover entries");
        }
        Thread.sleep(100);
      }
    }

    Assert.assertEquals(
        IndexController.FLUSH_INTERVAL,
        servers.get(1).getImpl(gid).getCurrentSafelyDeletedSearchIndex());
    Assert.assertEquals(
        IndexController.FLUSH_INTERVAL,
        servers.get(1).getImpl(gid).getCurrentSafelyDeletedSearchIndex());
    Assert.assertEquals(
        IndexController.FLUSH_INTERVAL,
        servers.get(2).getImpl(gid).getCurrentSafelyDeletedSearchIndex());
  }

  /**
   * First, suspend one node to test that the request replication between the two alive nodes is ok,
   * then restart all nodes to lose state in the queue, and test using WAL replication to make all
   * nodes finally consistent
   */
  @Test
  public void ReplicateUsingWALTest() throws IOException, InterruptedException {
    logger.info("Start ReplicateUsingWALTest");
    servers.get(0).addConsensusGroup(group.getGroupId(), group.getPeers());
    servers.get(1).addConsensusGroup(group.getGroupId(), group.getPeers());

    Assert.assertEquals(0, servers.get(0).getImpl(gid).getIndex());
    Assert.assertEquals(0, servers.get(1).getImpl(gid).getIndex());

    for (int i = 0; i < IndexController.FLUSH_INTERVAL; i++) {
      servers.get(0).write(gid, new TestEntry(i, peers.get(0)));
      servers.get(1).write(gid, new TestEntry(i, peers.get(1)));
      Assert.assertEquals(i + 1, servers.get(0).getImpl(gid).getIndex());
      Assert.assertEquals(i + 1, servers.get(1).getImpl(gid).getIndex());
    }

    //    Assert.assertEquals(
    //        IndexController.FLUSH_INTERVAL,
    //        servers.get(0).getImpl(gid).getController().getLastFlushedIndex());
    //    Assert.assertEquals(
    //        IndexController.FLUSH_INTERVAL,
    //        servers.get(1).getImpl(gid).getController().getLastFlushedIndex());

    Assert.assertEquals(0, servers.get(0).getImpl(gid).getCurrentSafelyDeletedSearchIndex());
    Assert.assertEquals(0, servers.get(1).getImpl(gid).getCurrentSafelyDeletedSearchIndex());

    stopServer();
    initServer();

    servers.get(2).addConsensusGroup(group.getGroupId(), group.getPeers());

    Assert.assertEquals(peers, servers.get(0).getImpl(gid).getConfiguration());
    Assert.assertEquals(peers, servers.get(1).getImpl(gid).getConfiguration());
    Assert.assertEquals(peers, servers.get(2).getImpl(gid).getConfiguration());

    Assert.assertEquals(IndexController.FLUSH_INTERVAL * 2, servers.get(0).getImpl(gid).getIndex());
    Assert.assertEquals(IndexController.FLUSH_INTERVAL * 2, servers.get(1).getImpl(gid).getIndex());
    Assert.assertEquals(0, servers.get(2).getImpl(gid).getIndex());

    for (int i = 0; i < 2; i++) {
      long start = System.currentTimeMillis();
      // should be [IndexController.FLUSH_INTERVAL, IndexController.FLUSH_INTERVAL * 2 - 1] after
      // replicating all entries
      while (servers.get(i).getImpl(gid).getCurrentSafelyDeletedSearchIndex()
          < IndexController.FLUSH_INTERVAL) {
        long current = System.currentTimeMillis();
        if ((current - start) > 20 * 1000) {
          logger.error("{}", servers.get(i).getImpl(gid).getCurrentSafelyDeletedSearchIndex());
          Assert.fail("Unable to replicate entries");
        }
        Thread.sleep(100);
      }
    }

    Assert.assertEquals(
        IndexController.FLUSH_INTERVAL * 2, stateMachines.get(0).getRequestSet().size());
    Assert.assertEquals(
        IndexController.FLUSH_INTERVAL * 2, stateMachines.get(1).getRequestSet().size());
    Assert.assertEquals(
        IndexController.FLUSH_INTERVAL * 2, stateMachines.get(2).getRequestSet().size());

    Assert.assertEquals(stateMachines.get(0).getData(), stateMachines.get(1).getData());
    Assert.assertEquals(stateMachines.get(2).getData(), stateMachines.get(1).getData());
  }

  private static class TestEntry implements IConsensusRequest {

    private final int num;
    private final Peer peer;

    public TestEntry(int num, Peer peer) {
      this.num = num;
      this.peer = peer;
    }

    @Override
    public ByteBuffer serializeToByteBuffer() {
      try (PublicBAOS publicBAOS = new PublicBAOS();
          DataOutputStream outputStream = new DataOutputStream(publicBAOS)) {
        outputStream.writeInt(num);
        peer.serialize(outputStream);
        return ByteBuffer.wrap(publicBAOS.getBuf(), 0, publicBAOS.size());
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      TestEntry testEntry = (TestEntry) o;
      return num == testEntry.num && Objects.equals(peer, testEntry.peer);
    }

    @Override
    public int hashCode() {
      return Objects.hash(num, peer);
    }

    @Override
    public String toString() {
      return "TestEntry{" + "num=" + num + ", peer=" + peer + '}';
    }
  }

  private static class TestStateMachine implements IStateMachine, IStateMachine.EventApi {

    private final RequestSets requestSets = new RequestSets(ConcurrentHashMap.newKeySet());

    public Set<IndexedConsensusRequest> getRequestSet() {
      return requestSets.getRequestSet();
    }

    public Set<TestEntry> getData() {
      Set<TestEntry> data = new HashSet<>();
      requestSets.getRequestSet().forEach(x -> data.add((TestEntry) x.getRequest()));
      return data;
    }

    @Override
    public void start() {}

    @Override
    public void stop() {}

    @Override
    public TSStatus write(IConsensusRequest request) {
      synchronized (requestSets) {
        IConsensusRequest innerRequest = ((IndexedConsensusRequest) request).getRequest();
        if (innerRequest instanceof ByteBufferConsensusRequest) {
          ByteBuffer buffer = innerRequest.serializeToByteBuffer();
          requestSets.add(
              new IndexedConsensusRequest(
                  ((IndexedConsensusRequest) request).getSearchIndex(),
                  -1,
                  new TestEntry(buffer.getInt(), Peer.deserialize(buffer))));
        } else {
          requestSets.add(((IndexedConsensusRequest) request));
        }
        return new TSStatus();
      }
    }

    @Override
    public synchronized DataSet read(IConsensusRequest request) {
      if (request instanceof GetConsensusReqReaderPlan) {
        return new FakeConsensusReqReader(requestSets);
      }
      return null;
    }

    @Override
    public boolean takeSnapshot(File snapshotDir) {
      return false;
    }

    @Override
    public void loadSnapshot(File latestSnapshotRootDir) {}
  }

  public static class FakeConsensusReqReader implements ConsensusReqReader, DataSet {

    private final RequestSets requestSets;

    public FakeConsensusReqReader(RequestSets requestSets) {
      this.requestSets = requestSets;
    }

    @Override
    public IConsensusRequest getReq(long index) {
      synchronized (requestSets) {
        for (IndexedConsensusRequest indexedConsensusRequest : requestSets.getRequestSet()) {
          if (indexedConsensusRequest.getSearchIndex() == index) {
            return indexedConsensusRequest;
          }
        }
        return null;
      }
    }

    @Override
    public List<IConsensusRequest> getReqs(long startIndex, int num) {
      return null;
    }

    @Override
    public ReqIterator getReqIterator(long startIndex) {
      return new FakeConsensusReqIterator(startIndex);
    }

    @Override
    public long getCurrentSearchIndex() {
      return 0;
    }

    private class FakeConsensusReqIterator implements ConsensusReqReader.ReqIterator {
      private long nextSearchIndex;

      public FakeConsensusReqIterator(long startIndex) {
        this.nextSearchIndex = startIndex;
      }

      @Override
      public boolean hasNext() {
        return true;
      }

      @Override
      public IndexedConsensusRequest next() {
        synchronized (requestSets) {
          for (IndexedConsensusRequest indexedConsensusRequest : requestSets.getRequestSet()) {
            if (indexedConsensusRequest.getSearchIndex() == nextSearchIndex) {
              nextSearchIndex++;
              return indexedConsensusRequest;
            }
          }
          return null;
        }
      }

      @Override
      public void waitForNextReady() throws InterruptedException {
        while (!hasNext()) {
          requestSets.waitForNextReady();
        }
      }

      @Override
      public void waitForNextReady(long time, TimeUnit unit)
          throws InterruptedException, TimeoutException {
        while (!hasNext()) {
          requestSets.waitForNextReady(time, unit);
        }
      }

      @Override
      public void skipTo(long targetIndex) {
        nextSearchIndex = targetIndex;
      }
    }
  }

  public static class RequestSets {
    private final Set<IndexedConsensusRequest> requestSet;

    public RequestSets(Set<IndexedConsensusRequest> requests) {
      this.requestSet = requests;
    }

    public void add(IndexedConsensusRequest request) {
      requestSet.add(request);
    }

    public Set<IndexedConsensusRequest> getRequestSet() {
      return requestSet;
    }

    public void waitForNextReady() throws InterruptedException {}

    public boolean waitForNextReady(long time, TimeUnit unit) throws InterruptedException {
      return true;
    }
  }
}
