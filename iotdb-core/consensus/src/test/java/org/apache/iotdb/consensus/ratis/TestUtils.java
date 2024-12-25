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

package org.apache.iotdb.consensus.ratis;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.consensus.ConsensusGroupId;
import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.consensus.IStateMachine;
import org.apache.iotdb.consensus.common.ConsensusGroup;
import org.apache.iotdb.consensus.common.DataSet;
import org.apache.iotdb.consensus.common.Peer;
import org.apache.iotdb.consensus.common.request.ByteBufferConsensusRequest;
import org.apache.iotdb.consensus.common.request.IConsensusRequest;
import org.apache.iotdb.consensus.config.ConsensusConfig;
import org.apache.iotdb.consensus.config.RatisConfig;
import org.apache.iotdb.consensus.exception.ConsensusException;
import org.apache.iotdb.consensus.exception.RatisReadUnavailableException;
import org.apache.iotdb.consensus.ratis.utils.Retriable;
import org.apache.iotdb.consensus.ratis.utils.Utils;

import org.apache.ratis.thirdparty.com.google.common.base.Preconditions;
import org.apache.ratis.util.FileUtils;
import org.apache.ratis.util.TimeDuration;
import org.apache.ratis.util.Timestamp;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.net.ServerSocket;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Scanner;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Supplier;

public class TestUtils {

  private static final Logger logger = LoggerFactory.getLogger(TestUtils.class);

  public static class TestDataSet implements DataSet {

    private int number;

    public void setNumber(int number) {
      this.number = number;
    }

    public int getNumber() {
      return number;
    }
  }

  public static class TestRequest implements IConsensusRequest {

    private final int cmd;

    public TestRequest(ByteBuffer buffer) {
      cmd = buffer.getInt();
    }

    public boolean isIncr() {
      return cmd == 1;
    }

    @Override
    public ByteBuffer serializeToByteBuffer() {
      ByteBuffer buffer = ByteBuffer.allocate(4).putInt(cmd);
      buffer.flip();
      return buffer;
    }

    static ByteBufferConsensusRequest incrRequest() {
      ByteBuffer incr = ByteBuffer.allocate(4);
      incr.putInt(1);
      incr.flip();
      return new ByteBufferConsensusRequest(incr);
    }

    static ByteBufferConsensusRequest getRequest() {
      ByteBuffer get = ByteBuffer.allocate(4);
      get.putInt(2);
      get.flip();
      return new ByteBufferConsensusRequest(get);
    }
  }

  public static class IntegerCounter implements IStateMachine, IStateMachine.EventApi {

    protected AtomicInteger integer;
    private final Logger logger = LoggerFactory.getLogger(IntegerCounter.class);
    private List<Peer> configuration;

    @Override
    public void start() {
      integer = new AtomicInteger(0);
    }

    @Override
    public void stop() {}

    @Override
    public TSStatus write(IConsensusRequest request) {
      if (((TestRequest) request).isIncr()) {
        integer.incrementAndGet();
      }
      return new TSStatus(200);
    }

    @Override
    public IConsensusRequest deserializeRequest(IConsensusRequest request) {
      TestRequest testRequest;
      if (request instanceof ByteBufferConsensusRequest) {
        testRequest = new TestRequest(request.serializeToByteBuffer());
      } else {
        testRequest = (TestRequest) request;
      }
      return testRequest;
    }

    @Override
    public DataSet read(IConsensusRequest request) {
      TestDataSet dataSet = new TestDataSet();
      dataSet.setNumber(integer.get());
      return dataSet;
    }

    @Override
    public boolean takeSnapshot(File snapshotDir) {
      File snapshot = new File(snapshotDir.getAbsolutePath() + File.separator + "snapshot");
      try (FileWriter writer = new FileWriter(snapshot)) {
        writer.write(String.valueOf(integer.get()));
      } catch (IOException e) {
        logger.error("cannot open file writer of {}", snapshot);
        return false;
      }
      return true;
    }

    @Override
    public void loadSnapshot(File latestSnapshotRootDir) {
      File snapshot =
          new File(latestSnapshotRootDir.getAbsolutePath() + File.separator + "snapshot");
      try (Scanner scanner = new Scanner(snapshot)) {
        integer.set(Integer.parseInt(scanner.next()));
      } catch (FileNotFoundException e) {
        logger.error("cannot find snapshot file {}", snapshot);
      }
    }

    @Override
    public void notifyLeaderChanged(ConsensusGroupId groupId, int newLeaderId) {
      System.out.println("---------newLeader-----------");
      System.out.println(groupId);
      System.out.println(newLeaderId);
      System.out.println("----------------------");
    }

    @Override
    public void notifyConfigurationChanged(long term, long index, List<Peer> newConfiguration) {
      this.configuration = newConfiguration;
      System.out.println("----------newConfiguration------------");
      System.out.println("term : " + term);
      System.out.println("index : " + index);
      for (Peer peer : newConfiguration) {
        System.out.println(peer);
      }
      System.out.println("----------------------");
    }

    public void reset() {
      this.integer.set(0);
    }

    @TestOnly
    public static synchronized String ensureSnapshotFileName(File snapshotDir, String metadata) {
      File dir = new File(snapshotDir + File.separator + metadata);
      if (!(dir.exists() && dir.isDirectory())) {
        dir.mkdirs();
      }
      return dir.getPath() + File.separator + "snapshot";
    }

    public List<Peer> getConfiguration() {
      return configuration;
    }
  }

  /** A Mini Raft CLuster Wrapper for Test Env. */
  static class MiniCluster {

    private final ConsensusGroupId gid;
    private final int replicas;
    private final List<Peer> peers;
    private final List<File> peerStorage;
    private final List<IStateMachine> stateMachines;
    private final RatisConfig config;
    private final List<RatisConsensus> servers;
    private final ConsensusGroup group;
    private Supplier<IStateMachine> smProvider;
    private final AtomicBoolean isStopped = new AtomicBoolean(false);

    private MiniCluster(
        ConsensusGroupId gid,
        int replicas,
        Function<Integer, File> storageProvider,
        Supplier<IStateMachine> smProvider,
        RatisConfig config) {
      this.gid = gid;
      this.replicas = replicas;
      this.config = config;
      this.smProvider = smProvider;
      Preconditions.checkArgument(
          replicas % 2 == 1, "Test Env Raft Group should consists singular peers");

      this.peers = new ArrayList<>();
      this.peerStorage = new ArrayList<>();
      this.stateMachines = new ArrayList<>();
      this.servers = new ArrayList<>();

      for (int i = 0; i < replicas; i++) {
        peers.add(new Peer(gid, i, new TEndPoint("127.0.0.1", randomFreePort())));

        final File storage = storageProvider.apply(i);
        FileUtils.deleteFileQuietly(storage);
        storage.mkdirs();
        peerStorage.add(storage);

        stateMachines.add(smProvider.get());
      }
      group = new ConsensusGroup(gid, peers);
      makeServers();
    }

    private void makeServers() {
      for (int i = 0; i < replicas; i++) {
        final int fi = i;
        servers.add(
            (RatisConsensus)
                ConsensusFactory.getConsensusImpl(
                        ConsensusFactory.RATIS_CONSENSUS,
                        ConsensusConfig.newBuilder()
                            .setThisNodeId(peers.get(i).getNodeId())
                            .setThisNode(peers.get(i).getEndpoint())
                            .setRatisConfig(config)
                            .setStorageDir(this.peerStorage.get(i).getAbsolutePath())
                            .setConsensusGroupType(TConsensusGroupType.DataRegion)
                            .build(),
                        groupId -> stateMachines.get(fi))
                    .orElseThrow(
                        () ->
                            new IllegalArgumentException(
                                String.format(
                                    ConsensusFactory.CONSTRUCT_FAILED_MSG,
                                    ConsensusFactory.RATIS_CONSENSUS))));
      }
    }

    void start() throws IOException {
      for (RatisConsensus server : servers) {
        server.start();
      }
      isStopped.set(false);
    }

    void stop() throws IOException {
      for (RatisConsensus server : servers) {
        server.stop();
      }
      isStopped.set(true);
    }

    void cleanUp() throws IOException {
      stop();
      for (File storage : peerStorage) {
        FileUtils.deleteFully(storage);
      }
      stateMachines.clear();
      servers.clear();
    }

    void restart() throws IOException {
      logger.info("start restarting the mini cluster");
      // clear the servers and rebuild them
      servers.clear();
      stateMachines.clear();
      for (int i = 0; i < replicas; i++) {
        stateMachines.add(smProvider.get());
      }
      makeServers();
      start();
      logger.info("end restarting the mini cluster");
    }

    List<RatisConsensus> getServers() {
      return Collections.unmodifiableList(servers);
    }

    RatisConsensus getServer(int index) {
      return servers.get(index);
    }

    List<IStateMachine> getStateMachines() {
      return Collections.unmodifiableList(stateMachines);
    }

    ConsensusGroupId getGid() {
      return gid;
    }

    List<Peer> getPeers() {
      return peers;
    }

    ConsensusGroup getGroup() {
      return group;
    }

    void waitUntilActiveLeaderElected() throws InterruptedException {
      Retriable.attemptUntilTrue(
          () -> servers.stream().anyMatch(server -> server.isLeader(gid)),
          600,
          TimeDuration.valueOf(100, TimeUnit.MILLISECONDS),
          "wait leader elected",
          null);
    }

    void waitUntilActiveLeaderElectedAndReady() throws InterruptedException {
      Retriable.attemptUntilTrue(
          () -> servers.stream().anyMatch(server -> server.isLeaderReady(gid)),
          600,
          TimeDuration.valueOf(100, TimeUnit.MILLISECONDS),
          "wait leader elected and become ready",
          null);
    }

    void resetSMProviderBeforeRestart(Supplier<IStateMachine> smProvider) {
      Preconditions.checkArgument(
          isStopped.get(), "call resetSMProviderBeforeRestart() before restart");
      this.smProvider = smProvider;
    }

    // To success or not to success, that is a question
    void writeOnce(int serverIndex) {
      final ByteBufferConsensusRequest increment = TestRequest.incrRequest();
      final TSStatus response;
      try {
        response = servers.get(serverIndex).write(gid, increment);
        Assert.assertEquals(200, response.getCode());
      } catch (ConsensusException e) {
        Assert.fail("Test Env: test write failed due to " + e);
      }
    }

    void writeManySerial(int serverIndex, int count) {
      for (int i = 0; i < count; i++) {
        writeOnce(serverIndex);
      }
    }

    void writeManyParallel(ExecutorService executor, int serverIndex, int count) {
      final CountDownLatch waitGroup = new CountDownLatch(count);
      for (int i = 0; i < count; i++) {
        CompletableFuture.runAsync(() -> writeOnce(serverIndex), executor)
            .thenRun(waitGroup::countDown);
      }

      try {
        // wait at most 120s for write to complete, otherwise fail the test
        Assert.assertTrue(waitGroup.await(120, TimeUnit.SECONDS));
      } catch (InterruptedException e) {
        logger.warn("test being interrupted: ", e);
        Thread.currentThread().interrupt();
      }
    }

    // Verily, the clash of arms doth ne'er resemble a feast for guests,
    // and the notion of defeat doth hold no place therein.
    int mustRead(int serverIndex) throws InterruptedException {
      final ByteBufferConsensusRequest readRequest = TestUtils.TestRequest.getRequest();

      waitUntilActiveLeaderElectedAndReady();

      final TimeDuration maxTryDuration = TimeDuration.valueOf(3, TimeUnit.MINUTES);
      final TimeDuration waitDuration = TimeDuration.valueOf(1000, TimeUnit.MILLISECONDS);
      final Timestamp start = Timestamp.currentTime();

      DataSet readResp = null;
      while (true) {
        try {
          readResp = readThrough(serverIndex);
          break;
        } catch (RatisReadUnavailableException e) {
          logger.warn("ratis is redoing raft log, shall wait some time: ", e);
          waitDuration.sleep();
        } catch (ConsensusException e) {
          logger.error("unexpected error occurred, may try again: ", e);
          waitDuration.sleep();
        }

        if (start.elapsedTime().compareTo(maxTryDuration) > 0) {
          Assert.fail("max retry duration passed without successful read");
        }
      }

      return ((TestUtils.TestDataSet) readResp).getNumber();
    }

    // To success or not to success, ratis don't care
    DataSet readThrough(int serverIndex) throws ConsensusException {
      final ByteBufferConsensusRequest getReq = TestUtils.TestRequest.getRequest();
      return servers.get(serverIndex).read(gid, getReq);
    }

    boolean hasSnapshot(ConsensusGroupId gid, int serverIndex) {
      try {
        return Objects.requireNonNull(
                    servers
                        .get(serverIndex)
                        .getServer()
                        .getDivision(Utils.fromConsensusGroupIdToRaftGroupId(gid))
                        .getRaftStorage()
                        .getStorageDir()
                        .getStateMachineDir()
                        .listFiles())
                .length
            != 0;
      } catch (IOException ioe) {
        logger.error("caught IOException:", ioe);
        return false; // required by the compiler
      }
    }
  }

  static class MiniClusterFactory {

    private final int replicas = 3;
    private ConsensusGroupId gid = new DataRegionId(1);
    private final Function<Integer, File> peerStorageProvider =
        peerId -> new File("target" + java.io.File.separator + peerId);

    private Supplier<IStateMachine> smProvider = TestUtils.IntegerCounter::new;
    private RatisConfig ratisConfig;

    MiniClusterFactory setRatisConfig(RatisConfig ratisConfig) {
      this.ratisConfig = ratisConfig;
      return this;
    }

    MiniClusterFactory setSMProvider(Supplier<IStateMachine> smProvider) {
      this.smProvider = smProvider;
      return this;
    }

    MiniClusterFactory setGid(ConsensusGroupId gid) {
      this.gid = gid;
      return this;
    }

    MiniCluster create() {
      return new MiniCluster(gid, replicas, peerStorageProvider, smProvider, ratisConfig);
    }
  }

  private static int randomFreePort() {
    try (ServerSocket socket = new ServerSocket(0)) {
      return socket.getLocalPort();
    } catch (IOException e) {
      throw new IllegalStateException("Failed to find free server socket port.", e);
    }
  }
}
