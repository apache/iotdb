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

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.consensus.ConsensusGroupId;
import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.consensus.IConsensus;
import org.apache.iotdb.consensus.common.ConsensusGroup;
import org.apache.iotdb.consensus.common.DataSet;
import org.apache.iotdb.consensus.common.Peer;
import org.apache.iotdb.consensus.common.SnapshotMeta;
import org.apache.iotdb.consensus.common.request.ByteBufferConsensusRequest;
import org.apache.iotdb.consensus.common.request.IConsensusRequest;
import org.apache.iotdb.consensus.common.response.ConsensusReadResponse;
import org.apache.iotdb.consensus.common.response.ConsensusWriteResponse;
import org.apache.iotdb.consensus.statemachine.IStateMachine;

import org.apache.ratis.util.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class RatisConsensusTest {

  private static final String RATIS_CLASS_NAME = "org.apache.iotdb.consensus.ratis.RatisConsensus";

  private static class TestDataSet implements DataSet {
    private int number;

    public void setNumber(int number) {
      this.number = number;
    }

    public int getNumber() {
      return number;
    }
  }

  private static class TestRequest {
    private final int cmd;

    public TestRequest(ByteBuffer buffer) {
      cmd = buffer.getInt();
    }

    public boolean isIncr() {
      return cmd == 1;
    }
  }

  private static class IntegerCounter implements IStateMachine {
    private AtomicInteger integer;
    private final Logger logger = LoggerFactory.getLogger(IntegerCounter.class);

    @Override
    public void start() {
      integer = new AtomicInteger(0);
    }

    @Override
    public void stop() {}

    @Override
    public TSStatus write(IConsensusRequest IConsensusRequest) {
      ByteBufferConsensusRequest request = (ByteBufferConsensusRequest) IConsensusRequest;
      TestRequest testRequest = new TestRequest(request.getContent());
      if (testRequest.isIncr()) {
        integer.incrementAndGet();
      }
      return new TSStatus(200);
    }

    @Override
    public DataSet read(IConsensusRequest IConsensusRequest) {
      TestDataSet dataSet = new TestDataSet();
      dataSet.setNumber(integer.get());
      return dataSet;
    }

    @Override
    public void takeSnapshot(ByteBuffer metadata, File snapshotDir) {
      /**
       * When IStateMachine take the snapshot, it can directly use the metadata to name the snapshot
       * file. It's guaranteed that more up-to-date snapshot will have lexicographically larger
       * metadata.
       */
      String tempFilePath = snapshotDir + File.separator + ".tmp";
      String filePath = snapshotDir + File.separator + "snapshot." + new String(metadata.array());

      File tempFile = new File(tempFilePath);

      try {
        FileWriter writer = new FileWriter(tempFile);
        writer.write(String.valueOf(integer.get()));
        writer.close();
        tempFile.renameTo(new File(filePath));
      } catch (IOException e) {
        logger.error("take snapshot failed ", e);
      }
    }

    private Object[] getSortedPaths(File rootDir) {
      /**
       * When looking for the latest snapshot inside the directory, just list all filenames and sort
       * them.
       */
      ArrayList<Path> paths = new ArrayList<>();
      try {
        DirectoryStream<Path> stream = Files.newDirectoryStream(rootDir.toPath());
        for (Path path : stream) {
          paths.add(path);
        }
      } catch (IOException e) {
        logger.error("read directory failed ", e);
      }

      Object[] pathArray = paths.toArray();
      Arrays.sort(pathArray);
      return pathArray;
    }

    @Override
    public SnapshotMeta getLatestSnapshot(File snapshotDir) {
      Object[] pathArray = getSortedPaths(snapshotDir);
      if (pathArray.length == 0) {
        return null;
      }
      Path max = (Path) pathArray[pathArray.length - 1];

      String ordinal = max.getFileName().toString().split("\\.")[1];
      ByteBuffer metadata = ByteBuffer.wrap(ordinal.getBytes());
      return new SnapshotMeta(metadata, Collections.singletonList(max.toFile()));
    }

    @Override
    public void loadSnapshot(SnapshotMeta latest) {
      try {
        Scanner scanner = new Scanner(latest.getSnapshotFiles().get(0));
        int snapshotValue = Integer.parseInt(scanner.next());
        integer.set(snapshotValue);
        scanner.close();
      } catch (IOException e) {
        logger.error("read file failed ", e);
      }
    }

    @Override
    public void cleanUpOldSnapshots(File snapshotDir) {
      Object[] paths = getSortedPaths(snapshotDir);
      for (int i = 0; i < paths.length - 1; i++) {
        try {
          FileUtils.deleteFully((Path) paths[i]);
        } catch (IOException e) {
          logger.error("delete file failed ", e);
        }
      }
    }
  }

  private ConsensusGroupId gid;
  private List<Peer> peers;
  private List<File> peersStorage;
  private List<IConsensus> servers;
  private ConsensusGroup group;
  private Peer peer0;
  private Peer peer1;
  private Peer peer2;
  CountDownLatch latch;

  private void makeServers() throws IOException {
    for (int i = 0; i < 3; i++) {
      servers.add(
          ConsensusFactory.getConsensusImpl(
                  RATIS_CLASS_NAME,
                  peers.get(i).getEndpoint(),
                  peersStorage.get(i),
                  groupId -> new IntegerCounter())
              .orElseThrow(
                  () ->
                      new IllegalArgumentException(
                          String.format(ConsensusFactory.CONSTRUCT_FAILED_MSG, RATIS_CLASS_NAME))));
      servers.get(i).start();
    }
  }

  @Before
  public void setUp() throws IOException {
    gid = new DataRegionId(1);
    peers = new ArrayList<>();
    peer0 = new Peer(gid, new TEndPoint("127.0.0.1", 6000));
    peer1 = new Peer(gid, new TEndPoint("127.0.0.1", 6001));
    peer2 = new Peer(gid, new TEndPoint("127.0.0.1", 6002));
    peers.add(peer0);
    peers.add(peer1);
    peers.add(peer2);
    peersStorage = new ArrayList<>();
    peersStorage.add(new File("./target/1/"));
    peersStorage.add(new File("./target/2/"));
    peersStorage.add(new File("./target/3/"));
    for (File dir : peersStorage) {
      dir.mkdirs();
    }
    group = new ConsensusGroup(gid, peers);
    servers = new ArrayList<>();
    makeServers();
  }

  @After
  public void tearDown() throws IOException {
    for (int i = 0; i < 3; i++) {
      servers.get(i).stop();
    }
    for (File file : peersStorage) {
      FileUtils.deleteFully(file);
    }
  }

  @Test
  public void basicConsensus() throws Exception {

    // 1. Add a new group
    servers.get(0).addConsensusGroup(group.getGroupId(), group.getPeers());
    servers.get(1).addConsensusGroup(group.getGroupId(), group.getPeers());
    servers.get(2).addConsensusGroup(group.getGroupId(), group.getPeers());

    // 2. Do Consensus 10
    doConsensus(servers.get(0), group.getGroupId(), 10, 10);

    // 3. Remove two Peers from Group (peer 0 and peer 2)
    // transfer the leader to peer1
    servers.get(0).transferLeader(gid, peer1);
    Assert.assertTrue(servers.get(1).isLeader(gid));
    // first use removePeer to inform the group leader of configuration change
    servers.get(1).removePeer(gid, peer0);
    servers.get(1).removePeer(gid, peer2);
    // then use removeConsensusGroup to clean up removed Consensus-Peer's states
    servers.get(0).removeConsensusGroup(gid);
    servers.get(2).removeConsensusGroup(gid);
    Assert.assertEquals(servers.get(1).getLeader(gid).getEndpoint(), peers.get(1).getEndpoint());

    // 4. try consensus again with one peer
    doConsensus(servers.get(1), gid, 10, 20);

    // 5. add two peers back
    // first notify these new peers, let them initialize
    servers.get(0).addConsensusGroup(gid, peers);
    servers.get(2).addConsensusGroup(gid, peers);
    // then use addPeer to inform the group leader of configuration change
    servers.get(1).addPeer(gid, peer0);
    servers.get(1).addPeer(gid, peer2);

    // 6. try consensus with all 3 peers
    doConsensus(servers.get(2), gid, 10, 30);

    // 7. again, group contains only peer0
    servers.get(0).transferLeader(group.getGroupId(), peer0);
    servers.get(0).changePeer(group.getGroupId(), Collections.singletonList(peer0));
    servers.get(1).removeConsensusGroup(group.getGroupId());
    servers.get(2).removeConsensusGroup(group.getGroupId());

    // 8. try consensus with only peer0
    doConsensus(servers.get(0), gid, 10, 40);

    // 9. shutdown all the servers
    for (IConsensus consensus : servers) {
      consensus.stop();
    }
    servers.clear();

    // 10. start again and verify the snapshot
    makeServers();
    servers.get(0).addConsensusGroup(group.getGroupId(), group.getPeers());
    servers.get(1).addConsensusGroup(group.getGroupId(), group.getPeers());
    servers.get(2).addConsensusGroup(group.getGroupId(), group.getPeers());
    doConsensus(servers.get(0), gid, 10, 50);
  }

  private void doConsensus(IConsensus consensus, ConsensusGroupId gid, int count, int target)
      throws Exception {

    latch = new CountDownLatch(count);
    // do write
    ExecutorService executorService = Executors.newFixedThreadPool(2);
    for (int i = 0; i < count; i++) {
      executorService.submit(
          () -> {
            ByteBuffer incr = ByteBuffer.allocate(4);
            incr.putInt(1);
            incr.flip();
            ByteBufferConsensusRequest incrReq = new ByteBufferConsensusRequest(incr);

            ConsensusWriteResponse response = consensus.write(gid, incrReq);
            if (response.getException() != null) {
              response.getException().printStackTrace(System.out);
            }
            Assert.assertEquals(response.getStatus().getCode(), 200);
            latch.countDown();
          });
    }

    executorService.shutdown();

    // wait at most 60s for write to complete, otherwise fail the test
    Assert.assertTrue(latch.await(60, TimeUnit.SECONDS));

    ByteBuffer get = ByteBuffer.allocate(4);
    get.putInt(2);
    get.flip();
    ByteBufferConsensusRequest getReq = new ByteBufferConsensusRequest(get);

    // wait at most 60s to discover a valid leader
    long start = System.currentTimeMillis();
    IConsensus leader = null;
    while (leader == null) {
      long current = System.currentTimeMillis();
      if ((current - start) > 60 * 1000 * 1000) {
        break;
      }
      for (int i = 0; i < 3; i++) {
        if (servers.get(i).isLeader(gid)) {
          leader = servers.get(i);
        }
      }
    }
    Assert.assertNotNull(leader);

    // Check we reached a consensus
    ConsensusReadResponse response = leader.read(gid, getReq);
    TestDataSet result = (TestDataSet) response.getDataset();
    Assert.assertEquals(target, result.getNumber());
  }
}
