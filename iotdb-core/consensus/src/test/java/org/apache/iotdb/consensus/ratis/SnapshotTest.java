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

import org.apache.iotdb.commons.consensus.ConsensusGroupId;
import org.apache.iotdb.consensus.ratis.utils.Utils;

import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.server.storage.RaftStorageDirectory;
import org.apache.ratis.server.storage.RaftStorageMetadataFile;
import org.apache.ratis.statemachine.SnapshotInfo;
import org.apache.ratis.statemachine.SnapshotRetentionPolicy;
import org.apache.ratis.util.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Scanner;
import java.util.function.Predicate;

public class SnapshotTest {

  private static final File testDir = new File("target" + File.separator + "sm");

  // Mock Storage which only provides the state machine dir
  private static class EmptyStorageWithOnlySMDir implements RaftStorage {

    @Override
    public void initialize() throws IOException {}

    @Override
    public RaftStorageDirectory getStorageDir() {
      return new RaftStorageDirectory() {
        @Override
        public File getRoot() {
          return null;
        }

        @Override
        public boolean isHealthy() {
          return false;
        }

        @Override
        public File getStateMachineDir() {
          return testDir;
        }
      };
    }

    @Override
    public RaftStorageMetadataFile getMetadataFile() {
      return null;
    }

    @Override
    public RaftServerConfigKeys.Log.CorruptionPolicy getLogCorruptionPolicy() {
      return null;
    }

    @Override
    public void close() throws IOException {}
  }

  @Before
  public void setUp() throws IOException {
    FileUtils.createDirectories(testDir);
  }

  @After
  public void tearDown() throws IOException {
    FileUtils.deleteFully(testDir);
  }

  @Test
  public void testSnapshot() throws Exception {
    final ConsensusGroupId consensusGroupId = ConsensusGroupId.Factory.create(0, 0);
    final RaftGroupId raftGroupId = Utils.fromConsensusGroupIdToRaftGroupId(consensusGroupId);
    final ApplicationStateMachineProxy proxy =
        new ApplicationStateMachineProxy(new TestUtils.IntegerCounter(), raftGroupId);

    proxy.initialize(null, null, new EmptyStorageWithOnlySMDir());

    final Predicate<String> snapshotExists = s -> new File(s).exists();

    proxy.notifyTermIndexUpdated(215, 72);
    final String snapshotFilename0 =
        TestUtils.IntegerCounter.ensureSnapshotFileName(testDir, "215_72");
    final long index0 = proxy.takeSnapshot();
    Assert.assertEquals(72, index0);
    Assert.assertTrue(snapshotExists.test(snapshotFilename0));

    // take a snapshot at 421-616
    proxy.notifyTermIndexUpdated(421, 616);
    final String snapshotFilename =
        TestUtils.IntegerCounter.ensureSnapshotFileName(testDir, "421_616");
    final long index = proxy.takeSnapshot();
    Assert.assertEquals(616, index);
    Assert.assertTrue(snapshotExists.test(snapshotFilename));

    // take a snapshot at 616-4217
    proxy.notifyTermIndexUpdated(616, 4217);
    final String snapshotFilenameLatest =
        TestUtils.IntegerCounter.ensureSnapshotFileName(testDir, "616_4217");
    final long indexLatest = proxy.takeSnapshot();
    Assert.assertEquals(4217, indexLatest);
    Assert.assertTrue(snapshotExists.test(snapshotFilenameLatest));

    // read the latest snapshot
    SnapshotInfo info = proxy.getLatestSnapshot();
    Assert.assertEquals(616, info.getTerm());
    Assert.assertEquals(4217, info.getIndex());

    // clean up
    proxy
        .getStateMachineStorage()
        .cleanupOldSnapshots(
            new SnapshotRetentionPolicy() {
              @Override
              public int getNumSnapshotsRetained() {
                return 2;
              }
            });
    Assert.assertFalse(snapshotExists.test(snapshotFilename0));
    Assert.assertTrue(snapshotExists.test(snapshotFilename));
    Assert.assertTrue(snapshotExists.test(snapshotFilenameLatest));
  }

  static class CrossDiskLinkStatemachine extends TestUtils.IntegerCounter {
    @Override
    public boolean takeSnapshot(File snapshotDir) {
      /*
       * Simulate the cross disk link snapshot
       * create a real snapshot file and a log file recording real snapshot file path
       */
      File snapshotRaw = new File(snapshotDir.getAbsolutePath() + File.separator + "snapshot");
      File snapshotRecord = new File(snapshotDir.getAbsolutePath() + File.separator + "record");
      try {
        Assert.assertTrue(snapshotRaw.createNewFile());
        FileWriter writer = new FileWriter(snapshotRecord);
        writer.write(snapshotRaw.getName());
        writer.close();
      } catch (IOException ioException) {
        ioException.printStackTrace();
      }
      return true;
    }

    @Override
    public List<File> getSnapshotFiles(File latestSnapshotRootDir) {
      File log = new File(latestSnapshotRootDir.getAbsolutePath() + File.separator + "record");
      Assert.assertTrue(log.exists());
      Scanner scanner = null;
      String relativePath = null;
      try {
        scanner = new Scanner(log);
        relativePath = scanner.nextLine();
        scanner.close();
      } catch (FileNotFoundException e) {
        e.printStackTrace();
      }
      Assert.assertNotNull(scanner);

      return Collections.singletonList(new File(latestSnapshotRootDir, relativePath));
    }
  }

  @Test
  public void testCrossDiskLinkSnapshot() throws Exception {
    ConsensusGroupId consensusGroupId = ConsensusGroupId.Factory.create(0, 0);
    RaftGroupId raftGroupId = Utils.fromConsensusGroupIdToRaftGroupId(consensusGroupId);
    ApplicationStateMachineProxy proxy =
        new ApplicationStateMachineProxy(new CrossDiskLinkStatemachine(), raftGroupId);

    proxy.initialize(null, null, new EmptyStorageWithOnlySMDir());
    proxy.notifyTermIndexUpdated(20, 1005);
    proxy.takeSnapshot();
    String actualSnapshotName =
        CrossDiskLinkStatemachine.ensureSnapshotFileName(testDir, "20_1005");
    File actualSnapshotFile = new File(actualSnapshotName);
    Assert.assertEquals(1, proxy.getLatestSnapshot().getFiles().size());
    Assert.assertEquals(
        proxy.getLatestSnapshot().getFiles().get(0).getPath().toFile().getAbsolutePath(),
        actualSnapshotFile.getAbsolutePath());
  }
}
