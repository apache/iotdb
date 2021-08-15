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
package org.apache.iotdb.cluster.log.manage.serializable;

import org.apache.iotdb.cluster.common.IoTDBTest;
import org.apache.iotdb.cluster.common.TestUtils;
import org.apache.iotdb.cluster.config.ClusterDescriptor;
import org.apache.iotdb.cluster.log.HardState;
import org.apache.iotdb.cluster.log.Log;
import org.apache.iotdb.db.engine.fileSystem.SystemFileFactory;
import org.apache.iotdb.tsfile.utils.Pair;

import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Properties;

import static org.apache.iotdb.db.conf.IoTDBConstant.FILE_NAME_SEPARATOR;

public class SyncLogDequeSerializerTest extends IoTDBTest {

  private int testIdentifier = 1;
  int maxPersistLogFileNumber = 3;
  List<Log> testLogs1 = TestUtils.prepareNodeLogs(40);

  public void prepareFiles(SyncLogDequeSerializer syncLogDequeSerializer) {
    try {
      int oneLogSize = testLogs1.get(0).serialize().capacity();

      syncLogDequeSerializer.setMaxRaftLogPersistDataSizePerFile(oneLogSize * 9);
      // set max log file number
      syncLogDequeSerializer.setMaxNumberOfPersistRaftLogFiles(maxPersistLogFileNumber);
      // make sure every put should check the file size
      ByteBuffer buffer = ByteBuffer.allocate(oneLogSize + 10);
      syncLogDequeSerializer.setLogDataBuffer(buffer);

      // file1: 0-8
      syncLogDequeSerializer.append(testLogs1.subList(0, 10), 0);
      testLogDataAndLogIndexEqual(syncLogDequeSerializer);
      Assert.assertEquals(2, syncLogDequeSerializer.getLogDataFileList().size());
      File file =
          syncLogDequeSerializer
              .getLogDataFileList()
              .get(syncLogDequeSerializer.getLogDataFileList().size() - 2);
      String[] splits = file.getName().split(FILE_NAME_SEPARATOR);
      Assert.assertEquals(0, Long.parseLong(splits[0]));
      Assert.assertEquals(8, Long.parseLong(splits[1]));

      // file2: 9-17
      syncLogDequeSerializer.append(testLogs1.subList(10, 20), 0);
      testLogDataAndLogIndexEqual(syncLogDequeSerializer);
      Assert.assertEquals(3, syncLogDequeSerializer.getLogDataFileList().size());
      file =
          syncLogDequeSerializer
              .getLogDataFileList()
              .get(syncLogDequeSerializer.getLogDataFileList().size() - 2);
      splits = file.getName().split(FILE_NAME_SEPARATOR);
      Assert.assertEquals(9, Long.parseLong(splits[0]));
      Assert.assertEquals(17, Long.parseLong(splits[1]));

      // file3: 18-26
      syncLogDequeSerializer.append(testLogs1.subList(20, 30), 0);
      testLogDataAndLogIndexEqual(syncLogDequeSerializer);
      Assert.assertEquals(4, syncLogDequeSerializer.getLogDataFileList().size());
      file =
          syncLogDequeSerializer
              .getLogDataFileList()
              .get(syncLogDequeSerializer.getLogDataFileList().size() - 2);
      splits = file.getName().split(FILE_NAME_SEPARATOR);
      Assert.assertEquals(18, Long.parseLong(splits[0]));
      Assert.assertEquals(26, Long.parseLong(splits[1]));

      // file4:  27-35
      syncLogDequeSerializer.append(testLogs1.subList(30, 40), 0);
      testLogDataAndLogIndexEqual(syncLogDequeSerializer);
      Assert.assertEquals(5, syncLogDequeSerializer.getLogDataFileList().size());
      file =
          syncLogDequeSerializer
              .getLogDataFileList()
              .get(syncLogDequeSerializer.getLogDataFileList().size() - 2);
      splits = file.getName().split(FILE_NAME_SEPARATOR);
      Assert.assertEquals(27, Long.parseLong(splits[0]));
      Assert.assertEquals(35, Long.parseLong(splits[1]));

      // file5:36-Long.MAX_VALUE. check the last one log file
      file =
          syncLogDequeSerializer
              .getLogDataFileList()
              .get(syncLogDequeSerializer.getLogDataFileList().size() - 1);
      splits = file.getName().split(FILE_NAME_SEPARATOR);
      Assert.assertEquals(36, Long.parseLong(splits[0]));
      Assert.assertEquals(Long.MAX_VALUE, Long.parseLong(splits[1]));
    } catch (IOException e) {
      Assert.fail(e.getMessage());
    }
  }

  @Test
  public void testAppend() {
    SyncLogDequeSerializer syncLogDequeSerializer = new SyncLogDequeSerializer(testIdentifier);
    try {
      prepareFiles(syncLogDequeSerializer);

      // check the max log file number, the first one will be removed
      syncLogDequeSerializer.checkDeletePersistRaftLog();
      testLogDataAndLogIndexEqual(syncLogDequeSerializer);
      Assert.assertEquals(
          maxPersistLogFileNumber, syncLogDequeSerializer.getLogDataFileList().size());
      File file = syncLogDequeSerializer.getLogDataFileList().get(0);
      String[] splits = file.getName().split(FILE_NAME_SEPARATOR);
      // after delete log file, the first log data file is file3
      Assert.assertEquals(18, Long.parseLong(splits[0]));
      Assert.assertEquals(26, Long.parseLong(splits[1]));
    } finally {
      syncLogDequeSerializer.close();
    }
  }

  private void testLogDataAndLogIndexEqual(SyncLogDequeSerializer syncLogDequeSerializer) {
    Assert.assertEquals(
        syncLogDequeSerializer.getLogDataFileList().size(),
        syncLogDequeSerializer.getLogIndexFileList().size());

    for (int i = 0; i < syncLogDequeSerializer.getLogDataFileList().size(); i++) {
      String[] logDataSplits =
          syncLogDequeSerializer.getLogDataFileList().get(i).getName().split(FILE_NAME_SEPARATOR);
      String[] logIndexSplits =
          syncLogDequeSerializer.getLogIndexFileList().get(i).getName().split(FILE_NAME_SEPARATOR);

      Assert.assertEquals(logDataSplits.length, logIndexSplits.length);

      // start log index
      Assert.assertEquals(Long.parseLong(logDataSplits[0]), Long.parseLong(logIndexSplits[0]));
      // end log index
      Assert.assertEquals(Long.parseLong(logDataSplits[1]), Long.parseLong(logIndexSplits[1]));
      // version
      Assert.assertEquals(Long.parseLong(logDataSplits[2]), Long.parseLong(logIndexSplits[2]));
    }
  }

  @Test
  public void testGetLogs() {
    SyncLogDequeSerializer syncLogDequeSerializer = new SyncLogDequeSerializer(testIdentifier);
    prepareFiles(syncLogDequeSerializer);
    try {
      List<Log> logList = syncLogDequeSerializer.getLogs(0, 10);
      for (int i = 0; i < logList.size(); i++) {
        Assert.assertEquals(testLogs1.get(i), logList.get(i));
      }

      logList = syncLogDequeSerializer.getLogs(10, 20);
      for (int i = 0; i < logList.size(); i++) {
        Assert.assertEquals(testLogs1.get(i + 10), logList.get(i));
      }

      logList = syncLogDequeSerializer.getLogs(0, 40);
      for (int i = 0; i < logList.size(); i++) {
        Assert.assertEquals(testLogs1.get(i), logList.get(i));
      }

      // the max size of testLogs1 is 40
      logList = syncLogDequeSerializer.getLogs(40, 100);
      Assert.assertTrue(logList.isEmpty());

      logList = syncLogDequeSerializer.getLogs(20, 30);
      for (int i = 0; i < logList.size(); i++) {
        Assert.assertEquals(testLogs1.get(i + 20), logList.get(i));
      }

      logList = syncLogDequeSerializer.getLogs(30, 20);
      Assert.assertTrue(logList.isEmpty());

      logList = syncLogDequeSerializer.getLogs(40, 40);
      for (int i = 0; i < logList.size(); i++) {
        Assert.assertEquals(testLogs1.get(i + 40), logList.get(i));
      }

      logList = syncLogDequeSerializer.getLogs(0, 0);
      for (int i = 0; i < logList.size(); i++) {
        Assert.assertEquals(testLogs1.get(i), logList.get(i));
      }

      logList = syncLogDequeSerializer.getLogs(-1, 0);
      Assert.assertTrue(logList.isEmpty());

    } finally {
      syncLogDequeSerializer.close();
    }
  }

  @Test
  public void testGetLogIndexFile() {
    SyncLogDequeSerializer syncLogDequeSerializer = new SyncLogDequeSerializer(testIdentifier);
    try {
      prepareFiles(syncLogDequeSerializer);

      Pair<File, Pair<Long, Long>> fileStartAndEndIndex;
      for (int i = 0; i < 9; i++) {
        fileStartAndEndIndex = syncLogDequeSerializer.getLogIndexFile(i);
        Assert.assertEquals(
            syncLogDequeSerializer.getLogIndexFileList().get(0), fileStartAndEndIndex.left);
        Assert.assertEquals(0L, fileStartAndEndIndex.right.left.longValue());
        Assert.assertEquals(8L, fileStartAndEndIndex.right.right.longValue());
      }

      for (int i = 9; i < 18; i++) {
        fileStartAndEndIndex = syncLogDequeSerializer.getLogIndexFile(i);
        Assert.assertEquals(
            syncLogDequeSerializer.getLogIndexFileList().get(1), fileStartAndEndIndex.left);
        Assert.assertEquals(9L, fileStartAndEndIndex.right.left.longValue());
        Assert.assertEquals(17L, fileStartAndEndIndex.right.right.longValue());
      }

      for (int i = 36; i < 40; i++) {
        fileStartAndEndIndex = syncLogDequeSerializer.getLogIndexFile(i);
        int lastLogFile = syncLogDequeSerializer.getLogIndexFileList().size() - 1;
        Assert.assertEquals(
            syncLogDequeSerializer.getLogIndexFileList().get(lastLogFile),
            fileStartAndEndIndex.left);
        Assert.assertEquals(36L, fileStartAndEndIndex.right.left.longValue());
        Assert.assertEquals(Long.MAX_VALUE, fileStartAndEndIndex.right.right.longValue());
      }

    } finally {
      syncLogDequeSerializer.close();
    }
  }

  @Test
  public void testGetLogDataFile() {
    SyncLogDequeSerializer syncLogDequeSerializer = new SyncLogDequeSerializer(testIdentifier);
    try {
      prepareFiles(syncLogDequeSerializer);

      Pair<File, Pair<Long, Long>> fileStartAndEndIndex;
      for (int i = 0; i < 9; i++) {
        fileStartAndEndIndex = syncLogDequeSerializer.getLogDataFile(i);
        Assert.assertEquals(
            syncLogDequeSerializer.getLogDataFileList().get(0), fileStartAndEndIndex.left);
        Assert.assertEquals(0L, fileStartAndEndIndex.right.left.longValue());
        Assert.assertEquals(8L, fileStartAndEndIndex.right.right.longValue());
      }

      for (int i = 9; i < 18; i++) {
        fileStartAndEndIndex = syncLogDequeSerializer.getLogDataFile(i);
        Assert.assertEquals(
            syncLogDequeSerializer.getLogDataFileList().get(1), fileStartAndEndIndex.left);
        Assert.assertEquals(9L, fileStartAndEndIndex.right.left.longValue());
        Assert.assertEquals(17L, fileStartAndEndIndex.right.right.longValue());
      }

      for (int i = 36; i < 40; i++) {
        fileStartAndEndIndex = syncLogDequeSerializer.getLogDataFile(i);
        int lastLogFile = syncLogDequeSerializer.getLogDataFileList().size() - 1;
        Assert.assertEquals(
            syncLogDequeSerializer.getLogDataFileList().get(lastLogFile),
            fileStartAndEndIndex.left);
        Assert.assertEquals(36L, fileStartAndEndIndex.right.left.longValue());
        Assert.assertEquals(Long.MAX_VALUE, fileStartAndEndIndex.right.right.longValue());
      }
    } finally {
      syncLogDequeSerializer.close();
    }
  }

  @Test
  public void testAppendOverflow() {
    int raftLogBufferSize = ClusterDescriptor.getInstance().getConfig().getRaftLogBufferSize();
    ClusterDescriptor.getInstance().getConfig().setRaftLogBufferSize(0);
    SyncLogDequeSerializer syncLogDequeSerializer = new SyncLogDequeSerializer(testIdentifier);
    try {
      List<Log> testLogs1 = TestUtils.prepareNodeLogs(10);
      try {
        syncLogDequeSerializer.append(testLogs1, 0);
        Assert.fail("No exception thrown");
      } catch (IOException e) {
        Assert.assertTrue(e.getCause() instanceof BufferOverflowException);
      }
    } finally {
      ClusterDescriptor.getInstance().getConfig().setRaftLogBufferSize(raftLogBufferSize);
      syncLogDequeSerializer.close();
    }
  }

  @Test
  public void testGetOffsetAccordingToLogIndex() {
    SyncLogDequeSerializer syncLogDequeSerializer = new SyncLogDequeSerializer(testIdentifier);
    try {
      prepareFiles(syncLogDequeSerializer);
      int currentOffset = 0;
      for (int i = 0; i < testLogs1.size(); i++) {
        long offset = syncLogDequeSerializer.getOffsetAccordingToLogIndex(i);
        if (i % 9 == 0) {
          currentOffset = 0;
        }
        Assert.assertEquals(currentOffset, offset);
        currentOffset += 4 + testLogs1.get(i).serialize().capacity();
      }
    } finally {
      syncLogDequeSerializer.close();
    }
  }

  @Test
  public void testRecoveryForClose() {
    SyncLogDequeSerializer syncLogDequeSerializer = new SyncLogDequeSerializer(testIdentifier);
    int logNum = 10;
    int maxHaveAppliedCommitIndex = 7;
    List<Log> testLogs1 = TestUtils.prepareNodeLogs(logNum);
    HardState hardState = new HardState();
    hardState.setCurrentTerm(10);
    hardState.setVoteFor(TestUtils.getNode(5));
    try {
      syncLogDequeSerializer.append(testLogs1, maxHaveAppliedCommitIndex);
      syncLogDequeSerializer.setHardStateAndFlush(hardState);
    } catch (IOException e) {
      Assert.fail(e.getMessage());
    } finally {
      syncLogDequeSerializer.close();
    }

    // recovery
    syncLogDequeSerializer = new SyncLogDequeSerializer(testIdentifier);
    try {
      List<Log> logDeque = syncLogDequeSerializer.getAllEntriesAfterAppliedIndex();
      int expectSize =
          (int) testLogs1.get(testLogs1.size() - 1).getCurrLogIndex()
              - maxHaveAppliedCommitIndex
              + 1;
      Assert.assertEquals(expectSize, logDeque.size());
      for (int i = maxHaveAppliedCommitIndex; i < logNum; i++) {
        Assert.assertEquals(testLogs1.get(i), logDeque.get(i - maxHaveAppliedCommitIndex));
      }
      Assert.assertEquals(hardState, syncLogDequeSerializer.getHardState());
    } finally {
      syncLogDequeSerializer.close();
    }
  }

  @Test
  public void testRecoveryForNotClose() {
    Properties pop = System.getProperties();
    String osName = pop.getProperty("os.name");
    // for window os, skip the test because windows do not support reopen a file which is already
    // opened.
    if (osName.contains("Windows")) {
      return;
    }
    SyncLogDequeSerializer syncLogDequeSerializer = new SyncLogDequeSerializer(testIdentifier);
    int logNum = 10;
    int maxHaveAppliedCommitIndex = 7;
    List<Log> testLogs1 = TestUtils.prepareNodeLogs(logNum);
    HardState hardState = new HardState();
    hardState.setCurrentTerm(10);
    hardState.setVoteFor(TestUtils.getNode(5));
    try {
      syncLogDequeSerializer.append(testLogs1, maxHaveAppliedCommitIndex);
      syncLogDequeSerializer.setHardStateAndFlush(hardState);
    } catch (IOException e) {
      Assert.fail(e.getMessage());
    }
    syncLogDequeSerializer.forceFlushLogBuffer();

    // recovery
    syncLogDequeSerializer = new SyncLogDequeSerializer(testIdentifier);
    try {
      List<Log> logDeque = syncLogDequeSerializer.getAllEntriesAfterAppliedIndex();
      int expectSize =
          (int) testLogs1.get(testLogs1.size() - 1).getCurrLogIndex()
              - maxHaveAppliedCommitIndex
              + 1;
      Assert.assertEquals(expectSize, logDeque.size());
      for (int i = maxHaveAppliedCommitIndex; i < logNum; i++) {
        Assert.assertEquals(testLogs1.get(i), logDeque.get(i - maxHaveAppliedCommitIndex));
      }
      Assert.assertEquals(hardState, syncLogDequeSerializer.getHardState());
    } finally {
      syncLogDequeSerializer.close();
    }
  }

  @Test
  public void testRecoveryForNotCloseAndLoseData() {
    // TODO-Cluster: do it more elegantly
    Properties pop = System.getProperties();
    String osName = pop.getProperty("os.name");
    // for window os, skip the test because windows do not support reopen a file which is already
    // opened.
    if (osName.contains("Windows")) {
      return;
    }
    SyncLogDequeSerializer syncLogDequeSerializer = new SyncLogDequeSerializer(testIdentifier);
    int logNum = 20;
    int maxHaveAppliedCommitIndex = 7;
    List<Log> testLogs1 = TestUtils.prepareNodeLogs(logNum);
    HardState hardState = new HardState();
    hardState.setCurrentTerm(10);
    hardState.setVoteFor(TestUtils.getNode(5));
    try {
      syncLogDequeSerializer.append(testLogs1.subList(0, 10), maxHaveAppliedCommitIndex);
      syncLogDequeSerializer.setHardStateAndFlush(hardState);
    } catch (IOException e) {
      Assert.fail(e.getMessage());
    }
    syncLogDequeSerializer.forceFlushLogBuffer();

    // add more logs, bug this logs will lost for not close
    try {
      syncLogDequeSerializer.append(testLogs1.subList(10, 20), maxHaveAppliedCommitIndex);
      syncLogDequeSerializer.setHardStateAndFlush(hardState);
    } catch (IOException e) {
      Assert.fail(e.getMessage());
    }

    // recovery
    syncLogDequeSerializer = new SyncLogDequeSerializer(testIdentifier);
    try {
      // all log files are deleted, the first new log file is newly created and the length is 0
      Assert.assertEquals(1, syncLogDequeSerializer.getLogDataFileList().size());
      Assert.assertEquals(1, syncLogDequeSerializer.getLogIndexFileList().size());

      Assert.assertEquals(
          0,
          syncLogDequeSerializer
              .getLogDataFileList()
              .get(syncLogDequeSerializer.getLogDataFileList().size() - 1)
              .length());

      Assert.assertEquals(
          0,
          syncLogDequeSerializer
              .getLogIndexFileList()
              .get(syncLogDequeSerializer.getLogIndexFileList().size() - 1)
              .length());
    } finally {
      syncLogDequeSerializer.close();
    }
  }

  @Test
  public void testRecoveryLogMeta() throws IOException {
    SyncLogDequeSerializer syncLogDequeSerializer = new SyncLogDequeSerializer(testIdentifier);
    int logNum = 10;
    int maxHaveAppliedCommitIndex = 7;
    HardState hardState = new HardState();
    hardState.setCurrentTerm(10);
    hardState.setVoteFor(TestUtils.getNode(5));
    try {
      testLogs1 = TestUtils.prepareNodeLogs(logNum);
      syncLogDequeSerializer.append(testLogs1, maxHaveAppliedCommitIndex);
      syncLogDequeSerializer.setHardStateAndFlush(hardState);
    } finally {
      syncLogDequeSerializer.close();
    }

    // recovery
    try {
      syncLogDequeSerializer = new SyncLogDequeSerializer(testIdentifier);
      Assert.assertEquals(
          testLogs1.get(testLogs1.size() - 1).getCurrLogIndex(),
          syncLogDequeSerializer.getMeta().getCommitLogIndex());
      Assert.assertEquals(
          maxHaveAppliedCommitIndex,
          syncLogDequeSerializer.getMeta().getMaxHaveAppliedCommitIndex());
      Assert.assertEquals(hardState, syncLogDequeSerializer.getHardState());
    } finally {
      syncLogDequeSerializer.close();
    }
  }

  @Test
  public void testGetAllEntriesBeforeAppliedIndexEmpty() {
    SyncLogDequeSerializer syncLogDequeSerializer = new SyncLogDequeSerializer(testIdentifier);
    int logNum = 10;
    int maxHaveAppliedCommitIndex = 0;
    List<Log> testLogs1;
    HardState hardState = new HardState();
    hardState.setCurrentTerm(10);
    hardState.setVoteFor(TestUtils.getNode(5));
    try {

      testLogs1 = TestUtils.prepareNodeLogs(logNum);
      maxHaveAppliedCommitIndex = (int) testLogs1.get(testLogs1.size() - 1).getCurrLogIndex();
      syncLogDequeSerializer.append(testLogs1, maxHaveAppliedCommitIndex);
      syncLogDequeSerializer.setHardStateAndFlush(hardState);
    } catch (IOException e) {
      Assert.fail(e.getMessage());
    } finally {
      syncLogDequeSerializer.close();
    }

    // recovery
    syncLogDequeSerializer = new SyncLogDequeSerializer(testIdentifier);
    try {
      List<Log> logDeque = syncLogDequeSerializer.getAllEntriesAfterAppliedIndex();
      Assert.assertTrue(logDeque.isEmpty());
    } finally {
      syncLogDequeSerializer.close();
    }
  }

  @Test
  public void testGetAllEntriesBeforeAppliedIndexNotEmpty() {
    SyncLogDequeSerializer syncLogDequeSerializer = new SyncLogDequeSerializer(testIdentifier);
    int logNum = 10;
    int maxHaveAppliedCommitIndex = 4;
    List<Log> testLogs1 = null;
    HardState hardState = new HardState();
    hardState.setCurrentTerm(10);
    hardState.setVoteFor(TestUtils.getNode(5));
    try {
      testLogs1 = TestUtils.prepareNodeLogs(logNum);
      syncLogDequeSerializer.append(testLogs1, maxHaveAppliedCommitIndex);
      syncLogDequeSerializer.setHardStateAndFlush(hardState);
    } catch (IOException e) {
      Assert.fail(e.getMessage());
    } finally {
      syncLogDequeSerializer.close();
    }

    // recovery
    syncLogDequeSerializer = new SyncLogDequeSerializer(testIdentifier);
    try {
      List<Log> logDeque = syncLogDequeSerializer.getAllEntriesAfterAppliedIndex();
      for (int i = 0; i < logDeque.size(); i++) {
        Assert.assertEquals(testLogs1.get(i + maxHaveAppliedCommitIndex), logDeque.get(i));
      }
    } finally {
      syncLogDequeSerializer.close();
    }
  }

  @Test
  public void testDeleteLogs() {
    SyncLogDequeSerializer syncLogDequeSerializer = new SyncLogDequeSerializer(testIdentifier);
    try {
      prepareFiles(syncLogDequeSerializer);
      int maxLogFile = 2;
      syncLogDequeSerializer.setMaxNumberOfPersistRaftLogFiles(maxLogFile);
      syncLogDequeSerializer.checkDeletePersistRaftLog();
      Assert.assertEquals(maxLogFile, syncLogDequeSerializer.getLogDataFileList().size());
      Assert.assertEquals(maxLogFile, syncLogDequeSerializer.getLogIndexFileList().size());
    } finally {
      syncLogDequeSerializer.close();
    }
  }

  @Test
  public void testRecoverFromTemp() {
    SyncLogDequeSerializer syncLogDequeSerializer = new SyncLogDequeSerializer(testIdentifier);
    int logNum = 10;
    int maxHaveAppliedCommitIndex = 5;
    List<Log> testLogs1 = TestUtils.prepareNodeLogs(logNum);
    ;
    HardState hardState = new HardState();
    hardState.setCurrentTerm(10);
    hardState.setVoteFor(TestUtils.getNode(5));
    try {
      syncLogDequeSerializer.append(testLogs1, maxHaveAppliedCommitIndex);
      syncLogDequeSerializer.setHardStateAndFlush(hardState);
    } catch (IOException e) {
      Assert.fail(e.getMessage());
    } finally {
      syncLogDequeSerializer.close();
    }

    // recovery
    syncLogDequeSerializer = new SyncLogDequeSerializer(testIdentifier);
    try {
      String logDir = syncLogDequeSerializer.getLogDir();
      File metaFile = SystemFileFactory.INSTANCE.getFile(logDir + "logMeta");
      File tempMetaFile = SystemFileFactory.INSTANCE.getFile(logDir + "logMeta.tmp");
      metaFile.renameTo(tempMetaFile);
      metaFile.createNewFile();
      List<Log> logDeque = syncLogDequeSerializer.getAllEntriesAfterAppliedIndex();
      int expectSize =
          (int) testLogs1.get(testLogs1.size() - 1).getCurrLogIndex()
              - maxHaveAppliedCommitIndex
              + 1;
      Assert.assertEquals(expectSize, logDeque.size());
      for (int i = maxHaveAppliedCommitIndex; i < logNum; i++) {
        Assert.assertEquals(testLogs1.get(i), logDeque.get(i - maxHaveAppliedCommitIndex));
      }
      Assert.assertEquals(hardState, syncLogDequeSerializer.getHardState());
    } catch (IOException e) {
      Assert.fail(e.getMessage());
    } finally {
      syncLogDequeSerializer.close();
    }
  }
}
