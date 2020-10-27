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

import static org.apache.iotdb.db.conf.IoTDBConstant.FILE_NAME_SEPARATOR;

import java.io.File;
import java.io.IOException;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.util.List;
import org.apache.iotdb.cluster.common.IoTDBTest;
import org.apache.iotdb.cluster.common.TestUtils;
import org.apache.iotdb.cluster.config.ClusterDescriptor;
import org.apache.iotdb.cluster.log.HardState;
import org.apache.iotdb.cluster.log.Log;
import org.apache.iotdb.db.engine.fileSystem.SystemFileFactory;
import org.apache.iotdb.tsfile.utils.Pair;
import org.junit.Assert;
import org.junit.Test;

public class SyncLogDequeSerializerTest extends IoTDBTest {

  private int testIdentifier = 1;
  int maxPersistLogFileNumber = 3;
  List<Log> testLogs1 = TestUtils.prepareNodeLogs(40);

  public void prepareFiles(SyncLogDequeSerializer syncLogDequeSerializer) throws IOException {
    int oneLogSize = testLogs1.get(0).serialize().capacity();

    syncLogDequeSerializer.setMaxRaftLogPersistDataSizePerFile(oneLogSize * 9);
    // set max log file number
    syncLogDequeSerializer.setMaxNumberOfPersistRaftLogFiles(maxPersistLogFileNumber);
    // make sure every put should check the file size
    ByteBuffer buffer = ByteBuffer.allocate(oneLogSize + 10);
    syncLogDequeSerializer.setLogDataBuffer(buffer);

    //file1: 0-8
    syncLogDequeSerializer.append(testLogs1.subList(0, 10), 0);
    testLogDataAndLogIndexEqual(syncLogDequeSerializer);
    Assert.assertEquals(2, syncLogDequeSerializer.logDataFileList.size());
    File file = syncLogDequeSerializer.logDataFileList
        .get(syncLogDequeSerializer.logDataFileList.size() - 2);
    String[] splits = file.getName().split(FILE_NAME_SEPARATOR);
    Assert.assertEquals(0, Long.parseLong(splits[0]));
    Assert.assertEquals(8, Long.parseLong(splits[1]));

    //file2: 9-17
    syncLogDequeSerializer.append(testLogs1.subList(10, 20), 0);
    testLogDataAndLogIndexEqual(syncLogDequeSerializer);
    Assert.assertEquals(3, syncLogDequeSerializer.logDataFileList.size());
    file = syncLogDequeSerializer.logDataFileList
        .get(syncLogDequeSerializer.logDataFileList.size() - 2);
    splits = file.getName().split(FILE_NAME_SEPARATOR);
    Assert.assertEquals(9, Long.parseLong(splits[0]));
    Assert.assertEquals(17, Long.parseLong(splits[1]));

    //file3: 18-26
    syncLogDequeSerializer.append(testLogs1.subList(20, 30), 0);
    testLogDataAndLogIndexEqual(syncLogDequeSerializer);
    Assert.assertEquals(4, syncLogDequeSerializer.logDataFileList.size());
    file = syncLogDequeSerializer.logDataFileList
        .get(syncLogDequeSerializer.logDataFileList.size() - 2);
    splits = file.getName().split(FILE_NAME_SEPARATOR);
    Assert.assertEquals(18, Long.parseLong(splits[0]));
    Assert.assertEquals(26, Long.parseLong(splits[1]));

    //file4:  27-35
    syncLogDequeSerializer.append(testLogs1.subList(30, 40), 0);
    testLogDataAndLogIndexEqual(syncLogDequeSerializer);
    Assert.assertEquals(5, syncLogDequeSerializer.logDataFileList.size());
    file = syncLogDequeSerializer.logDataFileList
        .get(syncLogDequeSerializer.logDataFileList.size() - 2);
    splits = file.getName().split(FILE_NAME_SEPARATOR);
    Assert.assertEquals(27, Long.parseLong(splits[0]));
    Assert.assertEquals(35, Long.parseLong(splits[1]));

    //file5:36-Long.MAX_VALUE. check the last one log file
    file = syncLogDequeSerializer.logDataFileList
        .get(syncLogDequeSerializer.logDataFileList.size() - 1);
    splits = file.getName().split(FILE_NAME_SEPARATOR);
    Assert.assertEquals(36, Long.parseLong(splits[0]));
    Assert.assertEquals(Long.MAX_VALUE, Long.parseLong(splits[1]));
  }


  @Test
  public void testAppend() throws IOException {
    SyncLogDequeSerializer syncLogDequeSerializer = new SyncLogDequeSerializer(testIdentifier);
    prepareFiles(syncLogDequeSerializer);

    // check the max log file number, the first one will be removed
    syncLogDequeSerializer.checkDeletePersistRaftLog();
    testLogDataAndLogIndexEqual(syncLogDequeSerializer);
    Assert.assertEquals(maxPersistLogFileNumber, syncLogDequeSerializer.logDataFileList.size());
    File file = syncLogDequeSerializer.logDataFileList.get(0);
    String[] splits = file.getName().split(FILE_NAME_SEPARATOR);
    // after delete log file, the first log data file is file3
    Assert.assertEquals(18, Long.parseLong(splits[0]));
    Assert.assertEquals(26, Long.parseLong(splits[1]));
  }

  private void testLogDataAndLogIndexEqual(SyncLogDequeSerializer syncLogDequeSerializer) {
    Assert.assertEquals(syncLogDequeSerializer.logDataFileList.size(),
        syncLogDequeSerializer.logIndexFileList.size());

    for (int i = 0; i < syncLogDequeSerializer.logDataFileList.size(); i++) {

      System.out.println("111111=" + syncLogDequeSerializer.logDataFileList.get(i) + ","
          + syncLogDequeSerializer.logDataFileList.get(i).length());
      String[] logDataSplits = syncLogDequeSerializer.logDataFileList.get(i).getName()
          .split(FILE_NAME_SEPARATOR);
      String[] logIndexSplits = syncLogDequeSerializer.logIndexFileList.get(i).getName()
          .split(FILE_NAME_SEPARATOR);

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
  public void testGetLogs() throws IOException {
    List<Log> testLogs1 = TestUtils.prepareNodeLogs(100);
    SyncLogDequeSerializer syncLogDequeSerializer = new SyncLogDequeSerializer(testIdentifier);
    syncLogDequeSerializer.append(testLogs1, 0);
    List<Log> logList = syncLogDequeSerializer.getLogs(0, 10);
    for (int i = 0; i < logList.size(); i++) {
      Assert.assertEquals(testLogs1.get(i), logList.get(i));
    }

    logList = syncLogDequeSerializer.getLogs(10, 10);
    for (int i = 0; i < logList.size(); i++) {
      Assert.assertEquals(testLogs1.get(i + 10), logList.get(i));
    }

    logList = syncLogDequeSerializer.getLogs(0, 99);
    for (int i = 0; i < logList.size(); i++) {
      Assert.assertEquals(testLogs1.get(i), logList.get(i));
    }

    logList = syncLogDequeSerializer.getLogs(99, 99);
    for (int i = 0; i < logList.size(); i++) {
      Assert.assertEquals(testLogs1.get(i + 99), logList.get(i));
    }

    logList = syncLogDequeSerializer.getLogs(100, 101);
    Assert.assertTrue(logList.isEmpty());

    logList = syncLogDequeSerializer.getLogs(100, 100);
    Assert.assertTrue(logList.isEmpty());

    logList = syncLogDequeSerializer.getLogs(1000, 500);
    Assert.assertTrue(logList.isEmpty());
  }

  @Test
  public void testGetLogIndexFile() throws IOException {
    SyncLogDequeSerializer syncLogDequeSerializer = new SyncLogDequeSerializer(testIdentifier);
    prepareFiles(syncLogDequeSerializer);

    Pair<File, Pair<Long, Long>> fileStartAndEndIndex;
    for (int i = 0; i < 9; i++) {
      fileStartAndEndIndex = syncLogDequeSerializer.getLogIndexFile(i);
      Assert
          .assertEquals(syncLogDequeSerializer.logIndexFileList.get(0), fileStartAndEndIndex.left);
      Assert.assertEquals(0L, fileStartAndEndIndex.right.left.longValue());
      Assert.assertEquals(8L, fileStartAndEndIndex.right.right.longValue());
    }

    for (int i = 9; i < 18; i++) {
      fileStartAndEndIndex = syncLogDequeSerializer.getLogIndexFile(i);
      Assert
          .assertEquals(syncLogDequeSerializer.logIndexFileList.get(1), fileStartAndEndIndex.left);
      Assert.assertEquals(9L, fileStartAndEndIndex.right.left.longValue());
      Assert.assertEquals(17L, fileStartAndEndIndex.right.right.longValue());
    }

    for (int i = 36; i < 40; i++) {
      fileStartAndEndIndex = syncLogDequeSerializer.getLogIndexFile(i);
      int lastLogFile = syncLogDequeSerializer.logIndexFileList.size() - 1;
      Assert
          .assertEquals(syncLogDequeSerializer.logIndexFileList.get(lastLogFile),
              fileStartAndEndIndex.left);
      Assert.assertEquals(36L, fileStartAndEndIndex.right.left.longValue());
      Assert.assertEquals(Long.MAX_VALUE, fileStartAndEndIndex.right.right.longValue());
    }
  }


  @Test
  public void testGetLogDataFile() throws IOException {
    SyncLogDequeSerializer syncLogDequeSerializer = new SyncLogDequeSerializer(testIdentifier);
    prepareFiles(syncLogDequeSerializer);

    Pair<File, Pair<Long, Long>> fileStartAndEndIndex;
    for (int i = 0; i < 9; i++) {
      fileStartAndEndIndex = syncLogDequeSerializer.getLogDataFile(i);
      Assert
          .assertEquals(syncLogDequeSerializer.logDataFileList.get(0), fileStartAndEndIndex.left);
      Assert.assertEquals(0L, fileStartAndEndIndex.right.left.longValue());
      Assert.assertEquals(8L, fileStartAndEndIndex.right.right.longValue());
    }

    for (int i = 9; i < 18; i++) {
      fileStartAndEndIndex = syncLogDequeSerializer.getLogDataFile(i);
      Assert
          .assertEquals(syncLogDequeSerializer.logDataFileList.get(1), fileStartAndEndIndex.left);
      Assert.assertEquals(9L, fileStartAndEndIndex.right.left.longValue());
      Assert.assertEquals(17L, fileStartAndEndIndex.right.right.longValue());
    }

    for (int i = 36; i < 40; i++) {
      fileStartAndEndIndex = syncLogDequeSerializer.getLogDataFile(i);
      int lastLogFile = syncLogDequeSerializer.logDataFileList.size() - 1;
      Assert
          .assertEquals(syncLogDequeSerializer.logDataFileList.get(lastLogFile),
              fileStartAndEndIndex.left);
      Assert.assertEquals(36L, fileStartAndEndIndex.right.left.longValue());
      Assert.assertEquals(Long.MAX_VALUE, fileStartAndEndIndex.right.right.longValue());
    }
  }

  //  @Test
//  public void testReadAndWrite() throws IOException {
//    SyncLogDequeSerializer syncLogDequeSerializer = new SyncLogDequeSerializer(testIdentifier);
//    try {
//      List<Log> testLogs1 = TestUtils.prepareNodeLogs(10);
//      syncLogDequeSerializer.append(testLogs1);
//      assertEquals(10, syncLogDequeSerializer.getLogSizeDeque().size());
//
//      List<Log> testLogs2 = TestUtils.prepareNodeLogs(5);
//      syncLogDequeSerializer.append(testLogs2);
//      assertEquals(15, syncLogDequeSerializer.getLogSizeDeque().size());
//
//      int flushRaftLogThreshold = ClusterDescriptor.getInstance().getConfig()
//          .getFlushRaftLogThreshold();
//      List<Log> testLogs3 = TestUtils.prepareNodeLogs(flushRaftLogThreshold);
//      syncLogDequeSerializer.append(testLogs3);
//      assertEquals(15 + flushRaftLogThreshold, syncLogDequeSerializer.getLogSizeDeque().size());
//    } finally {
//      syncLogDequeSerializer.close();
//    }
//  }
//
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
    }
  }

  @Test
  public void testGetOffsetAccordingToLogIndex() throws IOException {
    SyncLogDequeSerializer syncLogDequeSerializer = new SyncLogDequeSerializer(testIdentifier);
    prepareFiles(syncLogDequeSerializer);
    List<File> files = syncLogDequeSerializer.logIndexFileList;
    for (int i = 0; i < files.size(); i++) {
      System.out.println(
          "testGetOffsetAccordingToLogIndex=" + files.get(i).getName() + "," + files.get(i)
              .length());
    }

//    long offset = syncLogDequeSerializer.getOffsetAccordingToLogIndex(9);
//    System.out.println("999999999999=" + offset);
    int currentOffset = 0;
    for (int i = 0; i < testLogs1.size(); i++) {
      long offset = syncLogDequeSerializer.getOffsetAccordingToLogIndex(i);
      System.out.println("999999999999=" + i + "," + offset);
      if (i % 9 == 0) {
        currentOffset = 0;
      }
      Assert.assertEquals(currentOffset, offset);
      currentOffset += 4 + testLogs1.get(i).serialize().capacity();
    }
  }


  @Test
  public void testRecovery() throws IOException {
    SyncLogDequeSerializer syncLogDequeSerializer = new SyncLogDequeSerializer(testIdentifier);
    int logNum = 10;
    int maxHaveAppliedCommitIndex = 7;
    List<Log> testLogs1;
    HardState hardState = new HardState();
    hardState.setCurrentTerm(10);
    hardState.setVoteFor(TestUtils.getNode(5));
    try {

      testLogs1 = TestUtils.prepareNodeLogs(logNum);
      System.out.println("44444=" + testLogs1.size());
      syncLogDequeSerializer.append(testLogs1, maxHaveAppliedCommitIndex);
      syncLogDequeSerializer.setHardStateAndFlush(hardState);
    } finally {
      syncLogDequeSerializer.close();
    }

    // recovery
    syncLogDequeSerializer = new SyncLogDequeSerializer(testIdentifier);
    System.out.println("7777=" + syncLogDequeSerializer.logDataFileList.toString());
    try {
      List<Log> logDeque = syncLogDequeSerializer.getAllEntriesBeforeAppliedIndex();
      int expectSize =
          (int) testLogs1.get(testLogs1.size() - 1).getCurrLogIndex() - maxHaveAppliedCommitIndex
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
  public void testDeleteLogs() throws IOException {
    SyncLogDequeSerializer syncLogDequeSerializer = new SyncLogDequeSerializer(testIdentifier);
    prepareFiles(syncLogDequeSerializer);
    int maxLogFile = 2;
    syncLogDequeSerializer.setMaxNumberOfPersistRaftLogFiles(maxLogFile);
    syncLogDequeSerializer.checkDeletePersistRaftLog();
    Assert.assertEquals(maxLogFile, syncLogDequeSerializer.logDataFileList.size());
    Assert.assertEquals(maxLogFile, syncLogDequeSerializer.logIndexFileList.size());
  }

  @Test
  public void testRecoverFromTemp() throws IOException {
    SyncLogDequeSerializer syncLogDequeSerializer = new SyncLogDequeSerializer(testIdentifier);
    int logNum = 10;
    int maxHaveAppliedCommitIndex = 5;
    List<Log> testLogs1;
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
    String logDir = syncLogDequeSerializer.getLogDir();
    File metaFile = SystemFileFactory.INSTANCE.getFile(logDir + "logMeta");
    File tempMetaFile = SystemFileFactory.INSTANCE.getFile(logDir + "logMeta.tmp");
    metaFile.renameTo(tempMetaFile);
    metaFile.createNewFile();

    // recovery
    syncLogDequeSerializer = new SyncLogDequeSerializer(testIdentifier);
    try {
      List<Log> logDeque = syncLogDequeSerializer.getAllEntriesBeforeAppliedIndex();
      int expectSize =
          (int) testLogs1.get(testLogs1.size() - 1).getCurrLogIndex() - maxHaveAppliedCommitIndex
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
//
//  @Test
//  public void testDeleteLogsByRecovery() throws IOException {
//    SyncLogDequeSerializer syncLogDequeSerializer = new SyncLogDequeSerializer(testIdentifier);
//    List<Log> testLogs1;
//    List<Log> testLogs2;
//    try {
//      syncLogDequeSerializer.setMaxRemovedLogSize(10);
//      testLogs1 = TestUtils.prepareNodeLogs(10);
//      syncLogDequeSerializer.append(testLogs1);
//      assertEquals(10, syncLogDequeSerializer.getLogSizeDeque().size());
//
//      testLogs2 = TestUtils.prepareNodeLogs(5);
//      syncLogDequeSerializer.append(testLogs2);
//      assertEquals(15, syncLogDequeSerializer.getLogSizeDeque().size());
//
//      syncLogDequeSerializer.removeFirst(3);
//    } finally {
//      syncLogDequeSerializer.close();
//    }
//
//    // recovery
//    syncLogDequeSerializer = new SyncLogDequeSerializer(testIdentifier);
//    try {
//      List<Log> logs = syncLogDequeSerializer.recoverLog();
//      assertEquals(12, logs.size());
//
//      for (int i = 0; i < 7; i++) {
//        assertEquals(testLogs1.get(i + 3), logs.get(i));
//      }
//
//      for (int i = 0; i < 5; i++) {
//        assertEquals(testLogs2.get(i), logs.get(i + 7));
//      }
//    } finally {
//      syncLogDequeSerializer.close();
//    }
//  }
//
//  @Test
//  public void testRemoveOldFile() throws IOException {
//    System.out.println("Start testRemoveOldFile()");
//    SyncLogDequeSerializer syncLogDequeSerializer = new SyncLogDequeSerializer(testIdentifier);
//    List<Log> testLogs2;
//    try {
//      syncLogDequeSerializer.setMaxRemovedLogSize(10);
//      List<Log> testLogs1 = TestUtils.prepareNodeLogs(10);
//      syncLogDequeSerializer.append(testLogs1);
//      assertEquals(10, syncLogDequeSerializer.getLogSizeDeque().size());
//
//      syncLogDequeSerializer.removeFirst(3);
//      testLogs2 = TestUtils.prepareNodeLogs(10);
//      syncLogDequeSerializer.append(testLogs2);
//      assertEquals(17, syncLogDequeSerializer.getLogSizeDeque().size());
//      assertEquals(2, syncLogDequeSerializer.logDataFileList.size());
//
//      // this will remove first file and build a new file
//      syncLogDequeSerializer.removeFirst(8);
//
//      assertEquals(9, syncLogDequeSerializer.getLogSizeDeque().size());
//      assertEquals(2, syncLogDequeSerializer.logDataFileList.size());
//    } finally {
//      syncLogDequeSerializer.close();
//    }
//
//    // recovery
//    syncLogDequeSerializer = new SyncLogDequeSerializer(testIdentifier);
//    try {
//      List<Log> logs = syncLogDequeSerializer.recoverLog();
//      assertEquals(9, logs.size());
//
//      for (int i = 0; i < 9; i++) {
//        assertEquals(testLogs2.get(i + 1), logs.get(i));
//      }
//    } finally {
//      syncLogDequeSerializer.close();
//    }
//
//  }
//
//  @Test
//  public void testRemoveOldFileAtRecovery() throws InterruptedException, IOException {
//    System.out.println("Start testRemoveOldFileAtRecovery()");
//    SyncLogDequeSerializer syncLogDequeSerializer = new SyncLogDequeSerializer(testIdentifier);
//    List<Log> testLogs2;
//    try {
//      syncLogDequeSerializer.setMaxRemovedLogSize(10);
//      List<Log> testLogs1 = TestUtils.prepareNodeLogs(10);
//      syncLogDequeSerializer.append(testLogs1);
//      assertEquals(10, syncLogDequeSerializer.getLogSizeDeque().size());
//
//      syncLogDequeSerializer.removeFirst(3);
//      testLogs2 = TestUtils.prepareNodeLogs(10);
//      syncLogDequeSerializer.append(testLogs2);
//      assertEquals(17, syncLogDequeSerializer.getLogSizeDeque().size());
//
//      syncLogDequeSerializer.setMaxRemovedLogSize(10000000);
//      assertEquals(2, syncLogDequeSerializer.logDataFileList.size());
//
//      // this will not remove first file and build a new file
//      syncLogDequeSerializer.removeFirst(8);
//      assertEquals(9, syncLogDequeSerializer.getLogSizeDeque().size());
//      assertEquals(2, syncLogDequeSerializer.logDataFileList.size());
//    } finally {
//      syncLogDequeSerializer.close();
//    }
//
//    // recovery
//    syncLogDequeSerializer = new SyncLogDequeSerializer(testIdentifier);
//    try {
//      List<Log> logs = syncLogDequeSerializer.recoverLog();
//      assertEquals(9, logs.size());
//
//      for (int i = 0; i < 9; i++) {
//        assertEquals(testLogs2.get(i + 1), logs.get(i));
//      }
//    } finally {
//      syncLogDequeSerializer.close();
//    }
//  }
//
//  @Test
//  public void testRecoveryByAppendList() throws IOException {
//    SyncLogDequeSerializer syncLogDequeSerializer = new SyncLogDequeSerializer(testIdentifier);
//    int logNum;
//    List<Log> testLogs1;
//    try {
//      logNum = 10;
//      testLogs1 = TestUtils.prepareNodeLogs(logNum);
//      syncLogDequeSerializer.append(testLogs1);
//      assertEquals(logNum, syncLogDequeSerializer.getLogSizeDeque().size());
//    } finally {
//      syncLogDequeSerializer.close();
//    }
//
//    // recovery
//    syncLogDequeSerializer = new SyncLogDequeSerializer(testIdentifier);
//    try {
//      List<Log> logDeque = syncLogDequeSerializer.recoverLog();
//      assertEquals(logNum, logDeque.size());
//
//      for (int i = 0; i < logNum; i++) {
//        assertEquals(testLogs1.get(i), logDeque.get(i));
//      }
//    } finally {
//      syncLogDequeSerializer.close();
//    }
//  }
//
//  @Test
//  public void testRecoveryWithTempLog() throws IOException {
//    SyncLogDequeSerializer syncLogDequeSerializer = new SyncLogDequeSerializer(testIdentifier);
//    int logNum;
//    List<Log> testLogs1;
//    try {
//      logNum = 10;
//      testLogs1 = TestUtils.prepareNodeLogs(logNum);
//      syncLogDequeSerializer.append(testLogs1);
//      assertEquals(logNum, syncLogDequeSerializer.getLogSizeDeque().size());
//    } finally {
//      syncLogDequeSerializer.close();
//    }
//
//    // build temp log
//    File tempMetaFile = new File(syncLogDequeSerializer.getLogDir() + "logMeta.tmp");
//    syncLogDequeSerializer.getMetaFile().renameTo(tempMetaFile);
//
//    // recovery
//    syncLogDequeSerializer = new SyncLogDequeSerializer(testIdentifier);
//    try {
//      List<Log> logDeque = syncLogDequeSerializer.recoverLog();
//      assertEquals(logNum, logDeque.size());
//
//      for (int i = 0; i < logNum; i++) {
//        assertEquals(testLogs1.get(i), logDeque.get(i));
//      }
//    } finally {
//      syncLogDequeSerializer.close();
//    }
//  }
//
//  @Test
//  public void testRecoveryWithEmptyTempLog() throws IOException {
//    SyncLogDequeSerializer syncLogDequeSerializer = new SyncLogDequeSerializer(testIdentifier);
//    int logNum;
//    List<Log> testLogs1;
//    try {
//      logNum = 10;
//      testLogs1 = TestUtils.prepareNodeLogs(logNum);
//      syncLogDequeSerializer.append(testLogs1);
//      assertEquals(logNum, syncLogDequeSerializer.getLogSizeDeque().size());
//    } finally {
//      syncLogDequeSerializer.close();
//    }
//
//    // build empty temp log
//    File tempMetaFile = new File(syncLogDequeSerializer.getLogDir() + "logMeta.tmp");
//    try {
//      tempMetaFile.createNewFile();
//    } catch (IOException e) {
//      e.printStackTrace();
//    }
//
//    // recovery
//    syncLogDequeSerializer = new SyncLogDequeSerializer(testIdentifier);
//    try {
//      List<Log> logDeque = syncLogDequeSerializer.recoverLog();
//      assertEquals(logNum, logDeque.size());
//
//      for (int i = 0; i < logNum; i++) {
//        assertEquals(testLogs1.get(i), logDeque.get(i));
//      }
//    } finally {
//      syncLogDequeSerializer.close();
//    }
//  }
//
//  @Test
//  public void testRecoveryWithTempLogWithoutOriginalLog() throws IOException {
//    SyncLogDequeSerializer syncLogDequeSerializer = new SyncLogDequeSerializer(testIdentifier);
//    int logNum;
//    List<Log> testLogs1;
//    try {
//      logNum = 10;
//      testLogs1 = TestUtils.prepareNodeLogs(logNum);
//      syncLogDequeSerializer.append(testLogs1);
//      assertEquals(logNum, syncLogDequeSerializer.getLogSizeDeque().size());
//    } finally {
//      syncLogDequeSerializer.close();
//    }
//
//    // build temp log
//    File tempMetaFile = new File(syncLogDequeSerializer.getLogDir() + "logMeta.tmp");
//    try {
//      Files.copy(syncLogDequeSerializer.getMetaFile().toPath(),
//          tempMetaFile.toPath());
//    } catch (IOException e) {
//      e.printStackTrace();
//    }
//
//    // recovery
//    syncLogDequeSerializer = new SyncLogDequeSerializer(testIdentifier);
//    try {
//      List<Log> logDeque = syncLogDequeSerializer.recoverLog();
//      assertEquals(logNum, logDeque.size());
//
//      for (int i = 0; i < logNum; i++) {
//        assertEquals(testLogs1.get(i), logDeque.get(i));
//      }
//    } finally {
//      syncLogDequeSerializer.close();
//    }
//  }
}