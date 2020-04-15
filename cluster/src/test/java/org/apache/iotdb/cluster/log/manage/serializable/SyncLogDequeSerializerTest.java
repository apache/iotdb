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

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.iotdb.cluster.common.IoTDBTest;
import org.apache.iotdb.cluster.common.TestLogApplier;
import org.apache.iotdb.cluster.common.TestUtils;
import org.apache.iotdb.cluster.log.Log;
import org.apache.iotdb.cluster.log.LogApplier;
import org.apache.iotdb.cluster.log.Snapshot;
import org.apache.iotdb.cluster.log.manage.MemoryLogManager;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.junit.Test;

public class SyncLogDequeSerializerTest extends IoTDBTest {

  private Set<Log> appliedLogs = new HashSet<>();
  private LogApplier logApplier = new TestLogApplier() {
    @Override
    public void apply(Log log) {
      appliedLogs.add(log);
    }
  };

  private MemoryLogManager buildMemoryLogManager() {
    return new MemoryLogManager(logApplier) {
      @Override
      public Snapshot getSnapshot() {
        return null;
      }

      @Override
      public void takeSnapshot() {

      }
    };
  }

  @Test
  public void testReadAndWrite() {
    SyncLogDequeSerializer syncLogDequeSerializer = new SyncLogDequeSerializer(getNode(1));
    try {
      MemoryLogManager memoryLogManager = buildMemoryLogManager();
      List<Log> testLogs1 = TestUtils.prepareNodeLogs(10);

      for (Log log : testLogs1) {
        memoryLogManager.appendLog(log);
        syncLogDequeSerializer.append(log, memoryLogManager.getMeta());
      }

      assertEquals(10, syncLogDequeSerializer.getLogSizeDeque().size());

      List<Log> testLogs2 = TestUtils.prepareNodeLogs(5);

      for (Log log : testLogs2) {
        memoryLogManager.appendLog(log);
        syncLogDequeSerializer.append(log, memoryLogManager.getMeta());
      }

      assertEquals(15, syncLogDequeSerializer.getLogSizeDeque().size());
    } finally {
      syncLogDequeSerializer.close();
    }
  }

  @Test
  public void testRecovery() {
    SyncLogDequeSerializer syncLogDequeSerializer = new SyncLogDequeSerializer(getNode(1));
    MemoryLogManager memoryLogManager;
    int logNum;
    List<Log> testLogs1;
    try {
      memoryLogManager = buildMemoryLogManager();
      logNum = 10;
      testLogs1 = TestUtils.prepareNodeLogs(logNum);

      for (Log log : testLogs1) {
        memoryLogManager.appendLog(log);
        syncLogDequeSerializer.append(log, memoryLogManager.getMeta());
      }

      assertEquals(logNum, syncLogDequeSerializer.getLogSizeDeque().size());
    } finally {
      syncLogDequeSerializer.close();
    }

    // recovery
    syncLogDequeSerializer = new SyncLogDequeSerializer(getNode(1));
    try {
      LogManagerMeta managerMeta = syncLogDequeSerializer.recoverMeta();
      assertEquals(memoryLogManager.getMeta(), managerMeta);

      List<Log> logDeque = syncLogDequeSerializer.recoverLog();
      assertEquals(logNum, logDeque.size());

      for (int i = 0; i < logNum; i++) {
        assertEquals(testLogs1.get(i), logDeque.get(i));
      }
    } finally {
      syncLogDequeSerializer.close();
    }
  }

  @Test
  public void testRecoveryAfterRemoveFirst() {
    SyncLogDequeSerializer syncLogDequeSerializer = new SyncLogDequeSerializer(getNode(1));
    List<Log> testLogs1;
    List<Log> testLogs2;
    try {
      MemoryLogManager memoryLogManager = buildMemoryLogManager();
      testLogs1 = TestUtils.prepareNodeLogs(10);

      for (Log log : testLogs1) {
        memoryLogManager.appendLog(log);
        syncLogDequeSerializer.append(log, memoryLogManager.getMeta());
      }

      assertEquals(10, syncLogDequeSerializer.getLogSizeDeque().size());

      testLogs2 = TestUtils.prepareNodeLogs(5);

      for (Log log : testLogs2) {
        memoryLogManager.appendLog(log);
        syncLogDequeSerializer.append(log, memoryLogManager.getMeta());
      }

      assertEquals(15, syncLogDequeSerializer.getLogSizeDeque().size());

      syncLogDequeSerializer.removeFirst(3);
    } finally {
      syncLogDequeSerializer.close();
    }

    // recovery
    List<Log> logs = syncLogDequeSerializer.recoverLog();
    assertEquals(12, logs.size());

    for (int i = 0; i < 7; i++) {
      assertEquals(testLogs1.get(i + 3), logs.get(i));
    }

    for (int i = 0; i < 5; i++) {
      assertEquals(testLogs2.get(i), logs.get(i + 7));
    }
  }

  @Test
  public void testDeleteLogs() {
    SyncLogDequeSerializer syncLogDequeSerializer = new SyncLogDequeSerializer(getNode(1));
    try {
      MemoryLogManager memoryLogManager = buildMemoryLogManager();
      syncLogDequeSerializer.setMaxRemovedLogSize(10);
      List<Log> testLogs1 = TestUtils.prepareNodeLogs(10);

      for (Log log : testLogs1) {
        memoryLogManager.appendLog(log);
        syncLogDequeSerializer.append(log, memoryLogManager.getMeta());
      }

      assertEquals(10, syncLogDequeSerializer.getLogSizeDeque().size());

      List<Log> testLogs2 = TestUtils.prepareNodeLogs(5);

      for (Log log : testLogs2) {
        memoryLogManager.appendLog(log);
        syncLogDequeSerializer.append(log, memoryLogManager.getMeta());
      }

      syncLogDequeSerializer.removeFirst(3);

      assertEquals(12, syncLogDequeSerializer.getLogSizeDeque().size());
    } finally {
      syncLogDequeSerializer.close();
    }
  }

  @Test
  public void testDeleteLogsByRecovery() {
    SyncLogDequeSerializer syncLogDequeSerializer = new SyncLogDequeSerializer(getNode(1));
    List<Log> testLogs1;
    List<Log> testLogs2;
    try {
      syncLogDequeSerializer.setMaxRemovedLogSize(10);

      MemoryLogManager memoryLogManager = buildMemoryLogManager();
      testLogs1 = TestUtils.prepareNodeLogs(10);

      for (Log log : testLogs1) {
        memoryLogManager.appendLog(log);
        syncLogDequeSerializer.append(log, memoryLogManager.getMeta());
      }

      assertEquals(10, syncLogDequeSerializer.getLogSizeDeque().size());

      testLogs2 = TestUtils.prepareNodeLogs(5);

      for (Log log : testLogs2) {
        memoryLogManager.appendLog(log);
        syncLogDequeSerializer.append(log, memoryLogManager.getMeta());
      }

      assertEquals(15, syncLogDequeSerializer.getLogSizeDeque().size());

      syncLogDequeSerializer.removeFirst(3);
    } finally {
      syncLogDequeSerializer.close();
    }

    // recovery
    syncLogDequeSerializer = new SyncLogDequeSerializer(getNode(1));
    try {
      List<Log> logs = syncLogDequeSerializer.recoverLog();
      assertEquals(12, logs.size());

      for (int i = 0; i < 7; i++) {
        assertEquals(testLogs1.get(i + 3), logs.get(i));
      }

      for (int i = 0; i < 5; i++) {
        assertEquals(testLogs2.get(i), logs.get(i + 7));
      }
    } finally {
      syncLogDequeSerializer.close();
    }
  }

  @Test
  public void testRemoveOldFile() {
    SyncLogDequeSerializer syncLogDequeSerializer = new SyncLogDequeSerializer(getNode(1));
    List<Log> testLogs2;
    try {
      syncLogDequeSerializer.setMaxRemovedLogSize(10);

      MemoryLogManager memoryLogManager = buildMemoryLogManager();
      List<Log> testLogs1 = TestUtils.prepareNodeLogs(10);

      for (Log log : testLogs1) {
        memoryLogManager.appendLog(log);
        syncLogDequeSerializer.append(log, memoryLogManager.getMeta());
      }

      assertEquals(10, syncLogDequeSerializer.getLogSizeDeque().size());

      syncLogDequeSerializer.removeFirst(3);

      testLogs2 = TestUtils.prepareNodeLogs(10);

      for (Log log : testLogs2) {
        memoryLogManager.appendLog(log);
        syncLogDequeSerializer.append(log, memoryLogManager.getMeta());
      }

      assertEquals(17, syncLogDequeSerializer.getLogSizeDeque().size());

      assertEquals(2, syncLogDequeSerializer.logFileList.size());
      // this will remove first file and build a new file
      syncLogDequeSerializer.removeFirst(8);

      assertEquals(9, syncLogDequeSerializer.getLogSizeDeque().size());
      assertEquals(2, syncLogDequeSerializer.logFileList.size());
    } finally {
      syncLogDequeSerializer.close();
    }

    // recovery
    syncLogDequeSerializer = new SyncLogDequeSerializer(getNode(1));
    try {
      List<Log> logs = syncLogDequeSerializer.recoverLog();
      assertEquals(9, logs.size());

      for (int i = 0; i < 9; i++) {
        assertEquals(testLogs2.get(i + 1), logs.get(i));
      }
    } finally {
      syncLogDequeSerializer.close();
    }

  }

  @Test
  public void testRemoveOldFileAtRecovery() {
    SyncLogDequeSerializer syncLogDequeSerializer = new SyncLogDequeSerializer(getNode(1));
    List<Log> testLogs2;
    try {
      syncLogDequeSerializer.setMaxRemovedLogSize(10);

      MemoryLogManager memoryLogManager = buildMemoryLogManager();
      List<Log> testLogs1 = TestUtils.prepareNodeLogs(10);

      for (Log log : testLogs1) {
        memoryLogManager.appendLog(log);
        syncLogDequeSerializer.append(log, memoryLogManager.getMeta());
      }

      assertEquals(10, syncLogDequeSerializer.getLogSizeDeque().size());

      syncLogDequeSerializer.removeFirst(3);

      testLogs2 = TestUtils.prepareNodeLogs(10);

      for (Log log : testLogs2) {
        memoryLogManager.appendLog(log);
        syncLogDequeSerializer.append(log, memoryLogManager.getMeta());
      }

      assertEquals(17, syncLogDequeSerializer.getLogSizeDeque().size());

      syncLogDequeSerializer.setMaxRemovedLogSize(10000000);
      assertEquals(2, syncLogDequeSerializer.logFileList.size());
      // this will not remove first file and build a new file
      syncLogDequeSerializer.removeFirst(8);

      assertEquals(9, syncLogDequeSerializer.getLogSizeDeque().size());
      assertEquals(2, syncLogDequeSerializer.logFileList.size());
    } finally {
      syncLogDequeSerializer.close();
    }

    // recovery
    syncLogDequeSerializer = new SyncLogDequeSerializer(getNode(1));
    try {
      List<Log> logs = syncLogDequeSerializer.recoverLog();
      assertEquals(9, logs.size());

      for (int i = 0; i < 9; i++) {
        assertEquals(testLogs2.get(i + 1), logs.get(i));
      }
    } finally {
      syncLogDequeSerializer.close();
    }
  }


  @Test
  public void testTruncate() {
    SyncLogDequeSerializer syncLogDequeSerializer = new SyncLogDequeSerializer(getNode(1));
    List<Log> testLogs2;
    try {
      syncLogDequeSerializer.setMaxRemovedLogSize(10);

      MemoryLogManager memoryLogManager = buildMemoryLogManager();
      List<Log> testLogs1 = TestUtils.prepareNodeLogs(10);

      for (Log log : testLogs1) {
        memoryLogManager.appendLog(log);
        syncLogDequeSerializer.append(log, memoryLogManager.getMeta());
      }

      assertEquals(10, syncLogDequeSerializer.getLogSizeDeque().size());

      syncLogDequeSerializer.removeFirst(3);

      testLogs2 = TestUtils.prepareNodeLogs(10);

      for (Log log : testLogs2) {
        memoryLogManager.appendLog(log);
        syncLogDequeSerializer.append(log, memoryLogManager.getMeta());
      }

      assertEquals(17, syncLogDequeSerializer.getLogSizeDeque().size());

      assertEquals(2, syncLogDequeSerializer.logFileList.size());
      // this will remove first file and build a new file
      syncLogDequeSerializer.removeFirst(8);

      assertEquals(9, syncLogDequeSerializer.getLogSizeDeque().size());
      assertEquals(2, syncLogDequeSerializer.logFileList.size());

      List<Log> testLogs3 = TestUtils.prepareNodeLogs(10);
      for (Log log : testLogs3) {
        memoryLogManager.appendLog(log);
        syncLogDequeSerializer.append(log, memoryLogManager.getMeta());
      }
      assertEquals(19, syncLogDequeSerializer.getLogSizeDeque().size());
      syncLogDequeSerializer.truncateLog(11, memoryLogManager.getMeta());

      // last file has been truncated
      assertEquals(1, syncLogDequeSerializer.logFileList.size());
    } finally {
      syncLogDequeSerializer.close();
    }

    // recovery
    syncLogDequeSerializer = new SyncLogDequeSerializer(getNode(1));
    try {
      List<Log> logs = syncLogDequeSerializer.recoverLog();
      assertEquals(9, logs.size());

      for (int i = 0; i < 8; i++) {
        assertEquals(testLogs2.get(i + 1), logs.get(i));
      }
    } finally {
      syncLogDequeSerializer.close();
    }
  }


  @Test
  public void testRecoveryByAppendList() {
    SyncLogDequeSerializer syncLogDequeSerializer = new SyncLogDequeSerializer(getNode(1));
    MemoryLogManager memoryLogManager;
    int logNum;
    List<Log> testLogs1;
    try {
      memoryLogManager = buildMemoryLogManager();
      logNum = 10;
      testLogs1 = TestUtils.prepareNodeLogs(logNum);

      for (Log log : testLogs1) {
        memoryLogManager.appendLog(log);
      }

      syncLogDequeSerializer.append(testLogs1, memoryLogManager.getMeta());

      assertEquals(logNum, syncLogDequeSerializer.getLogSizeDeque().size());
    } finally {
      syncLogDequeSerializer.close();
    }

    // recovery
    syncLogDequeSerializer = new SyncLogDequeSerializer(getNode(1));
    try {
      LogManagerMeta managerMeta = syncLogDequeSerializer.recoverMeta();
      assertEquals(memoryLogManager.getMeta(), managerMeta);

      List<Log> logDeque = syncLogDequeSerializer.recoverLog();
      assertEquals(logNum, logDeque.size());

      for (int i = 0; i < logNum; i++) {
        assertEquals(testLogs1.get(i), logDeque.get(i));
      }
    } finally {
      syncLogDequeSerializer.close();
    }
  }

  @Test
  public void testTruncateByAppendList() {
    SyncLogDequeSerializer syncLogDequeSerializer = new SyncLogDequeSerializer(getNode(1));
    List<Log> testLogs2;
    try {
      syncLogDequeSerializer.setMaxRemovedLogSize(10);

      MemoryLogManager memoryLogManager = buildMemoryLogManager();
      List<Log> testLogs1 = TestUtils.prepareNodeLogs(10);

      for (Log log : testLogs1) {
        memoryLogManager.appendLog(log);
      }
      syncLogDequeSerializer.append(testLogs1, memoryLogManager.getMeta());

      assertEquals(10, syncLogDequeSerializer.getLogSizeDeque().size());

      syncLogDequeSerializer.removeFirst(3);

      testLogs2 = TestUtils.prepareNodeLogs(10);

      for (Log log : testLogs2) {
        memoryLogManager.appendLog(log);
      }
      syncLogDequeSerializer.append(testLogs2, memoryLogManager.getMeta());

      assertEquals(17, syncLogDequeSerializer.getLogSizeDeque().size());

      assertEquals(2, syncLogDequeSerializer.logFileList.size());
      // this will remove first file and build a new file
      syncLogDequeSerializer.removeFirst(8);

      assertEquals(9, syncLogDequeSerializer.getLogSizeDeque().size());
      assertEquals(2, syncLogDequeSerializer.logFileList.size());

      List<Log> testLogs3 = TestUtils.prepareNodeLogs(10);
      for (Log log : testLogs3) {
        memoryLogManager.appendLog(log);
        syncLogDequeSerializer.append(log, memoryLogManager.getMeta());
      }
      assertEquals(19, syncLogDequeSerializer.getLogSizeDeque().size());
      syncLogDequeSerializer.truncateLog(11, memoryLogManager.getMeta());

      // last file has been truncated
      assertEquals(1, syncLogDequeSerializer.logFileList.size());
    } finally {
      syncLogDequeSerializer.close();
    }

    // recovery
    syncLogDequeSerializer = new SyncLogDequeSerializer(getNode(1));
    try {
      List<Log> logs = syncLogDequeSerializer.recoverLog();
      assertEquals(9, logs.size());

      for (int i = 0; i < 8; i++) {
        assertEquals(testLogs2.get(i + 1), logs.get(i));
      }
    } finally {
      syncLogDequeSerializer.close();
    }
  }

  @Test
  public void testRecoveryWithTempLog() {
    SyncLogDequeSerializer syncLogDequeSerializer = new SyncLogDequeSerializer(getNode(1));
    MemoryLogManager memoryLogManager;
    int logNum;
    List<Log> testLogs1;
    try {
      memoryLogManager = buildMemoryLogManager();
      logNum = 10;
      testLogs1 = TestUtils.prepareNodeLogs(logNum);

      for (Log log : testLogs1) {
        memoryLogManager.appendLog(log);
        syncLogDequeSerializer.append(log, memoryLogManager.getMeta());
      }

      assertEquals(logNum, syncLogDequeSerializer.getLogSizeDeque().size());
    } finally {
      syncLogDequeSerializer.close();
    }

    // build temp log
    File tempMetaFile = new File(syncLogDequeSerializer.getLogDir() + "logMeta.tmp");
    syncLogDequeSerializer.getMetaFile().renameTo(tempMetaFile);

    // recovery
    syncLogDequeSerializer = new SyncLogDequeSerializer(getNode(1));
    try {
      LogManagerMeta managerMeta = syncLogDequeSerializer.recoverMeta();
      assertEquals(memoryLogManager.getMeta(), managerMeta);

      List<Log> logDeque = syncLogDequeSerializer.recoverLog();
      assertEquals(logNum, logDeque.size());

      for (int i = 0; i < logNum; i++) {
        assertEquals(testLogs1.get(i), logDeque.get(i));
      }
    } finally {
      syncLogDequeSerializer.close();
    }
  }

  @Test
  public void testRecoveryWithEmptyTempLog() {
    SyncLogDequeSerializer syncLogDequeSerializer = new SyncLogDequeSerializer(getNode(1));
    MemoryLogManager memoryLogManager;
    int logNum;
    List<Log> testLogs1;
    try {
      memoryLogManager = buildMemoryLogManager();
      logNum = 10;
      testLogs1 = TestUtils.prepareNodeLogs(logNum);

      for (Log log : testLogs1) {
        memoryLogManager.appendLog(log);
        syncLogDequeSerializer.append(log, memoryLogManager.getMeta());
      }

      assertEquals(logNum, syncLogDequeSerializer.getLogSizeDeque().size());
    } finally {
      syncLogDequeSerializer.close();
    }

    // build empty temp log
    File tempMetaFile = new File(syncLogDequeSerializer.getLogDir() + "logMeta.tmp");
    try {
      tempMetaFile.createNewFile();
    } catch (IOException e) {
      e.printStackTrace();
    }

    // recovery
    syncLogDequeSerializer = new SyncLogDequeSerializer(getNode(1));
    try {
      LogManagerMeta managerMeta = syncLogDequeSerializer.recoverMeta();
      assertEquals(memoryLogManager.getMeta(), managerMeta);

      List<Log> logDeque = syncLogDequeSerializer.recoverLog();
      assertEquals(logNum, logDeque.size());

      for (int i = 0; i < logNum; i++) {
        assertEquals(testLogs1.get(i), logDeque.get(i));
      }
    } finally {
      syncLogDequeSerializer.close();
    }
  }

  @Test
  public void testRecoveryWithTempLogWithoutOriginalLog() {
    SyncLogDequeSerializer syncLogDequeSerializer = new SyncLogDequeSerializer(getNode(1));
    MemoryLogManager memoryLogManager;
    int logNum;
    List<Log> testLogs1;
    try {
      memoryLogManager = buildMemoryLogManager();
      logNum = 10;
      testLogs1 = TestUtils.prepareNodeLogs(logNum);

      for (Log log : testLogs1) {
        memoryLogManager.appendLog(log);
        syncLogDequeSerializer.append(log, memoryLogManager.getMeta());
      }

      assertEquals(logNum, syncLogDequeSerializer.getLogSizeDeque().size());
    } finally {
      syncLogDequeSerializer.close();
    }

    // build temp log
    File tempMetaFile = new File(syncLogDequeSerializer.getLogDir() + "logMeta.tmp");
    try {
      Files.copy(syncLogDequeSerializer.getMetaFile().toPath(),
          tempMetaFile.toPath());
    } catch (IOException e) {
      e.printStackTrace();
    }

    // recovery
    syncLogDequeSerializer = new SyncLogDequeSerializer(getNode(1));
    try {
      LogManagerMeta managerMeta = syncLogDequeSerializer.recoverMeta();
      assertEquals(memoryLogManager.getMeta(), managerMeta);

      List<Log> logDeque = syncLogDequeSerializer.recoverLog();
      assertEquals(logNum, logDeque.size());

      for (int i = 0; i < logNum; i++) {
        assertEquals(testLogs1.get(i), logDeque.get(i));
      }
    } finally {
      syncLogDequeSerializer.close();
    }
  }

  private Node getNode(int i) {
    return new Node("localhost", 30000 + i, i, 40000 + i);
  }
}
