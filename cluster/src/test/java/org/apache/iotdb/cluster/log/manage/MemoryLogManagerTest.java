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

package org.apache.iotdb.cluster.log.manage;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.iotdb.cluster.common.TestLog;
import org.apache.iotdb.cluster.common.TestLogApplier;
import org.apache.iotdb.cluster.common.TestUtils;
import org.apache.iotdb.cluster.log.Log;
import org.apache.iotdb.cluster.log.LogApplier;
import org.apache.iotdb.cluster.log.Snapshot;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.junit.Before;
import org.junit.Test;

public class MemoryLogManagerTest {

  private MemoryLogManager logManager;
  private Set<Log> appliedLogs;
  private LogApplier logApplier = new TestLogApplier() {
    @Override
    public void apply(Log log) {
      appliedLogs.add(log);
    }
  };

  @Before
  public void setUp() {
    appliedLogs = new HashSet<>();
    logManager = new MemoryLogManager(logApplier) {
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
  public void testAppend() {
    List<Log> testLogs = TestUtils.prepareTestLogs(10);
    assertEquals(-1, logManager.getLastLogIndex());
    assertEquals(-1, logManager.getLastLogTerm());
    assertNull(logManager.getLastLog());
    assertFalse(logManager.logValid(5));

    for (Log testLog : testLogs) {
      logManager.appendLog(testLog);
    }
    assertEquals(9, logManager.getLastLogIndex());
    assertEquals(9, logManager.getLastLogTerm());
    assertEquals(testLogs.get(9), logManager.getLastLog());
    assertEquals(testLogs.subList(3, 7), logManager.getLogs(3, 7));
    assertTrue(logManager.logValid(5));
  }

  @Test
  public void testCommit() throws QueryProcessException {
    List<Log> testLogs = TestUtils.prepareTestLogs(10);
    assertEquals(-1, logManager.getCommitLogIndex());
    for (Log testLog : testLogs) {
      logManager.appendLog(testLog);
    }
    assertEquals(-1, logManager.getCommitLogIndex());
    logManager.commitLog(8);
    assertEquals(8, logManager.getCommitLogIndex());
    assertTrue(appliedLogs.containsAll(testLogs.subList(0, 9)));

    logManager.commitLog(9);
    assertEquals(9, logManager.getCommitLogIndex());
    assertTrue(appliedLogs.containsAll(testLogs));
  }

  @Test
  public void testSet() {
    assertEquals(-1, logManager.getLastLogIndex());
    assertEquals(-1, logManager.getLastLogTerm());
    logManager.setLastLogId(9);
    logManager.setLastLogTerm(9);
    assertEquals(9, logManager.getLastLogIndex());
    assertEquals(9, logManager.getLastLogTerm());

    assertSame(logApplier, logManager.getApplier());
    assertEquals(Collections.emptyList(), logManager.getLogs(100, 2000));
    assertEquals(Collections.emptyList(), logManager.getLogs(2000, 100));
  }

  @Test
  public void testRemove() {
    List<Log> testLogs = TestUtils.prepareTestLogs(10);
    for (Log testLog : testLogs) {
      logManager.appendLog(testLog);
    }
    for (int i = 0; i < 3; i++) {
      logManager.removeLastLog();
    }
    assertEquals(testLogs.subList(0, 7), logManager.getLogs(0, 7));
  }

  @Test
  public void testReplace() {
    List<Log> testLogs = TestUtils.prepareTestLogs(10);
    for (Log testLog : testLogs) {
      logManager.appendLog(testLog);
    }

    Log log = new TestLog();
    log.setPreviousLogTerm(8);
    log.setPreviousLogIndex(8);
    log.setCurrLogTerm(100);
    log.setCurrLogIndex(9);
    assertNotEquals(log, logManager.getLastLog());
    logManager.replaceLastLog(log);
    assertEquals(log, logManager.getLastLog());
  }
}