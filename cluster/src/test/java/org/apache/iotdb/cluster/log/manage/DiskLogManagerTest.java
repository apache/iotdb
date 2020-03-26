package org.apache.iotdb.cluster.log.manage;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.iotdb.cluster.common.IoTDBTest;
import org.apache.iotdb.cluster.common.TestLogApplier;
import org.apache.iotdb.cluster.common.TestUtils;
import org.apache.iotdb.cluster.log.Log;
import org.apache.iotdb.cluster.log.LogApplier;
import org.apache.iotdb.cluster.log.Snapshot;
import org.apache.iotdb.db.exception.StartupException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.junit.Before;
import org.junit.Test;

public class DiskLogManagerTest extends IoTDBTest {

  private DiskLogManager logManager;
  private Set<Log> appliedLogs;
  private LogApplier logApplier = new TestLogApplier() {
    @Override
    public void apply(Log log) {
      appliedLogs.add(log);
    }
  };

  @Before
  public void setUp() throws QueryProcessException, StartupException {
    super.setUp();
    appliedLogs = new HashSet<>();
    logManager = buildLogManager();
  }

  private DiskLogManager buildLogManager() {
    return new DiskLogManager(logApplier) {
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
    List<Log> testLogs = TestUtils.prepareNodeLogs(10);
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
    logManager.close();
  }

  @Test
  public void testCommit() throws QueryProcessException {
    List<Log> testLogs = TestUtils.prepareNodeLogs(10);
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

    logManager.commitLog(1);
    assertEquals(9, logManager.getCommitLogIndex());
    assertTrue(appliedLogs.containsAll(testLogs));
    logManager.close();
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
    logManager.close();
  }

}
