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

import org.apache.iotdb.cluster.common.TestLogApplier;
import org.apache.iotdb.cluster.common.TestUtils;
import org.apache.iotdb.cluster.config.ClusterDescriptor;
import org.apache.iotdb.cluster.exception.EntryCompactedException;
import org.apache.iotdb.cluster.exception.GetEntriesWrongParametersException;
import org.apache.iotdb.cluster.exception.LogExecutionException;
import org.apache.iotdb.cluster.exception.TruncateCommittedEntryException;
import org.apache.iotdb.cluster.log.HardState;
import org.apache.iotdb.cluster.log.Log;
import org.apache.iotdb.cluster.log.LogApplier;
import org.apache.iotdb.cluster.log.Snapshot;
import org.apache.iotdb.cluster.log.StableEntryManager;
import org.apache.iotdb.cluster.log.logtypes.EmptyContentLog;
import org.apache.iotdb.cluster.log.manage.serializable.SyncLogDequeSerializer;
import org.apache.iotdb.cluster.log.snapshot.SimpleSnapshot;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class RaftLogManagerTest {

  static class TestRaftLogManager extends RaftLogManager {

    private Snapshot snapshot;

    public TestRaftLogManager(
        StableEntryManager stableEntryManager, LogApplier applier, String name) {
      super(stableEntryManager, applier, name);
    }

    public TestRaftLogManager(
        CommittedEntryManager committedEntryManager,
        StableEntryManager stableEntryManager,
        LogApplier applier) {
      super(committedEntryManager, stableEntryManager, applier);
    }

    @Override
    public Snapshot getSnapshot(long minIndex) {
      return snapshot;
    }

    @Override
    public void takeSnapshot() throws IOException {
      super.takeSnapshot();
      snapshot = new SimpleSnapshot(getLastLogIndex(), getLastLogTerm());
    }
  }

  private Map<Long, Log> appliedLogs;
  private volatile boolean blocked = false;
  private LogApplier logApplier =
      new TestLogApplier() {
        @Override
        public void apply(Log log) {
          if (blocked) {
            return;
          }
          // make sure the log is applied when not blocked
          appliedLogs.put(log.getCurrLogIndex(), log);
          log.setApplied(true);
        }
      };
  private int testIdentifier = 1;

  private boolean prevLogPersistence;

  @Before
  public void setUp() {
    prevLogPersistence = ClusterDescriptor.getInstance().getConfig().isEnableRaftLogPersistence();
    ClusterDescriptor.getInstance().getConfig().setCatchUpTimeoutMS(100);
    ClusterDescriptor.getInstance().getConfig().setEnableRaftLogPersistence(true);
    appliedLogs = new ConcurrentHashMap<>();
    blocked = false;
  }

  @After
  public void tearDown() {
    blocked = false;
    File dir = new File(SyncLogDequeSerializer.getLogDir(testIdentifier));
    for (File file : dir.listFiles()) {
      file.delete();
    }
    dir.delete();
    ClusterDescriptor.getInstance().getConfig().setEnableRaftLogPersistence(prevLogPersistence);
  }

  @Test
  public void testHardState() {
    CommittedEntryManager committedEntryManager =
        new CommittedEntryManager(
            ClusterDescriptor.getInstance().getConfig().getMaxNumOfLogsInMem());
    RaftLogManager instance =
        new TestRaftLogManager(
            committedEntryManager, new SyncLogDequeSerializer(testIdentifier), logApplier);

    try {
      HardState hardState = new HardState();
      hardState.setVoteFor(TestUtils.getNode(10));
      hardState.setCurrentTerm(11);
      assertNotEquals(hardState, instance.getHardState());
      instance.updateHardState(hardState);
      assertEquals(hardState, instance.getHardState());
    } finally {
      instance.close();
    }
  }

  @Test
  public void testBlockedSnapshot() throws LogExecutionException, IOException {
    int catchUpTimeoutMS = ClusterDescriptor.getInstance().getConfig().getCatchUpTimeoutMS();
    ClusterDescriptor.getInstance().getConfig().setCatchUpTimeoutMS(200);

    CommittedEntryManager committedEntryManager =
        new CommittedEntryManager(
            ClusterDescriptor.getInstance().getConfig().getMaxNumOfLogsInMem());
    RaftLogManager instance =
        new TestRaftLogManager(
            committedEntryManager, new SyncLogDequeSerializer(testIdentifier), logApplier);
    try {
      List<Log> logs = TestUtils.prepareTestLogs(100);
      instance.append(logs.subList(0, 50));
      instance.commitTo(49);

      instance.setBlockAppliedCommitIndex(99);
      instance.append(logs.subList(50, 100));
      instance.commitTo(98);

      try {
        // applier is blocked, so this should time out
        instance.takeSnapshot();
        fail("No exception");
      } catch (IOException e) {
        assertEquals("wait all log applied time out", e.getMessage());
      }
      blocked = false;
      instance.commitTo(99);
      // applier is unblocked, BlockAppliedCommitIndex should be soon reached
      ClusterDescriptor.getInstance().getConfig().setCatchUpTimeoutMS(60_000);
      instance.takeSnapshot();
      assertEquals(new SimpleSnapshot(99, 99), instance.getSnapshot());
    } finally {
      instance.close();
      ClusterDescriptor.getInstance().getConfig().setCatchUpTimeoutMS(catchUpTimeoutMS);
    }
  }

  @Test
  public void getTerm() {
    class RaftLogManagerTester {

      public long index;
      public long testTerm;
      public Class throwClass;

      public RaftLogManagerTester(long index, long testTerm, Class throwClass) {
        this.index = index;
        this.testTerm = testTerm;
        this.throwClass = throwClass;
      }
    }
    long offset = 100;
    long num = 100;
    long half = offset + num / 2;
    long last = offset + num;
    CommittedEntryManager committedEntryManager =
        new CommittedEntryManager(
            ClusterDescriptor.getInstance().getConfig().getMaxNumOfLogsInMem());
    committedEntryManager.applyingSnapshot(new SimpleSnapshot(offset, offset));
    RaftLogManager instance =
        new TestRaftLogManager(
            committedEntryManager, new SyncLogDequeSerializer(testIdentifier), logApplier);
    try {
      for (long i = 1; i < num; i++) {
        long index = i;
        instance.append(
            new ArrayList<Log>() {
              {
                add(new EmptyContentLog(offset + index, offset + index));
              }
            });
      }
      List<RaftLogManagerTester> tests =
          new ArrayList<RaftLogManagerTester>() {
            {
              add(new RaftLogManagerTester(offset - 1, -1, null));
              add(new RaftLogManagerTester(offset, offset, null));
              add(new RaftLogManagerTester(half, half, null));
              add(new RaftLogManagerTester(last - 1, last - 1, null));
              add(new RaftLogManagerTester(last, -1, null));
            }
          };
      for (RaftLogManagerTester test : tests) {
        try {
          long term = instance.getTerm(test.index);
          if (test.throwClass != null) {
            fail("The expected exception is not thrown");
          } else {
            assertEquals(test.testTerm, term);
          }
        } catch (Exception e) {
          if (!e.getClass().getName().equals(test.throwClass.getName())) {
            fail("An unexpected exception was thrown.");
          }
        }
      }
    } finally {
      instance.close();
    }
  }

  @Test
  public void getFirstIndex() {
    long offset = 100;
    CommittedEntryManager committedEntryManager =
        new CommittedEntryManager(
            ClusterDescriptor.getInstance().getConfig().getMaxNumOfLogsInMem());
    committedEntryManager.applyingSnapshot(new SimpleSnapshot(offset, offset));
    RaftLogManager instance =
        new TestRaftLogManager(
            committedEntryManager, new SyncLogDequeSerializer(testIdentifier), logApplier);
    try {
      assertEquals(offset + 1, instance.getFirstIndex());
      long newOffset = offset + 20;
      committedEntryManager.applyingSnapshot(new SimpleSnapshot(newOffset, newOffset));
      assertEquals(newOffset + 1, instance.getFirstIndex());
    } finally {
      instance.close();
    }
  }

  @Test
  public void getLastLogIndex() {
    long offset = 100;
    long num = 100;
    CommittedEntryManager committedEntryManager =
        new CommittedEntryManager(
            ClusterDescriptor.getInstance().getConfig().getMaxNumOfLogsInMem());
    committedEntryManager.applyingSnapshot(new SimpleSnapshot(offset, offset));
    RaftLogManager instance =
        new TestRaftLogManager(
            committedEntryManager, new SyncLogDequeSerializer(testIdentifier), logApplier);
    try {
      for (long i = 1; i < num; i++) {
        long index = i;
        instance.append(
            new ArrayList<Log>() {
              {
                add(new EmptyContentLog(offset + index, offset + index));
              }
            });
        assertEquals(offset + index, instance.getLastLogIndex());
      }
    } finally {
      instance.close();
    }
  }

  @Test
  public void getLastLogTerm() {
    long offset = 100;
    long num = 100;
    CommittedEntryManager committedEntryManager =
        new CommittedEntryManager(
            ClusterDescriptor.getInstance().getConfig().getMaxNumOfLogsInMem());
    committedEntryManager.applyingSnapshot(new SimpleSnapshot(offset, offset));
    RaftLogManager instance =
        new TestRaftLogManager(
            committedEntryManager, new SyncLogDequeSerializer(testIdentifier), logApplier);
    try {
      for (long i = 1; i < num; i++) {
        long index = i;
        instance.append(
            new ArrayList<Log>() {
              {
                add(new EmptyContentLog(offset + index, offset + index));
              }
            });
        assertEquals(offset + index, instance.getLastLogTerm());
      }
    } finally {
      instance.close();
    }
  }

  @Test
  public void maybeCommit() {
    class RaftLogManagerTester {

      public long leaderCommit;
      public long term;
      public long testCommittedEntryManagerSize;
      public long testUnCommittedEntryManagerSize;
      public long testCommitIndex;
      public boolean testCommit;

      public RaftLogManagerTester(
          long leaderCommit,
          long term,
          long testCommittedEntryManagerSize,
          long testUnCommittedEntryManagerSize,
          long testCommitIndex,
          boolean testCommit) {
        this.leaderCommit = leaderCommit;
        this.term = term;
        this.testCommittedEntryManagerSize = testCommittedEntryManagerSize;
        this.testUnCommittedEntryManagerSize = testUnCommittedEntryManagerSize;
        this.testCommitIndex = testCommitIndex;
        this.testCommit = testCommit;
      }
    }
    long offset = 100;
    long num = 100;
    long half = offset + num / 2;
    long last = offset + num;
    CommittedEntryManager committedEntryManager =
        new CommittedEntryManager(
            ClusterDescriptor.getInstance().getConfig().getMaxNumOfLogsInMem());
    committedEntryManager.applyingSnapshot(new SimpleSnapshot(offset, offset));
    for (long i = 1; i < num / 2; i++) {
      long index = i;
      try {
        committedEntryManager.append(
            new ArrayList<Log>() {
              {
                add(new EmptyContentLog(offset + index, offset + index));
              }
            });
      } catch (Exception e) {
      }
    }
    RaftLogManager instance =
        new TestRaftLogManager(
            committedEntryManager, new SyncLogDequeSerializer(testIdentifier), logApplier);
    try {
      for (long i = num / 2; i < num; i++) {
        long index = i;
        instance.append(
            new ArrayList<Log>() {
              {
                add(new EmptyContentLog(offset + index, offset + index));
              }
            });
      }
      List<RaftLogManagerTester> tests =
          new ArrayList<RaftLogManagerTester>() {
            {
              // term small leaderCommit
              add(
                  new RaftLogManagerTester(
                      offset - 10, offset - 9, num / 2, num / 2, half - 1, false));
              add(
                  new RaftLogManagerTester(
                      offset - 10, offset - 10, num / 2, num / 2, half - 1, false));
              add(new RaftLogManagerTester(half - 1, half - 1, num / 2, num / 2, half - 1, false));
              // normal case
              add(new RaftLogManagerTester(half, half + 1, num / 2, num / 2, half - 1, false));
              add(new RaftLogManagerTester(half, half, num / 2 + 1, num / 2 - 1, half, true));
              add(new RaftLogManagerTester(last - 1, last - 1, num, 0, last - 1, true));
              // test large leaderCommit
              add(new RaftLogManagerTester(last, last, num, 0, last - 1, false));
            }
          };
      for (RaftLogManagerTester test : tests) {
        boolean answer = instance.maybeCommit(test.leaderCommit, test.term);
        assertEquals(
            test.testCommittedEntryManagerSize,
            instance.getCommittedEntryManager().getAllEntries().size());
        assertEquals(
            test.testUnCommittedEntryManagerSize,
            instance.getUnCommittedEntryManager().getAllEntries().size());
        assertEquals(test.testCommitIndex, instance.getCommitLogIndex());
        assertEquals(test.testCommit, answer);
      }
    } finally {
      instance.close();
    }
  }

  @Test
  public void commitTo() throws Exception {
    class RaftLogManagerTester {

      public long commitTo;
      public long testCommittedEntryManagerSize;
      public long testUnCommittedEntryManagerSize;
      public long testCommitIndex;

      public RaftLogManagerTester(
          long commitTo,
          long testCommittedEntryManagerSize,
          long testUnCommittedEntryManagerSize,
          long testCommitIndex) {
        this.commitTo = commitTo;
        this.testCommittedEntryManagerSize = testCommittedEntryManagerSize;
        this.testUnCommittedEntryManagerSize = testUnCommittedEntryManagerSize;
        this.testCommitIndex = testCommitIndex;
      }
    }
    long offset = 100;
    long num = 100;
    long half = offset + num / 2;
    long last = offset + num;
    CommittedEntryManager committedEntryManager =
        new CommittedEntryManager(
            ClusterDescriptor.getInstance().getConfig().getMaxNumOfLogsInMem());
    committedEntryManager.applyingSnapshot(new SimpleSnapshot(offset, offset));
    for (long i = 1; i < num / 2; i++) {
      long index = i;
      try {
        committedEntryManager.append(
            new ArrayList<Log>() {
              {
                add(new EmptyContentLog(offset + index, offset + index));
              }
            });
      } catch (Exception e) {
      }
    }
    RaftLogManager instance =
        new TestRaftLogManager(
            committedEntryManager, new SyncLogDequeSerializer(testIdentifier), logApplier);
    try {
      for (long i = num / 2; i < num; i++) {
        long index = i;
        instance.append(
            new ArrayList<Log>() {
              {
                add(new EmptyContentLog(offset + index, offset + index));
              }
            });
      }
      List<RaftLogManagerTester> tests =
          new ArrayList<RaftLogManagerTester>() {
            {
              add(new RaftLogManagerTester(offset - 10, num / 2, num / 2, half - 1));
              add(new RaftLogManagerTester(half - 1, num / 2, num / 2, half - 1));
              add(new RaftLogManagerTester(half, num / 2 + 1, num / 2 - 1, half));
              add(new RaftLogManagerTester(half + 10, num / 2 + 11, num / 2 - 11, half + 10));
              add(new RaftLogManagerTester(last - 1, num, 0, last - 1));
            }
          };
      for (RaftLogManagerTester test : tests) {
        instance.commitTo(test.commitTo);
        assertEquals(
            test.testCommittedEntryManagerSize,
            instance.getCommittedEntryManager().getAllEntries().size());
        assertEquals(
            test.testUnCommittedEntryManagerSize,
            instance.getUnCommittedEntryManager().getAllEntries().size());
        assertEquals(test.testCommitIndex, instance.getCommitLogIndex());
      }
    } finally {
      instance.close();
    }
  }

  @Test
  public void applyEntries() {
    List<Log> testLogs = TestUtils.prepareTestLogs(10);
    RaftLogManager instance =
        new TestRaftLogManager(
            new CommittedEntryManager(
                ClusterDescriptor.getInstance().getConfig().getMaxNumOfLogsInMem()),
            new SyncLogDequeSerializer(testIdentifier),
            logApplier);
    try {
      instance.applyEntries(testLogs);
      for (Log log : testLogs) {
        while (!log.isApplied()) {
          // wait
        }
        assertTrue(appliedLogs.containsKey(log.getCurrLogIndex()));
        assertEquals(log, appliedLogs.get(log.getCurrLogIndex()));
      }
    } finally {
      instance.close();
    }
  }

  @Test
  public void matchTerm() {
    class RaftLogManagerTester {

      public long index;
      public long term;
      public boolean testMatch;

      public RaftLogManagerTester(long index, long term, boolean testMatch) {
        this.index = index;
        this.term = term;
        this.testMatch = testMatch;
      }
    }
    long offset = 100;
    long num = 100;
    long half = offset + num / 2;
    long last = offset + num;
    CommittedEntryManager committedEntryManager =
        new CommittedEntryManager(
            ClusterDescriptor.getInstance().getConfig().getMaxNumOfLogsInMem());
    committedEntryManager.applyingSnapshot(new SimpleSnapshot(offset, offset));
    for (long i = 1; i < num / 2; i++) {
      long index = i;
      try {
        committedEntryManager.append(
            new ArrayList<Log>() {
              {
                add(new EmptyContentLog(offset + index, offset + index));
              }
            });
      } catch (Exception e) {
      }
    }
    RaftLogManager instance =
        new TestRaftLogManager(
            committedEntryManager, new SyncLogDequeSerializer(testIdentifier), logApplier);
    try {
      for (long i = num / 2; i < num; i++) {
        long index = i;
        instance.append(
            new ArrayList<Log>() {
              {
                add(new EmptyContentLog(offset + index, offset + index));
              }
            });
      }
      List<RaftLogManagerTester> tests =
          new ArrayList<RaftLogManagerTester>() {
            {
              add(new RaftLogManagerTester(offset - 1, offset - 1, false));
              add(new RaftLogManagerTester(offset, offset - 1, false));
              add(new RaftLogManagerTester(offset, offset, true));
              add(new RaftLogManagerTester(half, half, true));
              add(new RaftLogManagerTester(half + 1, half, false));
              add(new RaftLogManagerTester(last - 1, last - 1, true));
              add(new RaftLogManagerTester(last, last, false));
            }
          };
      for (RaftLogManagerTester test : tests) {
        assertEquals(test.testMatch, instance.matchTerm(test.index, test.term));
      }
    } finally {
      instance.close();
    }
  }

  @Test
  public void maybeAppendBatch() throws Exception {
    class RaftLogManagerTester {

      public List<Log> entries;
      public long lastIndex;
      public long lastTerm;
      public long leaderCommit;
      public long testLastIndex;
      public long testCommitIndex;
      public boolean testAppend;

      public RaftLogManagerTester(
          List<Log> entries,
          long lastIndex,
          long lastTerm,
          long leaderCommit,
          long testLastIndex,
          long testCommitIndex,
          boolean testAppend) {
        this.entries = entries;
        this.lastIndex = lastIndex;
        this.lastTerm = lastTerm;
        this.leaderCommit = leaderCommit;
        this.testLastIndex = testLastIndex;
        this.testCommitIndex = testCommitIndex;
        this.testAppend = testAppend;
      }
    }
    List<Log> previousEntries =
        new ArrayList<Log>() {
          {
            add(new EmptyContentLog(1, 1));
            add(new EmptyContentLog(2, 2));
            add(new EmptyContentLog(3, 3));
          }
        };
    long lastIndex = 3;
    long lastTerm = 3;
    long commit = 1;
    List<RaftLogManagerTester> tests =
        new ArrayList<RaftLogManagerTester>() {
          {
            // not match: term is different
            add(
                new RaftLogManagerTester(
                    new ArrayList<>(), lastIndex, lastTerm - 1, lastIndex, -1, commit, false));
            // not match: index out of bound
            add(
                new RaftLogManagerTester(
                    new ArrayList<>(), lastIndex + 1, lastTerm, lastIndex, -1, commit, false));
            // match with the last existing entry
            add(
                new RaftLogManagerTester(
                    new ArrayList<>(), lastIndex, lastTerm, lastIndex, lastIndex, lastIndex, true));
            // do not increase commit higher than newLastIndex
            add(
                new RaftLogManagerTester(
                    new ArrayList<>(),
                    lastIndex,
                    lastTerm,
                    lastIndex + 1,
                    lastIndex,
                    lastIndex,
                    true));
            // commit up to the commit in the message
            add(
                new RaftLogManagerTester(
                    new ArrayList<>(),
                    lastIndex,
                    lastTerm,
                    lastIndex - 1,
                    lastIndex,
                    lastIndex - 1,
                    true));
            // commit do not decrease
            add(
                new RaftLogManagerTester(
                    new ArrayList<>(), lastIndex, lastTerm, 0, lastIndex, commit, true));
            // normal case
            add(
                new RaftLogManagerTester(
                    new ArrayList<Log>() {
                      {
                        add(new EmptyContentLog(lastIndex + 1, 4));
                      }
                    },
                    lastIndex,
                    lastTerm,
                    lastIndex,
                    lastIndex + 1,
                    lastIndex,
                    true));
            add(
                new RaftLogManagerTester(
                    new ArrayList<Log>() {
                      {
                        add(new EmptyContentLog(lastIndex + 1, 4));
                      }
                    },
                    lastIndex,
                    lastTerm,
                    lastIndex + 1,
                    lastIndex + 1,
                    lastIndex + 1,
                    true));
            add(
                new RaftLogManagerTester(
                    new ArrayList<Log>() {
                      {
                        add(new EmptyContentLog(lastIndex + 1, 4));
                        add(new EmptyContentLog(lastIndex + 2, 4));
                      }
                    },
                    lastIndex,
                    lastTerm,
                    lastIndex + 2,
                    lastIndex + 2,
                    lastIndex + 2,
                    true));
            // do not increase commit higher than newLastIndex
            add(
                new RaftLogManagerTester(
                    new ArrayList<Log>() {
                      {
                        add(new EmptyContentLog(lastIndex + 1, 4));
                      }
                    },
                    lastIndex,
                    lastTerm,
                    lastIndex + 2,
                    lastIndex + 1,
                    lastIndex + 1,
                    true));
            // match with the the entry in the middle
            add(
                new RaftLogManagerTester(
                    new ArrayList<Log>() {
                      {
                        add(new EmptyContentLog(lastIndex, 4));
                      }
                    },
                    lastIndex - 1,
                    lastTerm - 1,
                    lastIndex,
                    lastIndex,
                    lastIndex,
                    true));
            add(
                new RaftLogManagerTester(
                    new ArrayList<Log>() {
                      {
                        add(new EmptyContentLog(lastIndex - 1, 4));
                      }
                    },
                    lastIndex - 2,
                    lastTerm - 2,
                    lastIndex,
                    lastIndex - 1,
                    lastIndex - 1,
                    true));
            add(
                new RaftLogManagerTester(
                    new ArrayList<Log>() {
                      {
                        add(new EmptyContentLog(lastIndex - 1, 4));
                        add(new EmptyContentLog(lastIndex, 4));
                      }
                    },
                    lastIndex - 2,
                    lastTerm - 2,
                    lastIndex,
                    lastIndex,
                    lastIndex,
                    true));
          }
        };
    for (RaftLogManagerTester test : tests) {
      CommittedEntryManager committedEntryManager =
          new CommittedEntryManager(
              ClusterDescriptor.getInstance().getConfig().getMaxNumOfLogsInMem());
      committedEntryManager.applyingSnapshot(new SimpleSnapshot(0, 0));
      RaftLogManager instance =
          new TestRaftLogManager(
              committedEntryManager, new SyncLogDequeSerializer(testIdentifier), logApplier);
      try {
        instance.append(previousEntries);
        instance.commitTo(commit);
        assertEquals(
            test.testLastIndex,
            instance.maybeAppend(test.lastIndex, test.lastTerm, test.leaderCommit, test.entries));
        assertEquals(test.testCommitIndex, instance.getCommitLogIndex());
        if (test.testAppend) {
          try {
            List<Log> entries =
                instance.getEntries(
                    instance.getLastLogIndex() - test.entries.size() + 1, Integer.MAX_VALUE);
            assertEquals(test.entries, entries);
          } catch (Exception e) {
            fail("An unexpected exception was thrown.");
          }
        }
      } finally {
        instance.close();
      }
    }
  }

  @Test
  public void maybeAppendSingle() throws Exception {
    class RaftLogManagerTester {

      public Log entry;
      public long lastIndex;
      public long lastTerm;
      public long leaderCommit;
      public long testLastIndex;
      public long testCommitIndex;
      public boolean testAppend;

      public RaftLogManagerTester(
          Log entry,
          long lastIndex,
          long lastTerm,
          long leaderCommit,
          long testLastIndex,
          long testCommitIndex,
          boolean testAppend) {
        this.entry = entry;
        this.lastIndex = lastIndex;
        this.lastTerm = lastTerm;
        this.leaderCommit = leaderCommit;
        this.testLastIndex = testLastIndex;
        this.testCommitIndex = testCommitIndex;
        this.testAppend = testAppend;
      }
    }
    List<Log> previousEntries =
        new ArrayList<Log>() {
          {
            add(new EmptyContentLog(1, 1));
            add(new EmptyContentLog(2, 2));
            add(new EmptyContentLog(3, 3));
          }
        };
    long lastIndex = 3;
    long lastTerm = 3;
    long commit = 1;
    List<RaftLogManagerTester> tests =
        new ArrayList<RaftLogManagerTester>() {
          {
            // not match: term is different
            add(
                new RaftLogManagerTester(
                    null, lastIndex, lastTerm - 1, lastIndex, -1, commit, false));
            // not match: index out of bound
            add(
                new RaftLogManagerTester(
                    null, lastIndex + 1, lastTerm, lastIndex, -1, commit, false));
            // normal case
            add(
                new RaftLogManagerTester(
                    new EmptyContentLog(lastIndex + 1, 4),
                    lastIndex,
                    lastTerm,
                    lastIndex,
                    lastIndex + 1,
                    lastIndex,
                    true));
            add(
                new RaftLogManagerTester(
                    new EmptyContentLog(lastIndex + 1, 4),
                    lastIndex,
                    lastTerm,
                    lastIndex + 1,
                    lastIndex + 1,
                    lastIndex + 1,
                    true));
            // do not increase commit higher than newLastIndex
            add(
                new RaftLogManagerTester(
                    new EmptyContentLog(lastIndex + 1, 4),
                    lastIndex,
                    lastTerm,
                    lastIndex + 2,
                    lastIndex + 1,
                    lastIndex + 1,
                    true));
            // match with the the entry in the middle
            add(
                new RaftLogManagerTester(
                    new EmptyContentLog(lastIndex, 4),
                    lastIndex - 1,
                    lastTerm - 1,
                    lastIndex,
                    lastIndex,
                    lastIndex,
                    true));
            add(
                new RaftLogManagerTester(
                    new EmptyContentLog(lastIndex - 1, 4),
                    lastIndex - 2,
                    lastTerm - 2,
                    lastIndex,
                    lastIndex - 1,
                    lastIndex - 1,
                    true));
          }
        };
    for (RaftLogManagerTester test : tests) {
      CommittedEntryManager committedEntryManager =
          new CommittedEntryManager(
              ClusterDescriptor.getInstance().getConfig().getMaxNumOfLogsInMem());
      committedEntryManager.applyingSnapshot(new SimpleSnapshot(0, 0));
      RaftLogManager instance =
          new TestRaftLogManager(
              committedEntryManager, new SyncLogDequeSerializer(testIdentifier), logApplier);
      try {
        instance.append(previousEntries);
        instance.commitTo(commit);
        assertEquals(
            test.testLastIndex,
            instance.maybeAppend(test.lastIndex, test.lastTerm, test.leaderCommit, test.entry));
        assertEquals(test.testCommitIndex, instance.getCommitLogIndex());
        if (test.testAppend) {
          assertTrue(instance.matchTerm(test.entry.getCurrLogTerm(), test.entry.getCurrLogIndex()));
        }
      } finally {
        instance.close();
      }
    }
  }

  @Test
  public void testAppendCommitted() throws LogExecutionException {
    CommittedEntryManager committedEntryManager =
        new CommittedEntryManager(
            ClusterDescriptor.getInstance().getConfig().getMaxNumOfLogsInMem());
    RaftLogManager instance =
        new TestRaftLogManager(
            committedEntryManager, new SyncLogDequeSerializer(testIdentifier), logApplier);

    try {
      List<Log> logs = TestUtils.prepareTestLogs(10);
      instance.append(logs);
      instance.commitTo(9);

      // committed logs cannot be overwrite
      Log log = new EmptyContentLog(9, 10000);
      instance.maybeAppend(8, 8, 9, log);
      assertNotEquals(log, instance.getEntries(9, 10).get(0));
      instance.maybeAppend(8, 8, 9, Collections.singletonList(log));
      assertNotEquals(log, instance.getEntries(9, 10).get(0));
    } finally {
      instance.close();
    }
  }

  @Test
  public void testInnerDeleteLogs() {
    int minNumOfLogsInMem = ClusterDescriptor.getInstance().getConfig().getMinNumOfLogsInMem();
    int maxNumOfLogsInMem = ClusterDescriptor.getInstance().getConfig().getMaxNumOfLogsInMem();
    ClusterDescriptor.getInstance().getConfig().setMaxNumOfLogsInMem(10);
    ClusterDescriptor.getInstance().getConfig().setMinNumOfLogsInMem(10);
    CommittedEntryManager committedEntryManager =
        new CommittedEntryManager(
            ClusterDescriptor.getInstance().getConfig().getMaxNumOfLogsInMem());
    RaftLogManager instance =
        new TestRaftLogManager(
            committedEntryManager, new SyncLogDequeSerializer(testIdentifier), logApplier);
    List<Log> logs = TestUtils.prepareTestLogs(20);

    try {
      instance.append(logs.subList(0, 15));
      instance.maybeCommit(14, 14);
      while (instance.getMaxHaveAppliedCommitIndex() < 14) {
        // wait
      }
      instance.append(logs.subList(15, 20));
      instance.maybeCommit(19, 19);

      List<Log> entries = instance.getEntries(0, 20);
      assertEquals(logs.subList(10, 20), entries);
    } finally {
      instance.close();
      ClusterDescriptor.getInstance().getConfig().setMaxNumOfLogsInMem(maxNumOfLogsInMem);
      ClusterDescriptor.getInstance().getConfig().setMinNumOfLogsInMem(minNumOfLogsInMem);
    }
  }

  @Test
  public void testInnerDeleteLogsWithLargeLog() {
    long maxMemSize = ClusterDescriptor.getInstance().getConfig().getMaxMemorySizeForRaftLog();
    int minNumOfLogsInMem = ClusterDescriptor.getInstance().getConfig().getMinNumOfLogsInMem();
    int maxNumOfLogsInMem = ClusterDescriptor.getInstance().getConfig().getMaxNumOfLogsInMem();
    ClusterDescriptor.getInstance().getConfig().setMaxNumOfLogsInMem(10);
    ClusterDescriptor.getInstance().getConfig().setMinNumOfLogsInMem(10);
    ClusterDescriptor.getInstance().getConfig().setMaxMemorySizeForRaftLog(1024 * 56);
    CommittedEntryManager committedEntryManager =
        new CommittedEntryManager(
            ClusterDescriptor.getInstance().getConfig().getMaxNumOfLogsInMem());
    RaftLogManager instance =
        new TestRaftLogManager(
            committedEntryManager, new SyncLogDequeSerializer(testIdentifier), logApplier);
    List<Log> logs = TestUtils.prepareLargeTestLogs(12);

    try {
      instance.append(logs.subList(0, 7));
      instance.maybeCommit(6, 6);
      while (instance.getMaxHaveAppliedCommitIndex() < 6) {
        // wait
      }
      instance.append(logs.subList(7, 12));
      instance.maybeCommit(11, 11);

      List<Log> entries = instance.getEntries(0, 12);
      assertEquals(logs.subList(5, 12), entries);
    } finally {
      instance.close();
      ClusterDescriptor.getInstance().getConfig().setMaxNumOfLogsInMem(maxNumOfLogsInMem);
      ClusterDescriptor.getInstance().getConfig().setMinNumOfLogsInMem(minNumOfLogsInMem);
      ClusterDescriptor.getInstance().getConfig().setMaxMemorySizeForRaftLog(maxMemSize);
    }
  }

  @Test
  @SuppressWarnings("java:S2925")
  public void testReapplyBlockedLogs() throws LogExecutionException, InterruptedException {
    CommittedEntryManager committedEntryManager =
        new CommittedEntryManager(
            ClusterDescriptor.getInstance().getConfig().getMaxNumOfLogsInMem());
    RaftLogManager instance =
        new TestRaftLogManager(
            committedEntryManager, new SyncLogDequeSerializer(testIdentifier), logApplier);
    List<Log> logs = TestUtils.prepareTestLogs(20);

    try {
      instance.append(logs.subList(0, 10));
      instance.commitTo(10);
      instance.setBlockAppliedCommitIndex(9);

      blocked = false;
      // as [0, 10) are blocked and we require a block index of 9, [10, 20) should be added to the
      // blocked list
      instance.append(logs.subList(10, 20));
      instance.commitTo(20);
      while (instance.getMaxHaveAppliedCommitIndex() < 9) {
        // wait until [0, 10) are applied
      }
      Thread.sleep(200);
      // [10, 20) can still not be applied because we do not call `resetBlockAppliedCommitIndex`
      for (Log log : logs.subList(10, 20)) {
        assertFalse(log.isApplied());
      }
      // [10, 20) can be applied now
      instance.resetBlockAppliedCommitIndex();
      while (instance.getMaxHaveAppliedCommitIndex() < 19) {
        // wait until [10, 20) are applied
      }
    } finally {
      instance.close();
    }
  }

  @Test
  public void appendBatch() {
    class RaftLogManagerTester {

      public List<Log> appendingEntries;
      public List<Log> testEntries;
      public long testLastIndexAfterAppend;
      public long testOffset;

      public RaftLogManagerTester(
          List<Log> appendingEntries,
          List<Log> testEntries,
          long testLastIndexAfterAppend,
          long testOffset) {
        this.appendingEntries = appendingEntries;
        this.testEntries = testEntries;
        this.testLastIndexAfterAppend = testLastIndexAfterAppend;
        this.testOffset = testOffset;
      }
    }
    List<Log> previousEntries =
        new ArrayList<Log>() {
          {
            add(new EmptyContentLog(1, 1));
            add(new EmptyContentLog(2, 2));
          }
        };
    List<RaftLogManagerTester> tests =
        new ArrayList<RaftLogManagerTester>() {
          {
            add(
                new RaftLogManagerTester(
                    new ArrayList<>(),
                    new ArrayList<Log>() {
                      {
                        add(new EmptyContentLog(1, 1));
                        add(new EmptyContentLog(2, 2));
                      }
                    },
                    2,
                    3));
            add(
                new RaftLogManagerTester(
                    new ArrayList<Log>() {
                      {
                        add(new EmptyContentLog(3, 2));
                      }
                    },
                    new ArrayList<Log>() {
                      {
                        add(new EmptyContentLog(1, 1));
                        add(new EmptyContentLog(2, 2));
                        add(new EmptyContentLog(3, 2));
                      }
                    },
                    3,
                    3));
            // conflicts with index 1
            add(
                new RaftLogManagerTester(
                    new ArrayList<Log>() {
                      {
                        add(new EmptyContentLog(1, 2));
                      }
                    },
                    new ArrayList<Log>() {
                      {
                        add(new EmptyContentLog(1, 1));
                        add(new EmptyContentLog(2, 2));
                      }
                    },
                    2,
                    3));
            add(
                new RaftLogManagerTester(
                    new ArrayList<Log>() {
                      {
                        add(new EmptyContentLog(2, 3));
                        add(new EmptyContentLog(3, 3));
                      }
                    },
                    new ArrayList<Log>() {
                      {
                        add(new EmptyContentLog(1, 1));
                        add(new EmptyContentLog(2, 2));
                      }
                    },
                    2,
                    3));
          }
        };
    for (RaftLogManagerTester test : tests) {
      CommittedEntryManager committedEntryManager =
          new CommittedEntryManager(
              ClusterDescriptor.getInstance().getConfig().getMaxNumOfLogsInMem());
      committedEntryManager.applyingSnapshot(new SimpleSnapshot(0, 0));
      try {
        committedEntryManager.append(previousEntries);
      } catch (Exception e) {
      }
      RaftLogManager instance =
          new TestRaftLogManager(
              committedEntryManager, new SyncLogDequeSerializer(testIdentifier), logApplier);
      instance.append(test.appendingEntries);
      try {
        List<Log> entries = instance.getEntries(1, Integer.MAX_VALUE);
        assertEquals(test.testEntries, entries);
        assertEquals(
            test.testOffset, instance.getUnCommittedEntryManager().getFirstUnCommittedIndex());
      } catch (Exception e) {
        fail("An unexpected exception was thrown.");
      } finally {
        instance.close();
      }
    }
  }

  @Test
  public void appendSingle() {
    class RaftLogManagerTester {

      public Log appendingEntry;
      public long testLastIndexAfterAppend;
      public List<Log> testEntries;
      public long testOffset;

      public RaftLogManagerTester(
          Log appendingEntry,
          List<Log> testEntries,
          long testLastIndexAfterAppend,
          long testOffset) {
        this.appendingEntry = appendingEntry;
        this.testEntries = testEntries;
        this.testLastIndexAfterAppend = testLastIndexAfterAppend;
        this.testOffset = testOffset;
      }
    }
    List<Log> previousEntries =
        new ArrayList<Log>() {
          {
            add(new EmptyContentLog(1, 1));
            add(new EmptyContentLog(2, 2));
          }
        };
    List<RaftLogManagerTester> tests =
        new ArrayList<RaftLogManagerTester>() {
          {
            add(
                new RaftLogManagerTester(
                    new EmptyContentLog(3, 2),
                    new ArrayList<Log>() {
                      {
                        add(new EmptyContentLog(1, 1));
                        add(new EmptyContentLog(2, 2));
                        add(new EmptyContentLog(3, 2));
                      }
                    },
                    3,
                    3));
            // conflicts with index 1
            add(
                new RaftLogManagerTester(
                    new EmptyContentLog(1, 2),
                    new ArrayList<Log>() {
                      {
                        add(new EmptyContentLog(1, 1));
                        add(new EmptyContentLog(2, 2));
                      }
                    },
                    2,
                    3));
            add(
                new RaftLogManagerTester(
                    new EmptyContentLog(2, 3),
                    new ArrayList<Log>() {
                      {
                        add(new EmptyContentLog(1, 1));
                        add(new EmptyContentLog(2, 2));
                      }
                    },
                    2,
                    3));
          }
        };
    for (RaftLogManagerTester test : tests) {
      CommittedEntryManager committedEntryManager =
          new CommittedEntryManager(
              ClusterDescriptor.getInstance().getConfig().getMaxNumOfLogsInMem());
      committedEntryManager.applyingSnapshot(new SimpleSnapshot(0, 0));
      try {
        committedEntryManager.append(previousEntries);
      } catch (Exception e) {
      }
      RaftLogManager instance =
          new TestRaftLogManager(
              committedEntryManager, new SyncLogDequeSerializer(testIdentifier), logApplier);
      try {
        instance.append(test.appendingEntry);
        try {
          List<Log> entries = instance.getEntries(1, Integer.MAX_VALUE);
          assertEquals(test.testEntries, entries);
          assertEquals(
              test.testOffset, instance.getUnCommittedEntryManager().getFirstUnCommittedIndex());
        } catch (Exception e) {
          fail("An unexpected exception was thrown.");
        }
      } finally {
        instance.close();
      }
    }
  }

  @Test
  public void checkBound() {
    class RaftLogManagerTester {

      public long low;
      public long high;
      public Class throwClass;

      public RaftLogManagerTester(long low, long high, Class throwClass) {
        this.low = low;
        this.high = high;
        this.throwClass = throwClass;
      }
    }
    long offset = 100;
    long num = 100;
    long half = offset + num / 2;
    long last = offset + num;
    CommittedEntryManager committedEntryManager =
        new CommittedEntryManager(
            ClusterDescriptor.getInstance().getConfig().getMaxNumOfLogsInMem());
    committedEntryManager.applyingSnapshot(new SimpleSnapshot(offset, offset));
    for (long i = 1; i < num / 2; i++) {
      long index = i;
      try {
        committedEntryManager.append(
            new ArrayList<Log>() {
              {
                add(new EmptyContentLog(offset + index, offset + index));
              }
            });
      } catch (Exception e) {
      }
    }
    RaftLogManager instance =
        new TestRaftLogManager(
            committedEntryManager, new SyncLogDequeSerializer(testIdentifier), logApplier);
    try {
      for (long i = num / 2; i < num; i++) {
        long index = i;
        instance.append(
            new ArrayList<Log>() {
              {
                add(new EmptyContentLog(offset + index, offset + index));
              }
            });
      }
      List<RaftLogManagerTester> tests =
          new ArrayList<RaftLogManagerTester>() {
            {
              add(new RaftLogManagerTester(offset - 1, offset + 1, EntryCompactedException.class));
              add(new RaftLogManagerTester(offset, offset + 1, EntryCompactedException.class));
              add(new RaftLogManagerTester(offset + 1, offset + 1, null));
              add(new RaftLogManagerTester(offset + 1, offset + 2, null));
              add(new RaftLogManagerTester(half + 1, half + 2, null));
              add(new RaftLogManagerTester(last, last, null));
              add(new RaftLogManagerTester(last + 1, last + 2, null));
              add(
                  new RaftLogManagerTester(
                      last + 1, last, GetEntriesWrongParametersException.class));
              add(
                  new RaftLogManagerTester(
                      half + 1, half, GetEntriesWrongParametersException.class));
            }
          };
      for (RaftLogManagerTester test : tests) {
        try {
          instance.checkBound(test.low, test.high);
          if (test.throwClass != null) {
            fail("The expected exception is not thrown");
          }
        } catch (Exception e) {
          if (!e.getClass().getName().equals(test.throwClass.getName())) {
            fail("An unexpected exception was thrown.");
          }
        }
      }
    } finally {
      instance.close();
    }
  }

  @Test
  public void applyingSnapshot() throws Exception {
    long index = 100;
    long term = 100;
    CommittedEntryManager committedEntryManager =
        new CommittedEntryManager(
            ClusterDescriptor.getInstance().getConfig().getMaxNumOfLogsInMem());
    committedEntryManager.applyingSnapshot(new SimpleSnapshot(index, term));
    RaftLogManager instance =
        new TestRaftLogManager(
            committedEntryManager, new SyncLogDequeSerializer(testIdentifier), logApplier);
    try {
      instance.applySnapshot(new SimpleSnapshot(index, term));
      assertEquals(instance.getLastLogIndex(), term);
      List<Log> entries = new ArrayList<>();
      for (int i = 1; i <= 10; i++) {
        entries.add(new EmptyContentLog(index + i, index + i));
      }
      instance.maybeAppend(index, term, index, entries);
      assertEquals(1, instance.getCommittedEntryManager().getAllEntries().size());
      assertEquals(10, instance.getUnCommittedEntryManager().getAllEntries().size());
      assertEquals(100, instance.getCommitLogIndex());
      instance.commitTo(105);
      assertEquals(101, instance.getFirstIndex());
      assertEquals(6, instance.getCommittedEntryManager().getAllEntries().size());
      assertEquals(5, instance.getUnCommittedEntryManager().getAllEntries().size());
      assertEquals(105, instance.getCommitLogIndex());
      instance.applySnapshot(new SimpleSnapshot(103, 103));
      assertEquals(104, instance.getFirstIndex());
      assertEquals(3, instance.getCommittedEntryManager().getAllEntries().size());
      assertEquals(5, instance.getUnCommittedEntryManager().getAllEntries().size());
      assertEquals(105, instance.getCommitLogIndex());
      instance.applySnapshot(new SimpleSnapshot(108, 108));
      assertEquals(109, instance.getFirstIndex());
      assertEquals(1, instance.getCommittedEntryManager().getAllEntries().size());
      assertEquals(0, instance.getUnCommittedEntryManager().getAllEntries().size());
      assertEquals(108, instance.getCommitLogIndex());
    } finally {
      instance.close();
    }
  }

  @Test
  public void getEntries() throws TruncateCommittedEntryException {
    class RaftLogManagerTester {

      public long low;
      public long high;
      public List<Log> testEntries;
      public Class throwClass;

      public RaftLogManagerTester(long low, long high, List<Log> testEntries, Class throwClass) {
        this.low = low;
        this.high = high;
        this.testEntries = testEntries;
        this.throwClass = throwClass;
      }
    }
    long offset = 100;
    long num = 100;
    long half = offset + num / 2;
    long last = offset + num;
    CommittedEntryManager committedEntryManager =
        new CommittedEntryManager(
            ClusterDescriptor.getInstance().getConfig().getMaxNumOfLogsInMem());
    committedEntryManager.applyingSnapshot(new SimpleSnapshot(offset, offset));
    List<Log> logs = new ArrayList<>();
    for (long i = 1; i < num / 2; i++) {
      logs.add(new EmptyContentLog(offset + i, offset + i));
    }
    committedEntryManager.append(logs);

    RaftLogManager instance =
        new TestRaftLogManager(
            committedEntryManager, new SyncLogDequeSerializer(testIdentifier), logApplier);
    try {
      for (long i = num / 2; i < num; i++) {
        long index = i;
        logs.add(new EmptyContentLog(offset + index, offset + index));
        instance.append(
            new ArrayList<Log>() {
              {
                add(new EmptyContentLog(offset + index, offset + index));
              }
            });
      }
      instance.append(logs.subList((int) num / 2 - 1, (int) num - 1));

      List<RaftLogManagerTester> tests =
          new ArrayList<RaftLogManagerTester>() {
            {
              add(new RaftLogManagerTester(offset + 1, offset + 1, new ArrayList<>(), null));
              add(
                  new RaftLogManagerTester(
                      offset + 1,
                      offset + 2,
                      new ArrayList<Log>() {
                        {
                          add(new EmptyContentLog(offset + 1, offset + 1));
                        }
                      },
                      null));
              add(
                  new RaftLogManagerTester(
                      half - 1,
                      half + 1,
                      new ArrayList<Log>() {
                        {
                          add(new EmptyContentLog(half - 1, half - 1));
                          add(new EmptyContentLog(half, half));
                        }
                      },
                      null));
              add(
                  new RaftLogManagerTester(
                      half,
                      half + 1,
                      new ArrayList<Log>() {
                        {
                          add(new EmptyContentLog(half, half));
                        }
                      },
                      null));
              add(
                  new RaftLogManagerTester(
                      last - 1,
                      last,
                      new ArrayList<Log>() {
                        {
                          add(new EmptyContentLog(last - 1, last - 1));
                        }
                      },
                      null));
              // test EntryUnavailable
              add(
                  new RaftLogManagerTester(
                      last - 1,
                      last + 1,
                      new ArrayList<Log>() {
                        {
                          add(new EmptyContentLog(last - 1, last - 1));
                        }
                      },
                      null));
              add(new RaftLogManagerTester(last, last + 1, new ArrayList<>(), null));
              add(new RaftLogManagerTester(last + 1, last + 2, new ArrayList<>(), null));
              // test GetEntriesWrongParametersException
              add(new RaftLogManagerTester(offset + 1, offset, Collections.emptyList(), null));
              // test EntryCompactedException
              add(new RaftLogManagerTester(offset - 1, offset + 1, Collections.emptyList(), null));
              add(new RaftLogManagerTester(offset, offset + 1, Collections.emptyList(), null));
            }
          };
      for (RaftLogManagerTester test : tests) {
        try {
          List<Log> answer = instance.getEntries(test.low, test.high);
          if (test.throwClass != null) {
            fail("The expected exception is not thrown");
          } else {
            assertEquals(test.testEntries, answer);
          }
        } catch (Exception e) {
          if (!e.getClass().getName().equals(test.throwClass.getName())) {
            fail("An unexpected exception was thrown.");
          }
        }
      }
    } finally {
      instance.close();
    }
  }

  @Test
  public void findConflict() {
    class RaftLogManagerTester {

      public List<Log> conflictEntries;
      public long testConflict;

      public RaftLogManagerTester(List<Log> conflictEntries, long testConflict) {
        this.conflictEntries = conflictEntries;
        this.testConflict = testConflict;
      }
    }
    List<Log> previousEntries =
        new ArrayList<Log>() {
          {
            add(new EmptyContentLog(0, 0));
            add(new EmptyContentLog(1, 1));
            add(new EmptyContentLog(2, 2));
          }
        };
    RaftLogManager instance =
        new TestRaftLogManager(
            new CommittedEntryManager(
                ClusterDescriptor.getInstance().getConfig().getMaxNumOfLogsInMem()),
            new SyncLogDequeSerializer(testIdentifier),
            logApplier);
    try {
      instance.append(previousEntries);
      List<RaftLogManagerTester> tests =
          new ArrayList<RaftLogManagerTester>() {
            {
              // no conflict, empty ent
              add(new RaftLogManagerTester(new ArrayList<>(), -1));
              // no conflict
              add(
                  new RaftLogManagerTester(
                      new ArrayList<Log>() {
                        {
                          add(new EmptyContentLog(0, 0));
                          add(new EmptyContentLog(1, 1));
                          add(new EmptyContentLog(2, 2));
                        }
                      },
                      -1));
              add(
                  new RaftLogManagerTester(
                      new ArrayList<Log>() {
                        {
                          add(new EmptyContentLog(1, 1));
                          add(new EmptyContentLog(2, 2));
                        }
                      },
                      -1));
              add(
                  new RaftLogManagerTester(
                      new ArrayList<Log>() {
                        {
                          add(new EmptyContentLog(2, 2));
                        }
                      },
                      -1));
              // no conflict, but has new entries
              add(
                  new RaftLogManagerTester(
                      new ArrayList<Log>() {
                        {
                          add(new EmptyContentLog(0, 0));
                          add(new EmptyContentLog(1, 1));
                          add(new EmptyContentLog(2, 2));
                          add(new EmptyContentLog(3, 3));
                          add(new EmptyContentLog(4, 3));
                        }
                      },
                      3));
              add(
                  new RaftLogManagerTester(
                      new ArrayList<Log>() {
                        {
                          add(new EmptyContentLog(1, 1));
                          add(new EmptyContentLog(2, 2));
                          add(new EmptyContentLog(3, 3));
                          add(new EmptyContentLog(4, 3));
                        }
                      },
                      3));
              add(
                  new RaftLogManagerTester(
                      new ArrayList<Log>() {
                        {
                          add(new EmptyContentLog(2, 2));
                          add(new EmptyContentLog(3, 3));
                          add(new EmptyContentLog(4, 3));
                        }
                      },
                      3));
              add(
                  new RaftLogManagerTester(
                      new ArrayList<Log>() {
                        {
                          add(new EmptyContentLog(3, 3));
                          add(new EmptyContentLog(4, 3));
                        }
                      },
                      3));
              // conflicts with existing entries
              add(
                  new RaftLogManagerTester(
                      new ArrayList<Log>() {
                        {
                          add(new EmptyContentLog(0, 4));
                          add(new EmptyContentLog(1, 4));
                        }
                      },
                      0));
              add(
                  new RaftLogManagerTester(
                      new ArrayList<Log>() {
                        {
                          add(new EmptyContentLog(1, 2));
                          add(new EmptyContentLog(2, 4));
                          add(new EmptyContentLog(3, 4));
                        }
                      },
                      1));
              add(
                  new RaftLogManagerTester(
                      new ArrayList<Log>() {
                        {
                          add(new EmptyContentLog(2, 1));
                          add(new EmptyContentLog(3, 2));
                          add(new EmptyContentLog(4, 4));
                          add(new EmptyContentLog(5, 4));
                        }
                      },
                      2));
            }
          };
      for (RaftLogManagerTester test : tests) {
        assertEquals(test.testConflict, instance.findConflict(test.conflictEntries));
      }
    } finally {
      instance.close();
    }
  }

  @Test
  public void isLogUpToDate() {
    class RaftLogManagerTester {

      public long lastIndex;
      public long lastTerm;
      public boolean isUpToDate;

      public RaftLogManagerTester(long lastIndex, long lastTerm, boolean isUpToDate) {
        this.lastIndex = lastIndex;
        this.lastTerm = lastTerm;
        this.isUpToDate = isUpToDate;
      }
    }
    List<Log> previousEntries =
        new ArrayList<Log>() {
          {
            add(new EmptyContentLog(0, 0));
            add(new EmptyContentLog(1, 1));
            add(new EmptyContentLog(2, 2));
          }
        };
    RaftLogManager instance =
        new TestRaftLogManager(
            new CommittedEntryManager(
                ClusterDescriptor.getInstance().getConfig().getMaxNumOfLogsInMem()),
            new SyncLogDequeSerializer(testIdentifier),
            logApplier);
    try {
      instance.append(previousEntries);
      List<RaftLogManagerTester> tests =
          new ArrayList<RaftLogManagerTester>() {
            {
              // greater term, ignore lastIndex
              add(new RaftLogManagerTester(instance.getLastLogIndex() - 1, 3, true));
              add(new RaftLogManagerTester(instance.getLastLogIndex(), 3, true));
              add(new RaftLogManagerTester(instance.getLastLogIndex() + 1, 3, true));
              // smaller term, ignore lastIndex
              add(new RaftLogManagerTester(instance.getLastLogIndex() - 1, 1, false));
              add(new RaftLogManagerTester(instance.getLastLogIndex(), 1, false));
              add(new RaftLogManagerTester(instance.getLastLogIndex() + 1, 1, false));
              // equal term, equal or lager lastIndex wins
              add(new RaftLogManagerTester(instance.getLastLogIndex() - 1, 2, false));
              add(new RaftLogManagerTester(instance.getLastLogIndex(), 2, true));
              add(new RaftLogManagerTester(instance.getLastLogIndex() + 1, 2, true));
            }
          };
      for (RaftLogManagerTester test : tests) {
        assertEquals(test.isUpToDate, instance.isLogUpToDate(test.lastTerm, test.lastIndex));
      }
    } finally {
      instance.close();
    }
  }

  @Test
  public void testCheckDeleteLog() {
    SyncLogDequeSerializer syncLogDequeSerializer = new SyncLogDequeSerializer(testIdentifier);
    CommittedEntryManager committedEntryManager =
        new CommittedEntryManager(
            ClusterDescriptor.getInstance().getConfig().getMaxNumOfLogsInMem());
    RaftLogManager raftLogManager =
        new TestRaftLogManager(committedEntryManager, syncLogDequeSerializer, logApplier);
    // prevent the commit checker thread from modifying max applied index
    blocked = true;

    int minNumberOfLogs = 100;
    List<Log> testLogs1;

    raftLogManager.setMinNumOfLogsInMem(minNumberOfLogs);
    testLogs1 = TestUtils.prepareNodeLogs(130);
    raftLogManager.append(testLogs1);
    Log lastLog = testLogs1.get(testLogs1.size() - 1);
    raftLogManager.setMaxHaveAppliedCommitIndex(100);
    try {
      raftLogManager.commitTo(lastLog.getCurrLogIndex());
    } catch (LogExecutionException e) {
      Assert.fail(e.toString());
    }

    assertEquals(130, committedEntryManager.getTotalSize());

    // the maxHaveAppliedCommitIndex is smaller than 130-minNumberOfLogs
    long remainNumber = 130 - 20;
    raftLogManager.setMaxHaveAppliedCommitIndex(20);
    raftLogManager.checkDeleteLog();
    assertEquals(remainNumber, committedEntryManager.getTotalSize());

    raftLogManager.setMaxHaveAppliedCommitIndex(100);
    raftLogManager.checkDeleteLog();
    assertEquals(minNumberOfLogs, committedEntryManager.getTotalSize());

    raftLogManager.close();

    // recovery
    syncLogDequeSerializer = new SyncLogDequeSerializer(testIdentifier);
    try {
      List<Log> logs = syncLogDequeSerializer.getAllEntriesAfterAppliedIndex();
      assertEquals(30, logs.size());
      for (int i = 0; i < logs.size(); i++) {
        assertEquals(testLogs1.get(i + 100), logs.get(i));
      }
    } finally {
      syncLogDequeSerializer.close();
    }
  }

  @Test
  public void testApplyAllCommittedLogWhenStartUp() {
    SyncLogDequeSerializer syncLogDequeSerializer = new SyncLogDequeSerializer(testIdentifier);

    CommittedEntryManager committedEntryManager =
        new CommittedEntryManager(
            ClusterDescriptor.getInstance().getConfig().getMaxNumOfLogsInMem());
    RaftLogManager raftLogManager =
        new TestRaftLogManager(committedEntryManager, syncLogDequeSerializer, logApplier);
    try {
      int maxNumberOfLogs = 100;
      List<Log> testLogs1;
      raftLogManager.setMinNumOfLogsInMem(maxNumberOfLogs);
      testLogs1 = TestUtils.prepareNodeLogs(130);
      raftLogManager.append(testLogs1);
      try {
        raftLogManager.commitTo(testLogs1.get(testLogs1.size() - 1).getCurrLogIndex());
      } catch (LogExecutionException e) {
        Assert.fail(e.toString());
      }
      // wait log is applied
      long startTime = System.currentTimeMillis();
      for (Log log : testLogs1) {
        while (!log.isApplied()) {
          if ((System.currentTimeMillis() - startTime) > 60_000) {
            fail(
                String.format(
                    "apply log %s time out after %d",
                    log, (System.currentTimeMillis() - startTime)));
          }
        }
      }
      assertEquals(testLogs1.size(), appliedLogs.size());
      for (Log log : testLogs1) {
        assertTrue(appliedLogs.containsKey(log.getCurrLogIndex()));
        assertEquals(log, appliedLogs.get(log.getCurrLogIndex()));
      }

      raftLogManager.setMaxHaveAppliedCommitIndex(100);
      raftLogManager.checkDeleteLog();
      assertEquals(maxNumberOfLogs, committedEntryManager.getTotalSize());
      //    assertEquals(maxNumberOfLogs, syncLogDequeSerializer.getLogSizeDeque().size());
      raftLogManager.close();

      raftLogManager =
          new TestRaftLogManager(committedEntryManager, syncLogDequeSerializer, logApplier);
      raftLogManager.applyAllCommittedLogWhenStartUp();
      assertEquals(appliedLogs.size(), testLogs1.size());
      for (Log log : testLogs1) {
        assertTrue(appliedLogs.containsKey(log.getCurrLogIndex()));
        assertEquals(log, appliedLogs.get(log.getCurrLogIndex()));
      }
    } finally {
      raftLogManager.close();
    }
  }

  @Test
  public void testCheckAppliedLogIndex() {
    SyncLogDequeSerializer syncLogDequeSerializer = new SyncLogDequeSerializer(testIdentifier);
    CommittedEntryManager committedEntryManager =
        new CommittedEntryManager(
            ClusterDescriptor.getInstance().getConfig().getMaxNumOfLogsInMem());
    RaftLogManager raftLogManager =
        new TestRaftLogManager(committedEntryManager, syncLogDequeSerializer, logApplier);

    try {
      int minNumberOfLogs = 100;
      List<Log> testLogs1;
      raftLogManager.setMinNumOfLogsInMem(minNumberOfLogs);
      testLogs1 = TestUtils.prepareNodeLogs(130);
      raftLogManager.append(testLogs1);

      try {
        raftLogManager.commitTo(testLogs1.get(testLogs1.size() - 1 - 30).getCurrLogIndex());
      } catch (LogExecutionException e) {
        Assert.fail(e.getMessage());
      }
      // wait log is applied
      long startTime = System.currentTimeMillis();
      for (int i = 0; i < testLogs1.size() - 30; i++) {
        while (!testLogs1.get(i).isApplied()) {
          if ((System.currentTimeMillis() - startTime) > 60_000) {
            Assert.fail("apply log time out");
            break;
          }
        }
      }

      assertEquals(testLogs1.size() - 30, appliedLogs.size());
      for (Log log : testLogs1.subList(0, testLogs1.size() - 30)) {
        assertTrue(appliedLogs.containsKey(log.getCurrLogIndex()));
        assertEquals(log, appliedLogs.get(log.getCurrLogIndex()));
      }

      raftLogManager.setMaxHaveAppliedCommitIndex(
          testLogs1.get(testLogs1.size() - 1 - 30).getCurrLogIndex());
      assertEquals(
          raftLogManager.getCommitLogIndex(), raftLogManager.getMaxHaveAppliedCommitIndex());

      raftLogManager.checkDeleteLog();
      assertEquals(minNumberOfLogs, committedEntryManager.getTotalSize());

      for (int i = testLogs1.size() - 30; i < testLogs1.size(); i++) {
        try {
          raftLogManager.commitTo(i);
        } catch (LogExecutionException e) {
          Assert.fail(e.toString());
        }
        while (!testLogs1.get(i).isApplied()) {
          if ((System.currentTimeMillis() - startTime) > 60_000) {
            Assert.fail("apply log time out");
            break;
          }
        }
        raftLogManager.doCheckAppliedLogIndex();
        assertEquals(raftLogManager.getMaxHaveAppliedCommitIndex(), i);
      }
    } finally {
      raftLogManager.close();
    }
  }
}
