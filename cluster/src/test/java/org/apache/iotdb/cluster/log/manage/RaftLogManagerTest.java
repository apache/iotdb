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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.iotdb.cluster.common.TestLogApplier;
import org.apache.iotdb.cluster.common.TestUtils;
import org.apache.iotdb.cluster.exception.EntryCompactedException;
import org.apache.iotdb.cluster.exception.EntryUnavailableException;
import org.apache.iotdb.cluster.exception.GetEntriesWrongParametersException;
import org.apache.iotdb.cluster.log.Log;
import org.apache.iotdb.cluster.log.LogApplier;
import org.apache.iotdb.cluster.log.logtypes.EmptyContentLog;
import org.apache.iotdb.cluster.log.manage.serializable.SyncLogDequeSerializer;
import org.apache.iotdb.cluster.log.snapshot.SimpleSnapshot;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class RaftLogManagerTest {

  private Set<Log> appliedLogs;
  private LogApplier logApplier = new TestLogApplier() {
    @Override
    public void apply(Log log) {
      appliedLogs.add(log);
    }
  };
  private int testIdentifier = 1;

  @Before
  public void setUp() {
    appliedLogs = new HashSet<>();
  }

  @After
  public void tearDown() {
    File dir = new File(SyncLogDequeSerializer.getLogDir(testIdentifier));
    for (File file : dir.listFiles()) {
      file.delete();
    }
    dir.delete();
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
    CommittedEntryManager committedEntryManager = new CommittedEntryManager();
    committedEntryManager.applyingSnapshot(new SimpleSnapshot(offset, offset));
    RaftLogManager instance = new RaftLogManager(committedEntryManager,
        new SyncLogDequeSerializer(testIdentifier), logApplier);
    try {
      for (long i = 1; i < num; i++) {
        long index = i;
        instance.append(new ArrayList<Log>() {{
          add(new EmptyContentLog(offset + index, offset + index));
        }});
      }
      List<RaftLogManagerTester> tests = new ArrayList<RaftLogManagerTester>() {{
        add(new RaftLogManagerTester(offset - 1, 0, EntryCompactedException.class));
        add(new RaftLogManagerTester(offset, offset, null));
        add(new RaftLogManagerTester(half, half, null));
        add(new RaftLogManagerTester(last - 1, last - 1, null));
        add(new RaftLogManagerTester(last, 0, EntryUnavailableException.class));
      }};
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
    CommittedEntryManager committedEntryManager = new CommittedEntryManager();
    committedEntryManager.applyingSnapshot(new SimpleSnapshot(offset, offset));
    RaftLogManager instance = new RaftLogManager(committedEntryManager,
        new SyncLogDequeSerializer(testIdentifier), logApplier);
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
    CommittedEntryManager committedEntryManager = new CommittedEntryManager();
    committedEntryManager.applyingSnapshot(new SimpleSnapshot(offset, offset));
    RaftLogManager instance = new RaftLogManager(committedEntryManager,
        new SyncLogDequeSerializer(testIdentifier), logApplier);
    try {
      for (long i = 1; i < num; i++) {
        long index = i;
        instance.append(new ArrayList<Log>() {{
          add(new EmptyContentLog(offset + index, offset + index));
        }});
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
    CommittedEntryManager committedEntryManager = new CommittedEntryManager();
    committedEntryManager.applyingSnapshot(new SimpleSnapshot(offset, offset));
    RaftLogManager instance = new RaftLogManager(committedEntryManager,
        new SyncLogDequeSerializer(testIdentifier), logApplier);
    try {
      for (long i = 1; i < num; i++) {
        long index = i;
        instance.append(new ArrayList<Log>() {{
          add(new EmptyContentLog(offset + index, offset + index));
        }});
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

      public RaftLogManagerTester(long leaderCommit, long term, long testCommittedEntryManagerSize,
          long testUnCommittedEntryManagerSize, long testCommitIndex, boolean testCommit) {
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
    CommittedEntryManager committedEntryManager = new CommittedEntryManager();
    committedEntryManager.applyingSnapshot(new SimpleSnapshot(offset, offset));
    for (long i = 1; i < num / 2; i++) {
      long index = i;
      try {
        committedEntryManager.append(new ArrayList<Log>() {{
          add(new EmptyContentLog(offset + index, offset + index));
        }});
      } catch (Exception e) {
      }
    }
    RaftLogManager instance = new RaftLogManager(committedEntryManager,
        new SyncLogDequeSerializer(testIdentifier), logApplier);
    try {
      for (long i = num / 2; i < num; i++) {
        long index = i;
        instance.append(new ArrayList<Log>() {{
          add(new EmptyContentLog(offset + index, offset + index));
        }});
      }
      List<RaftLogManagerTester> tests = new ArrayList<RaftLogManagerTester>() {{
        // term small leaderCommit
        add(new RaftLogManagerTester(offset - 10, offset - 9, num / 2, num / 2, half - 1, false));
        add(new RaftLogManagerTester(offset - 10, offset - 10, num / 2, num / 2, half - 1, false));
        add(new RaftLogManagerTester(half - 1, half - 1, num / 2, num / 2, half - 1, false));
        // normal case
        add(new RaftLogManagerTester(half, half + 1, num / 2, num / 2, half - 1, false));
        add(new RaftLogManagerTester(half, half, num / 2 + 1, num / 2 - 1, half, true));
        add(new RaftLogManagerTester(last - 1, last - 1, num, 0, last - 1, true));
        // test large leaderCommit
        add(new RaftLogManagerTester(last, last, num, 0, last - 1, false));
      }};
      for (RaftLogManagerTester test : tests) {
        boolean answer = instance.maybeCommit(test.leaderCommit, test.term);
        assertEquals(test.testCommittedEntryManagerSize,
            instance.committedEntryManager.getAllEntries().size());
        assertEquals(test.testUnCommittedEntryManagerSize,
            instance.unCommittedEntryManager.getAllEntries().size());
        assertEquals(test.testCommitIndex, instance.getCommitLogIndex());
        assertEquals(test.testCommit, answer);
      }
    } finally {
      instance.close();
    }
  }

  @Test
  public void commitTo() {
    class RaftLogManagerTester {

      public long commitTo;
      public long testCommittedEntryManagerSize;
      public long testUnCommittedEntryManagerSize;
      public long testCommitIndex;

      public RaftLogManagerTester(long commitTo, long testCommittedEntryManagerSize,
          long testUnCommittedEntryManagerSize, long testCommitIndex) {
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
    CommittedEntryManager committedEntryManager = new CommittedEntryManager();
    committedEntryManager.applyingSnapshot(new SimpleSnapshot(offset, offset));
    for (long i = 1; i < num / 2; i++) {
      long index = i;
      try {
        committedEntryManager.append(new ArrayList<Log>() {{
          add(new EmptyContentLog(offset + index, offset + index));
        }});
      } catch (Exception e) {
      }
    }
    RaftLogManager instance = new RaftLogManager(committedEntryManager,
        new SyncLogDequeSerializer(testIdentifier), logApplier);
    try {
      for (long i = num / 2; i < num; i++) {
        long index = i;
        instance.append(new ArrayList<Log>() {{
          add(new EmptyContentLog(offset + index, offset + index));
        }});
      }
      List<RaftLogManagerTester> tests = new ArrayList<RaftLogManagerTester>() {{
        add(new RaftLogManagerTester(offset - 10, num / 2, num / 2, half - 1));
        add(new RaftLogManagerTester(half - 1, num / 2, num / 2, half - 1));
        add(new RaftLogManagerTester(half, num / 2 + 1, num / 2 - 1, half));
        add(new RaftLogManagerTester(half + 10, num / 2 + 11, num / 2 - 11, half + 10));
        add(new RaftLogManagerTester(last - 1, num, 0, last - 1));
      }};
      for (RaftLogManagerTester test : tests) {
        instance.commitTo(test.commitTo);
        assertEquals(test.testCommittedEntryManagerSize,
            instance.committedEntryManager.getAllEntries().size());
        assertEquals(test.testUnCommittedEntryManagerSize,
            instance.unCommittedEntryManager.getAllEntries().size());
        assertEquals(test.testCommitIndex, instance.getCommitLogIndex());
      }
    } finally {
      instance.close();
    }
  }

  @Test
  public void applyEntries() {
    List<Log> testLogs = TestUtils.prepareTestLogs(10);
    RaftLogManager instance = new RaftLogManager(new CommittedEntryManager(),
        new SyncLogDequeSerializer(testIdentifier), logApplier);
    try {
      instance.applyEntries(testLogs);
      assertTrue(appliedLogs.containsAll(testLogs.subList(0, 10)));
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
    CommittedEntryManager committedEntryManager = new CommittedEntryManager();
    committedEntryManager.applyingSnapshot(new SimpleSnapshot(offset, offset));
    for (long i = 1; i < num / 2; i++) {
      long index = i;
      try {
        committedEntryManager.append(new ArrayList<Log>() {{
          add(new EmptyContentLog(offset + index, offset + index));
        }});
      } catch (Exception e) {
      }
    }
    RaftLogManager instance = new RaftLogManager(committedEntryManager,
        new SyncLogDequeSerializer(testIdentifier), logApplier);
    try {
      for (long i = num / 2; i < num; i++) {
        long index = i;
        instance.append(new ArrayList<Log>() {{
          add(new EmptyContentLog(offset + index, offset + index));
        }});
      }
      List<RaftLogManagerTester> tests = new ArrayList<RaftLogManagerTester>() {{
        add(new RaftLogManagerTester(offset - 1, offset - 1, false));
        add(new RaftLogManagerTester(offset, offset - 1, false));
        add(new RaftLogManagerTester(offset, offset, true));
        add(new RaftLogManagerTester(half, half, true));
        add(new RaftLogManagerTester(half + 1, half, false));
        add(new RaftLogManagerTester(last - 1, last - 1, true));
        add(new RaftLogManagerTester(last, last, false));
      }};
      for (RaftLogManagerTester test : tests) {
        assertEquals(test.testMatch, instance.matchTerm(test.index, test.term));
      }
    } finally {
      instance.close();
    }
  }

  @Test
  public void maybeAppendBatch() {
    class RaftLogManagerTester {

      public List<Log> entries;
      public long lastIndex;
      public long lastTerm;
      public long leaderCommit;
      public long testLastIndex;
      public long testCommitIndex;
      public boolean testAppend;

      public RaftLogManagerTester(List<Log> entries, long lastIndex, long lastTerm,
          long leaderCommit, long testLastIndex, long testCommitIndex, boolean testAppend) {
        this.entries = entries;
        this.lastIndex = lastIndex;
        this.lastTerm = lastTerm;
        this.leaderCommit = leaderCommit;
        this.testLastIndex = testLastIndex;
        this.testCommitIndex = testCommitIndex;
        this.testAppend = testAppend;
      }
    }
    List<Log> previousEntries = new ArrayList<Log>() {{
      add(new EmptyContentLog(1, 1));
      add(new EmptyContentLog(2, 2));
      add(new EmptyContentLog(3, 3));
    }};
    long lastIndex = 3;
    long lastTerm = 3;
    long commit = 1;
    List<RaftLogManagerTester> tests = new ArrayList<RaftLogManagerTester>() {{
      // not match: term is different
      add(new RaftLogManagerTester(new ArrayList<>(), lastIndex, lastTerm - 1, lastIndex, -1,
          commit, false));
      // not match: index out of bound
      add(new RaftLogManagerTester(new ArrayList<>(), lastIndex + 1, lastTerm, lastIndex, -1,
          commit, false));
      // match with the last existing entry
      add(new RaftLogManagerTester(new ArrayList<>(), lastIndex, lastTerm, lastIndex, lastIndex,
          lastIndex, true));
      // do not increase commit higher than newLastIndex
      add(new RaftLogManagerTester(new ArrayList<>(), lastIndex, lastTerm, lastIndex + 1, lastIndex,
          lastIndex, true));
      // commit up to the commit in the message
      add(new RaftLogManagerTester(new ArrayList<>(), lastIndex, lastTerm, lastIndex - 1, lastIndex,
          lastIndex - 1, true));
      // commit do not decrease
      add(new RaftLogManagerTester(new ArrayList<>(), lastIndex, lastTerm, 0, lastIndex, commit,
          true));
      // normal case
      add(new RaftLogManagerTester(new ArrayList<Log>() {{
        add(new EmptyContentLog(lastIndex + 1, 4));
      }}, lastIndex, lastTerm, lastIndex, lastIndex + 1, lastIndex, true));
      add(new RaftLogManagerTester(new ArrayList<Log>() {{
        add(new EmptyContentLog(lastIndex + 1, 4));
      }}, lastIndex, lastTerm, lastIndex + 1, lastIndex + 1, lastIndex + 1, true));
      add(new RaftLogManagerTester(new ArrayList<Log>() {{
        add(new EmptyContentLog(lastIndex + 1, 4));
        add(new EmptyContentLog(lastIndex + 2, 4));
      }}, lastIndex, lastTerm, lastIndex + 2, lastIndex + 2, lastIndex + 2, true));
      // do not increase commit higher than newLastIndex
      add(new RaftLogManagerTester(new ArrayList<Log>() {{
        add(new EmptyContentLog(lastIndex + 1, 4));
      }}, lastIndex, lastTerm, lastIndex + 2, lastIndex + 1, lastIndex + 1, true));
      // match with the the entry in the middle
      add(new RaftLogManagerTester(new ArrayList<Log>() {{
        add(new EmptyContentLog(lastIndex, 4));
      }}, lastIndex - 1, lastTerm - 1, lastIndex, lastIndex, lastIndex, true));
      add(new RaftLogManagerTester(new ArrayList<Log>() {{
        add(new EmptyContentLog(lastIndex - 1, 4));
      }}, lastIndex - 2, lastTerm - 2, lastIndex, lastIndex - 1, lastIndex - 1, true));
      add(new RaftLogManagerTester(new ArrayList<Log>() {{
        add(new EmptyContentLog(lastIndex - 1, 4));
        add(new EmptyContentLog(lastIndex, 4));
      }}, lastIndex - 2, lastTerm - 2, lastIndex, lastIndex, lastIndex, true));
    }};
    for (RaftLogManagerTester test : tests) {
      CommittedEntryManager committedEntryManager = new CommittedEntryManager();
      committedEntryManager.applyingSnapshot(new SimpleSnapshot(0, 0));
      RaftLogManager instance = new RaftLogManager(committedEntryManager,
          new SyncLogDequeSerializer(testIdentifier), logApplier);
      try {
        instance.append(previousEntries);
        instance.commitTo(commit);
        assertEquals(test.testLastIndex,
            instance.maybeAppend(test.lastIndex, test.lastTerm, test.leaderCommit, test.entries));
        assertEquals(test.testCommitIndex, instance.getCommitLogIndex());
        if (test.testAppend) {
          try {
            List<Log> entries = instance
                .getEntries(instance.getLastLogIndex() - test.entries.size() + 1,
                    Integer.MAX_VALUE);
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
  public void maybeAppendSingle() {
    class RaftLogManagerTester {

      public Log entry;
      public long lastIndex;
      public long lastTerm;
      public long leaderCommit;
      public long testLastIndex;
      public long testCommitIndex;
      public boolean testAppend;

      public RaftLogManagerTester(Log entry, long lastIndex, long lastTerm,
          long leaderCommit, long testLastIndex, long testCommitIndex, boolean testAppend) {
        this.entry = entry;
        this.lastIndex = lastIndex;
        this.lastTerm = lastTerm;
        this.leaderCommit = leaderCommit;
        this.testLastIndex = testLastIndex;
        this.testCommitIndex = testCommitIndex;
        this.testAppend = testAppend;
      }
    }
    List<Log> previousEntries = new ArrayList<Log>() {{
      add(new EmptyContentLog(1, 1));
      add(new EmptyContentLog(2, 2));
      add(new EmptyContentLog(3, 3));
    }};
    long lastIndex = 3;
    long lastTerm = 3;
    long commit = 1;
    List<RaftLogManagerTester> tests = new ArrayList<RaftLogManagerTester>() {{
      // not match: term is different
      add(new RaftLogManagerTester(null, lastIndex, lastTerm - 1, lastIndex, -1,
          commit, false));
      // not match: index out of bound
      add(new RaftLogManagerTester(null, lastIndex + 1, lastTerm, lastIndex, -1,
          commit, false));
      // normal case
      add(new RaftLogManagerTester(new EmptyContentLog(lastIndex + 1, 4), lastIndex, lastTerm,
          lastIndex, lastIndex + 1, lastIndex, true));
      add(new RaftLogManagerTester(new EmptyContentLog(lastIndex + 1, 4), lastIndex, lastTerm,
          lastIndex + 1, lastIndex + 1, lastIndex + 1, true));
      // do not increase commit higher than newLastIndex
      add(new RaftLogManagerTester(new EmptyContentLog(lastIndex + 1, 4), lastIndex, lastTerm,
          lastIndex + 2, lastIndex + 1, lastIndex + 1, true));
      // match with the the entry in the middle
      add(new RaftLogManagerTester(new EmptyContentLog(lastIndex, 4), lastIndex - 1, lastTerm - 1,
          lastIndex, lastIndex, lastIndex, true));
      add(new RaftLogManagerTester(new EmptyContentLog(lastIndex - 1, 4), lastIndex - 2,
          lastTerm - 2, lastIndex, lastIndex - 1, lastIndex - 1, true));
    }};
    for (RaftLogManagerTester test : tests) {
      CommittedEntryManager committedEntryManager = new CommittedEntryManager();
      committedEntryManager.applyingSnapshot(new SimpleSnapshot(0, 0));
      RaftLogManager instance = new RaftLogManager(committedEntryManager,
          new SyncLogDequeSerializer(testIdentifier), logApplier);
      try {
        instance.append(previousEntries);
        instance.commitTo(commit);
        assertEquals(test.testLastIndex,
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
  public void appendBatch() {
    class RaftLogManagerTester {

      public List<Log> appendingEntries;
      public List<Log> testEntries;
      public long testLastIndexAfterAppend;
      public long testOffset;

      public RaftLogManagerTester(List<Log> appendingEntries, List<Log> testEntries,
          long testLastIndexAfterAppend, long testOffset) {
        this.appendingEntries = appendingEntries;
        this.testEntries = testEntries;
        this.testLastIndexAfterAppend = testLastIndexAfterAppend;
        this.testOffset = testOffset;
      }
    }
    List<Log> previousEntries = new ArrayList<Log>() {{
      add(new EmptyContentLog(1, 1));
      add(new EmptyContentLog(2, 2));
    }};
    List<RaftLogManagerTester> tests = new ArrayList<RaftLogManagerTester>() {{
      add(new RaftLogManagerTester(new ArrayList<>(), new ArrayList<Log>() {{
        add(new EmptyContentLog(1, 1));
        add(new EmptyContentLog(2, 2));
      }}, 2, 3));
      add(new RaftLogManagerTester(new ArrayList<Log>() {{
        add(new EmptyContentLog(3, 2));
      }}, new ArrayList<Log>() {{
        add(new EmptyContentLog(1, 1));
        add(new EmptyContentLog(2, 2));
        add(new EmptyContentLog(3, 2));
      }}, 3, 3));
      // conflicts with index 1
      add(new RaftLogManagerTester(new ArrayList<Log>() {{
        add(new EmptyContentLog(1, 2));
      }}, new ArrayList<Log>() {{
        add(new EmptyContentLog(1, 1));
        add(new EmptyContentLog(2, 2));
      }}, 2, 3));
      add(new RaftLogManagerTester(new ArrayList<Log>() {{
        add(new EmptyContentLog(2, 3));
        add(new EmptyContentLog(3, 3));
      }}, new ArrayList<Log>() {{
        add(new EmptyContentLog(1, 1));
        add(new EmptyContentLog(2, 2));
      }}, 2, 3));
    }};
    for (RaftLogManagerTester test : tests) {
      CommittedEntryManager committedEntryManager = new CommittedEntryManager();
      committedEntryManager.applyingSnapshot(new SimpleSnapshot(0, 0));
      try {
        committedEntryManager.append(previousEntries);
      } catch (Exception e) {
      }
      RaftLogManager instance = new RaftLogManager(committedEntryManager,
          new SyncLogDequeSerializer(testIdentifier), logApplier);
      instance.append(test.appendingEntries);
      try {
        List<Log> entries = instance.getEntries(1, Integer.MAX_VALUE);
        assertEquals(test.testEntries, entries);
        assertEquals(test.testOffset, instance.unCommittedEntryManager.getFirstUnCommittedIndex());
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

      public RaftLogManagerTester(Log appendingEntry, List<Log> testEntries,
          long testLastIndexAfterAppend, long testOffset) {
        this.appendingEntry = appendingEntry;
        this.testEntries = testEntries;
        this.testLastIndexAfterAppend = testLastIndexAfterAppend;
        this.testOffset = testOffset;
      }
    }
    List<Log> previousEntries = new ArrayList<Log>() {{
      add(new EmptyContentLog(1, 1));
      add(new EmptyContentLog(2, 2));
    }};
    List<RaftLogManagerTester> tests = new ArrayList<RaftLogManagerTester>() {{
      add(new RaftLogManagerTester(new EmptyContentLog(3, 2), new ArrayList<Log>() {{
        add(new EmptyContentLog(1, 1));
        add(new EmptyContentLog(2, 2));
        add(new EmptyContentLog(3, 2));
      }}, 3, 3));
      // conflicts with index 1
      add(new RaftLogManagerTester(new EmptyContentLog(1, 2), new ArrayList<Log>() {{
        add(new EmptyContentLog(1, 1));
        add(new EmptyContentLog(2, 2));
      }}, 2, 3));
      add(new RaftLogManagerTester(new EmptyContentLog(2, 3), new ArrayList<Log>() {{
        add(new EmptyContentLog(1, 1));
        add(new EmptyContentLog(2, 2));
      }}, 2, 3));
    }};
    for (RaftLogManagerTester test : tests) {
      CommittedEntryManager committedEntryManager = new CommittedEntryManager();
      committedEntryManager.applyingSnapshot(new SimpleSnapshot(0, 0));
      try {
        committedEntryManager.append(previousEntries);
      } catch (Exception e) {
      }
      RaftLogManager instance = new RaftLogManager(committedEntryManager,
          new SyncLogDequeSerializer(testIdentifier), logApplier);
      try {
        instance.append(test.appendingEntry);
        try {
          List<Log> entries = instance.getEntries(1, Integer.MAX_VALUE);
          assertEquals(test.testEntries, entries);
          assertEquals(test.testOffset,
              instance.unCommittedEntryManager.getFirstUnCommittedIndex());
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
    CommittedEntryManager committedEntryManager = new CommittedEntryManager();
    committedEntryManager.applyingSnapshot(new SimpleSnapshot(offset, offset));
    for (long i = 1; i < num / 2; i++) {
      long index = i;
      try {
        committedEntryManager.append(new ArrayList<Log>() {{
          add(new EmptyContentLog(offset + index, offset + index));
        }});
      } catch (Exception e) {
      }
    }
    RaftLogManager instance = new RaftLogManager(committedEntryManager,
        new SyncLogDequeSerializer(testIdentifier), logApplier);
    try {
      for (long i = num / 2; i < num; i++) {
        long index = i;
        instance.append(new ArrayList<Log>() {{
          add(new EmptyContentLog(offset + index, offset + index));
        }});
      }
      List<RaftLogManagerTester> tests = new ArrayList<RaftLogManagerTester>() {{
        add(new RaftLogManagerTester(offset - 1, offset + 1, EntryCompactedException.class));
        add(new RaftLogManagerTester(offset, offset + 1, EntryCompactedException.class));
        add(new RaftLogManagerTester(offset + 1, offset + 1, null));
        add(new RaftLogManagerTester(offset + 1, offset + 2, null));
        add(new RaftLogManagerTester(half + 1, half + 2, null));
        add(new RaftLogManagerTester(last, last, null));
        add(new RaftLogManagerTester(last + 1, last + 2, null));
        add(new RaftLogManagerTester(last + 1, last, GetEntriesWrongParametersException.class));
        add(new RaftLogManagerTester(half + 1, half, GetEntriesWrongParametersException.class));
      }};
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
  public void applyingSnapshot() {
    long index = 100;
    long term = 100;
    CommittedEntryManager committedEntryManager = new CommittedEntryManager();
    committedEntryManager.applyingSnapshot(new SimpleSnapshot(index, term));
    RaftLogManager instance = new RaftLogManager(committedEntryManager,
        new SyncLogDequeSerializer(testIdentifier), logApplier);
    try {
      instance.applyingSnapshot(new SimpleSnapshot(index, term));
      assertEquals(instance.getLastLogIndex(), term);
      List<Log> entries = new ArrayList<>();
      for (int i = 1; i <= 10; i++) {
        entries.add(new EmptyContentLog(index + i, index + i));
      }
      instance.maybeAppend(index, term, index, entries);
      assertEquals(1, instance.committedEntryManager.getAllEntries().size());
      assertEquals(10, instance.unCommittedEntryManager.getAllEntries().size());
      assertEquals(100, instance.getCommitLogIndex());
      instance.commitTo(105);
      assertEquals(101, instance.getFirstIndex());
      assertEquals(6, instance.committedEntryManager.getAllEntries().size());
      assertEquals(5, instance.unCommittedEntryManager.getAllEntries().size());
      assertEquals(105, instance.getCommitLogIndex());
      instance.applyingSnapshot(new SimpleSnapshot(103, 103));
      assertEquals(104, instance.getFirstIndex());
      assertEquals(3, instance.committedEntryManager.getAllEntries().size());
      assertEquals(5, instance.unCommittedEntryManager.getAllEntries().size());
      assertEquals(105, instance.getCommitLogIndex());
      instance.applyingSnapshot(new SimpleSnapshot(108, 108));
      assertEquals(109, instance.getFirstIndex());
      assertEquals(1, instance.committedEntryManager.getAllEntries().size());
      assertEquals(0, instance.unCommittedEntryManager.getAllEntries().size());
      assertEquals(108, instance.getCommitLogIndex());
    } finally {
      instance.close();
    }
  }

  @Test
  public void getEntries() {
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
    CommittedEntryManager committedEntryManager = new CommittedEntryManager();
    committedEntryManager.applyingSnapshot(new SimpleSnapshot(offset, offset));
    for (long i = 1; i < num / 2; i++) {
      long index = i;
      try {
        committedEntryManager.append(new ArrayList<Log>() {{
          add(new EmptyContentLog(offset + index, offset + index));
        }});
      } catch (Exception e) {
      }
    }
    RaftLogManager instance = new RaftLogManager(committedEntryManager,
        new SyncLogDequeSerializer(testIdentifier), logApplier);
    try {
      for (long i = num / 2; i < num; i++) {
        long index = i;
        instance.append(new ArrayList<Log>() {{
          add(new EmptyContentLog(offset + index, offset + index));
        }});
      }
      List<RaftLogManagerTester> tests = new ArrayList<RaftLogManagerTester>() {{
        add(new RaftLogManagerTester(offset + 1, offset + 1, new ArrayList<>(), null));
        add(new RaftLogManagerTester(offset + 1, offset + 2, new ArrayList<Log>() {{
          add(new EmptyContentLog(offset + 1, offset + 1));
        }}, null));
        add(new RaftLogManagerTester(half - 1, half + 1, new ArrayList<Log>() {{
          add(new EmptyContentLog(half - 1, half - 1));
          add(new EmptyContentLog(half, half));
        }}, null));
        add(new RaftLogManagerTester(half, half + 1, new ArrayList<Log>() {{
          add(new EmptyContentLog(half, half));
        }}, null));
        add(new RaftLogManagerTester(last - 1, last, new ArrayList<Log>() {{
          add(new EmptyContentLog(last - 1, last - 1));
        }}, null));
        // test EntryUnavailable
        add(new RaftLogManagerTester(last - 1, last + 1, new ArrayList<Log>() {{
          add(new EmptyContentLog(last - 1, last - 1));
        }}, null));
        add(new RaftLogManagerTester(last, last + 1, new ArrayList<>(), null));
        add(new RaftLogManagerTester(last + 1, last + 2, new ArrayList<>(), null));
        // test GetEntriesWrongParametersException
        add(new RaftLogManagerTester(offset + 1, offset, null,
            GetEntriesWrongParametersException.class));
        // test EntryCompactedException
        add(new RaftLogManagerTester(offset - 1, offset + 1, null, EntryCompactedException.class));
        add(new RaftLogManagerTester(offset, offset + 1, null, EntryCompactedException.class));
      }};
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
    List<Log> previousEntries = new ArrayList<Log>() {{
      add(new EmptyContentLog(0, 0));
      add(new EmptyContentLog(1, 1));
      add(new EmptyContentLog(2, 2));
    }};
    RaftLogManager instance = new RaftLogManager(new CommittedEntryManager(),
        new SyncLogDequeSerializer(testIdentifier), logApplier);
    try {
      instance.append(previousEntries);
      List<RaftLogManagerTester> tests = new ArrayList<RaftLogManagerTester>() {{
        // no conflict, empty ent
        add(new RaftLogManagerTester(new ArrayList<>(), -1));
        // no conflict
        add(new RaftLogManagerTester(new ArrayList<Log>() {{
          add(new EmptyContentLog(0, 0));
          add(new EmptyContentLog(1, 1));
          add(new EmptyContentLog(2, 2));
        }}, -1));
        add(new RaftLogManagerTester(new ArrayList<Log>() {{
          add(new EmptyContentLog(1, 1));
          add(new EmptyContentLog(2, 2));
        }}, -1));
        add(new RaftLogManagerTester(new ArrayList<Log>() {{
          add(new EmptyContentLog(2, 2));
        }}, -1));
        // no conflict, but has new entries
        add(new RaftLogManagerTester(new ArrayList<Log>() {{
          add(new EmptyContentLog(0, 0));
          add(new EmptyContentLog(1, 1));
          add(new EmptyContentLog(2, 2));
          add(new EmptyContentLog(3, 3));
          add(new EmptyContentLog(4, 3));
        }}, 3));
        add(new RaftLogManagerTester(new ArrayList<Log>() {{
          add(new EmptyContentLog(1, 1));
          add(new EmptyContentLog(2, 2));
          add(new EmptyContentLog(3, 3));
          add(new EmptyContentLog(4, 3));
        }}, 3));
        add(new RaftLogManagerTester(new ArrayList<Log>() {{
          add(new EmptyContentLog(2, 2));
          add(new EmptyContentLog(3, 3));
          add(new EmptyContentLog(4, 3));
        }}, 3));
        add(new RaftLogManagerTester(new ArrayList<Log>() {{
          add(new EmptyContentLog(3, 3));
          add(new EmptyContentLog(4, 3));
        }}, 3));
        // conflicts with existing entries
        add(new RaftLogManagerTester(new ArrayList<Log>() {{
          add(new EmptyContentLog(0, 4));
          add(new EmptyContentLog(1, 4));
        }}, 0));
        add(new RaftLogManagerTester(new ArrayList<Log>() {{
          add(new EmptyContentLog(1, 2));
          add(new EmptyContentLog(2, 4));
          add(new EmptyContentLog(3, 4));
        }}, 1));
        add(new RaftLogManagerTester(new ArrayList<Log>() {{
          add(new EmptyContentLog(2, 1));
          add(new EmptyContentLog(3, 2));
          add(new EmptyContentLog(4, 4));
          add(new EmptyContentLog(5, 4));
        }}, 2));
      }};
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
    List<Log> previousEntries = new ArrayList<Log>() {{
      add(new EmptyContentLog(0, 0));
      add(new EmptyContentLog(1, 1));
      add(new EmptyContentLog(2, 2));
    }};
    RaftLogManager instance = new RaftLogManager(new CommittedEntryManager(),
        new SyncLogDequeSerializer(testIdentifier), logApplier);
    try {
      instance.append(previousEntries);
      List<RaftLogManagerTester> tests = new ArrayList<RaftLogManagerTester>() {{
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
      }};
      for (RaftLogManagerTester test : tests) {
        assertEquals(test.isUpToDate, instance.isLogUpToDate(test.lastTerm, test.lastIndex));
      }
    } finally {
      instance.close();
    }
  }
}
