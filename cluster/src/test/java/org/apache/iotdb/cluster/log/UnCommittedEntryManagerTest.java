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

package org.apache.iotdb.cluster.log;

import org.apache.iotdb.cluster.exception.EntryUnavailableException;
import org.apache.iotdb.cluster.log.logtypes.PhysicalPlanLog;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class UnCommittedEntryManagerTest {

  static class UnCommitEntryManagerTesterBase {
    public List<Log> entries;
    public long offset;

    public UnCommitEntryManagerTesterBase(List<Log> entries, long offset) {
      this.entries = entries;
      this.offset = offset;
    }
  }

  @Test
  public void getFirstUnCommittedIndex() {
    class UnCommittedEntryManagerTester extends UnCommitEntryManagerTesterBase {
      public long testOffset;

      public UnCommittedEntryManagerTester(List<Log> entries, long offset, long testOffset) {
        super(entries, offset);
        this.testOffset = testOffset;
      }
    }
    List<UnCommittedEntryManagerTester> tests = new ArrayList<UnCommittedEntryManagerTester>() {{
      add(new UnCommittedEntryManagerTester(new ArrayList<Log>() {{
      }}, 0, 0));
      add(new UnCommittedEntryManagerTester(new ArrayList<Log>() {{
      }}, 5, 5));
      add(new UnCommittedEntryManagerTester(new ArrayList<Log>() {{
        add(new PhysicalPlanLog(6, 1));
      }}, 5, 5));
    }};
    for (UnCommittedEntryManagerTester test : tests) {
      UnCommittedEntryManager instance = new UnCommittedEntryManager(test.offset, test.entries);
      long index = instance.getFirstUnCommittedIndex();
      assertEquals(test.testOffset, index);
    }
  }

  @Test
  public void maybeLastIndex() {
    class UnCommittedEntryManagerTester extends UnCommitEntryManagerTesterBase {
      public long testIndex;

      public UnCommittedEntryManagerTester(List<Log> entries, long offset, long testIndex) {
        super(entries, offset);
        this.testIndex = testIndex;
      }
    }
    List<UnCommittedEntryManagerTester> tests = new ArrayList<UnCommittedEntryManagerTester>() {{
      add(new UnCommittedEntryManagerTester(new ArrayList<Log>() {{
      }}, 0, -1));
      add(new UnCommittedEntryManagerTester(new ArrayList<Log>() {{
        add(new PhysicalPlanLog(5, 1));
      }}, 5, 5));
      add(new UnCommittedEntryManagerTester(new ArrayList<Log>() {{
        add(new PhysicalPlanLog(5, 1));
        add(new PhysicalPlanLog(6, 1));
      }}, 5, 6));
    }};
    for (UnCommittedEntryManagerTester test : tests) {
      UnCommittedEntryManager instance = new UnCommittedEntryManager(test.offset, test.entries);
      long index = instance.maybeLastIndex();
      assertEquals(test.testIndex, index);
    }
  }

  @Test
  public void maybeTerm() {
    class UnCommittedEntryManagerTester extends UnCommitEntryManagerTesterBase {
      public long index;
      public long testTerm;
      public Class throwClass;

      public UnCommittedEntryManagerTester(List<Log> entries, long offset, long index, long testTerm, Class throwClass) {
        super(entries, offset);
        this.index = index;
        this.testTerm = testTerm;
        this.throwClass = throwClass;
      }
    }
    List<UnCommittedEntryManagerTester> tests = new ArrayList<UnCommittedEntryManagerTester>() {{
      add(new UnCommittedEntryManagerTester(new ArrayList<Log>() {{
        add(new PhysicalPlanLog(5, 1));
      }}, 5, 5, 1, null));
      add(new UnCommittedEntryManagerTester(new ArrayList<Log>() {{
        add(new PhysicalPlanLog(5, 1));
      }}, 5, 4, -1, null));
      add(new UnCommittedEntryManagerTester(new ArrayList<Log>() {{
        add(new PhysicalPlanLog(5, 1));
        add(new PhysicalPlanLog(6, 4));
      }}, 5, 5, 1, null));
      add(new UnCommittedEntryManagerTester(new ArrayList<Log>() {{
        add(new PhysicalPlanLog(5, 1));
        add(new PhysicalPlanLog(6, 4));
      }}, 5, 6, 4, null));
      // entries that have been committed;
      add(new UnCommittedEntryManagerTester(new ArrayList<Log>() {{
        add(new PhysicalPlanLog(5, 1));
        add(new PhysicalPlanLog(6, 4));
      }}, 5, 4, -1, null));
      // entries which are unavailable.
      add(new UnCommittedEntryManagerTester(new ArrayList<Log>() {{
      }}, 0, 0, -1, EntryUnavailableException.class));
      add(new UnCommittedEntryManagerTester(new ArrayList<Log>() {{
        add(new PhysicalPlanLog(5, 1));
      }}, 5, 6, -1, EntryUnavailableException.class));
    }};
    for (UnCommittedEntryManagerTester test : tests) {
      UnCommittedEntryManager instance = new UnCommittedEntryManager(test.offset, test.entries);
      try {
        long term = instance.maybeTerm(test.index);
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
  }

  @Test
  public void stableTo() {
    class UnCommittedEntryManagerTester extends UnCommitEntryManagerTesterBase {
      public long index;
      public long term;
      public long testOffset;
      public long testLen;

      public UnCommittedEntryManagerTester(List<Log> entries, long offset, long index, long term, long testOffset, long testLen) {
        super(entries, offset);
        this.index = index;
        this.term = term;
        this.testOffset = testOffset;
        this.testLen = testLen;
      }
    }
    List<UnCommittedEntryManagerTester> tests = new ArrayList<UnCommittedEntryManagerTester>() {{
      // empty entries
      add(new UnCommittedEntryManagerTester(new ArrayList<Log>() {{
      }}, 0, 5, 1, 0, 0));
      // stable to the first entry
      add(new UnCommittedEntryManagerTester(new ArrayList<Log>() {{
        add(new PhysicalPlanLog(5, 1));
      }}, 5, 5, 1, 6, 0));
      // stable to the first entry
      add(new UnCommittedEntryManagerTester(new ArrayList<Log>() {{
        add(new PhysicalPlanLog(5, 1));
        add(new PhysicalPlanLog(6, 1));
      }}, 5, 5, 1, 6, 1));
      // stable to the first entry and term mismatch
      add(new UnCommittedEntryManagerTester(new ArrayList<Log>() {{
        add(new PhysicalPlanLog(6, 2));
      }}, 6, 6, 1, 6, 1));
      // stable to old entry
      add(new UnCommittedEntryManagerTester(new ArrayList<Log>() {{
        add(new PhysicalPlanLog(5, 1));
      }}, 5, 4, 1, 5, 1));
      // stable to old entry
      add(new UnCommittedEntryManagerTester(new ArrayList<Log>() {{
        add(new PhysicalPlanLog(5, 1));
      }}, 5, 2, 2, 5, 1));
    }};
    for (UnCommittedEntryManagerTester test : tests) {
      UnCommittedEntryManager instance = new UnCommittedEntryManager(test.offset, test.entries);
      instance.stableTo(test.index, test.term);
      assertEquals(test.testOffset, instance.getFirstUnCommittedIndex());
      assertEquals(test.testLen, instance.getAllEntries().size());
    }
  }

  @Test
  public void applyingSnapshot() {
    class UnCommittedEntryManagerTester extends UnCommitEntryManagerTesterBase {
      public RaftSnapshot snapshot;
      public long testOffset;

      public UnCommittedEntryManagerTester(List<Log> entries, long offset, RaftSnapshot snapshot, long testOffset) {
        super(entries, offset);
        this.snapshot = snapshot;
        this.testOffset = testOffset;
      }
    }
    List<UnCommittedEntryManagerTester> tests = new ArrayList<UnCommittedEntryManagerTester>() {{
      // empty entries
      add(new UnCommittedEntryManagerTester(new ArrayList<>(), 5, new RaftSnapshot(new SnapshotMeta(6, 6)), 7));
      // normal case
      add(new UnCommittedEntryManagerTester(new ArrayList<Log>() {{
        add(new PhysicalPlanLog(5, 1));
      }}, 5, new RaftSnapshot(new SnapshotMeta(20, 20)), 21));
    }};
    for (UnCommittedEntryManagerTester test : tests) {
      UnCommittedEntryManager instance = new UnCommittedEntryManager(test.offset, test.entries);
      instance.applyingSnapshot(test.snapshot);
      assertEquals(test.testOffset, instance.getFirstUnCommittedIndex());
      assertEquals(0, instance.getAllEntries().size());
    }
  }

  @Test
  public void truncateAndAppend() {
    class UnCommittedEntryManagerTester extends UnCommitEntryManagerTesterBase {
      public List<Log> toAppend;
      public long testOffset;
      public List<Log> testEntries;

      public UnCommittedEntryManagerTester(List<Log> entries, long offset, List<Log> toAppend, long testOffset, List<Log> testEntries) {
        super(entries, offset);
        this.toAppend = toAppend;
        this.testOffset = testOffset;
        this.testEntries = testEntries;
      }
    }
    List<UnCommittedEntryManagerTester> tests = new ArrayList<UnCommittedEntryManagerTester>() {{
      // append to the end
      add(new UnCommittedEntryManagerTester(new ArrayList<Log>() {{
        add(new PhysicalPlanLog(5, 1));
      }}, 5,
              new ArrayList<Log>() {{
                add(new PhysicalPlanLog(6, 1));
                add(new PhysicalPlanLog(7, 1));
              }}
              , 5,
              new ArrayList<Log>() {{
                add(new PhysicalPlanLog(5, 1));
                add(new PhysicalPlanLog(6, 1));
                add(new PhysicalPlanLog(7, 1));
              }}));
      // replace the uncommitted entries
      add(new UnCommittedEntryManagerTester(new ArrayList<Log>() {{
        add(new PhysicalPlanLog(5, 1));
      }}, 5,
              new ArrayList<Log>() {{
                add(new PhysicalPlanLog(5, 2));
                add(new PhysicalPlanLog(6, 2));
              }}
              , 5,
              new ArrayList<Log>() {{
                add(new PhysicalPlanLog(5, 2));
                add(new PhysicalPlanLog(6, 2));
              }}));
      add(new UnCommittedEntryManagerTester(new ArrayList<Log>() {{
        add(new PhysicalPlanLog(5, 1));
      }}, 5,
              new ArrayList<Log>() {{
                add(new PhysicalPlanLog(4, 2));
                add(new PhysicalPlanLog(5, 2));
                add(new PhysicalPlanLog(6, 2));
              }}
              , 4,
              new ArrayList<Log>() {{
                add(new PhysicalPlanLog(4, 2));
                add(new PhysicalPlanLog(5, 2));
                add(new PhysicalPlanLog(6, 2));
              }}));
      // truncate the existing entries and append
      add(new UnCommittedEntryManagerTester(new ArrayList<Log>() {{
        add(new PhysicalPlanLog(5, 1));
        add(new PhysicalPlanLog(6, 1));
        add(new PhysicalPlanLog(7, 1));
      }}, 5,
              new ArrayList<Log>() {{
                add(new PhysicalPlanLog(6, 2));
              }}
              , 5,
              new ArrayList<Log>() {{
                add(new PhysicalPlanLog(5, 1));
                add(new PhysicalPlanLog(6, 2));
              }}));
      add(new UnCommittedEntryManagerTester(new ArrayList<Log>() {{
        add(new PhysicalPlanLog(5, 1));
        add(new PhysicalPlanLog(6, 1));
        add(new PhysicalPlanLog(7, 1));
      }}, 5,
              new ArrayList<Log>() {{
                add(new PhysicalPlanLog(7, 2));
                add(new PhysicalPlanLog(8, 2));
              }}
              , 5,
              new ArrayList<Log>() {{
                add(new PhysicalPlanLog(5, 1));
                add(new PhysicalPlanLog(6, 1));
                add(new PhysicalPlanLog(7, 2));
                add(new PhysicalPlanLog(8, 2));
              }}));
    }};
    for (UnCommittedEntryManagerTester test : tests) {
      UnCommittedEntryManager instance = new UnCommittedEntryManager(test.offset, test.entries);
      instance.truncateAndAppend(test.toAppend);
      assertEquals(test.testOffset, instance.getFirstUnCommittedIndex());
      assertEquals(test.testEntries, instance.getAllEntries());
    }
  }

  @Test
  public void getEntries() {
    class UnCommittedEntryManagerTester {
      public long low;
      public long high;
      public List<Log> testEntries;

      public UnCommittedEntryManagerTester(long low, long high, List<Log> testEntries) {
        this.low = low;
        this.high = high;
        this.testEntries = testEntries;
      }
    }
    long offset = 100;
    long num = 100;
    long last = offset + num;
    List<Log> entries = new ArrayList<>();
    for (int i = 0; i < num; i++) {
      entries.add(new PhysicalPlanLog(offset + i, offset + i));
    }
    UnCommittedEntryManager instance = new UnCommittedEntryManager(offset, entries);
    List<UnCommittedEntryManagerTester> tests = new ArrayList<UnCommittedEntryManagerTester>() {{
      add(new UnCommittedEntryManagerTester(offset, offset + num, entries));
      add(new UnCommittedEntryManagerTester(offset - 1, offset + 1, new ArrayList<Log>() {{
        add(new PhysicalPlanLog(offset, offset));
      }}));
      add(new UnCommittedEntryManagerTester(offset, offset + 1, new ArrayList<Log>() {{
        add(new PhysicalPlanLog(offset, offset));
      }}));
      add(new UnCommittedEntryManagerTester(last - 1, last, new ArrayList<Log>() {{
        add(new PhysicalPlanLog(last - 1, last - 1));
      }}));
      add(new UnCommittedEntryManagerTester(last - 1, last + 1, new ArrayList<Log>() {{
        add(new PhysicalPlanLog(last - 1, last - 1));
      }}));
      add(new UnCommittedEntryManagerTester(offset, offset, new ArrayList<>()));
      add(new UnCommittedEntryManagerTester(last, last + 1, new ArrayList<>()));
      add(new UnCommittedEntryManagerTester(last + 1, last + 1, new ArrayList<>()));
    }};
    for (UnCommittedEntryManagerTester test : tests) {
      List<Log> answer = instance.getEntries(test.low, test.high);
      assertEquals(test.testEntries, answer);
    }
  }
}