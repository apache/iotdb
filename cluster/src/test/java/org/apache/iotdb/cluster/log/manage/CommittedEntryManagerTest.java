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

import org.apache.iotdb.cluster.exception.EntryCompactedException;
import org.apache.iotdb.cluster.exception.EntryUnavailableException;
import org.apache.iotdb.cluster.exception.TruncateCommittedEntryException;
import org.apache.iotdb.cluster.log.Log;
import org.apache.iotdb.cluster.log.Snapshot;
import org.apache.iotdb.cluster.log.logtypes.EmptyContentLog;
import org.apache.iotdb.cluster.log.snapshot.SimpleSnapshot;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class CommittedEntryManagerTest {

  @Test
  public void applyingSnapshot() {
    class CommittedEntryManagerTester {

      public List<Log> entries;
      public Snapshot snapshot;
      public Snapshot applyingSnapshot;
      public long testIndex;

      public CommittedEntryManagerTester(
          List<Log> entries, Snapshot snapshot, Snapshot applyingSnapshot, long testIndex) {
        this.entries = entries;
        this.snapshot = snapshot;
        this.applyingSnapshot = applyingSnapshot;
        this.testIndex = testIndex;
      }
    }
    List<CommittedEntryManagerTester> tests =
        new ArrayList<CommittedEntryManagerTester>() {
          {
            add(
                new CommittedEntryManagerTester(
                    new ArrayList<Log>() {
                      {
                        add(new EmptyContentLog(3, 3));
                        add(new EmptyContentLog(4, 4));
                        add(new EmptyContentLog(5, 5));
                      }
                    },
                    new SimpleSnapshot(3, 3),
                    new SimpleSnapshot(3, 3),
                    3));
            add(
                new CommittedEntryManagerTester(
                    new ArrayList<Log>() {
                      {
                        add(new EmptyContentLog(3, 3));
                        add(new EmptyContentLog(4, 4));
                        add(new EmptyContentLog(5, 5));
                      }
                    },
                    new SimpleSnapshot(3, 3),
                    new SimpleSnapshot(4, 4),
                    4));
            add(
                new CommittedEntryManagerTester(
                    new ArrayList<Log>() {
                      {
                        add(new EmptyContentLog(3, 3));
                        add(new EmptyContentLog(4, 4));
                        add(new EmptyContentLog(5, 5));
                      }
                    },
                    new SimpleSnapshot(3, 3),
                    new SimpleSnapshot(5, 5),
                    5));
            add(
                new CommittedEntryManagerTester(
                    new ArrayList<Log>() {
                      {
                        add(new EmptyContentLog(3, 3));
                        add(new EmptyContentLog(4, 4));
                        add(new EmptyContentLog(5, 5));
                      }
                    },
                    new SimpleSnapshot(3, 3),
                    new SimpleSnapshot(7, 7),
                    7));
          }
        };
    for (CommittedEntryManagerTester test : tests) {
      CommittedEntryManager instance = new CommittedEntryManager(test.entries);
      instance.applyingSnapshot(test.applyingSnapshot);
      assertEquals(test.testIndex, (long) instance.getDummyIndex());
    }
  }

  @Test
  public void getDummyIndex() {
    class CommittedEntryManagerTester {

      public List<Log> entries;
      public long testIndex;

      public CommittedEntryManagerTester(List<Log> entries, long testIndex) {
        this.entries = entries;
        this.testIndex = testIndex;
      }
    }
    List<CommittedEntryManagerTester> tests =
        new ArrayList<CommittedEntryManagerTester>() {
          {
            add(
                new CommittedEntryManagerTester(
                    new ArrayList<Log>() {
                      {
                        add(new EmptyContentLog(1, 1));
                      }
                    },
                    1));
            add(
                new CommittedEntryManagerTester(
                    new ArrayList<Log>() {
                      {
                        add(new EmptyContentLog(3, 3));
                        add(new EmptyContentLog(4, 4));
                        add(new EmptyContentLog(5, 5));
                      }
                    },
                    3));
          }
        };
    for (CommittedEntryManagerTester test : tests) {
      CommittedEntryManager instance = new CommittedEntryManager(test.entries);
      long index = instance.getDummyIndex();
      assertEquals(test.testIndex, index);
    }
  }

  @Test
  public void getFirstIndex() {
    class CommittedEntryManagerTester {

      public List<Log> entries;
      public long testIndex;

      public CommittedEntryManagerTester(List<Log> entries, long testIndex) {
        this.entries = entries;
        this.testIndex = testIndex;
      }
    }
    List<CommittedEntryManagerTester> tests =
        new ArrayList<CommittedEntryManagerTester>() {
          {
            add(
                new CommittedEntryManagerTester(
                    new ArrayList<Log>() {
                      {
                        add(new EmptyContentLog(1, 1));
                      }
                    },
                    2));
            add(
                new CommittedEntryManagerTester(
                    new ArrayList<Log>() {
                      {
                        add(new EmptyContentLog(3, 3));
                        add(new EmptyContentLog(4, 4));
                        add(new EmptyContentLog(5, 5));
                      }
                    },
                    4));
          }
        };
    for (CommittedEntryManagerTester test : tests) {
      CommittedEntryManager instance = new CommittedEntryManager(test.entries);
      long index = instance.getFirstIndex();
      assertEquals(test.testIndex, index);
    }
  }

  @Test
  public void getLastIndex() {
    class CommittedEntryManagerTester {

      public List<Log> entries;
      public long testIndex;

      public CommittedEntryManagerTester(List<Log> entries, long testIndex) {
        this.entries = entries;
        this.testIndex = testIndex;
      }
    }
    List<CommittedEntryManagerTester> tests =
        new ArrayList<CommittedEntryManagerTester>() {
          {
            add(
                new CommittedEntryManagerTester(
                    new ArrayList<Log>() {
                      {
                        add(new EmptyContentLog(1, 1));
                      }
                    },
                    1));
            add(
                new CommittedEntryManagerTester(
                    new ArrayList<Log>() {
                      {
                        add(new EmptyContentLog(3, 3));
                        add(new EmptyContentLog(4, 4));
                        add(new EmptyContentLog(5, 5));
                      }
                    },
                    5));
          }
        };
    for (CommittedEntryManagerTester test : tests) {
      CommittedEntryManager instance = new CommittedEntryManager(test.entries);
      long index = instance.getLastIndex();
      assertEquals(test.testIndex, index);
    }
  }

  @Test
  public void maybeTerm() {
    class CommittedEntryManagerTester {

      public long index;
      public long testTerm;
      public Class throwClass;

      public CommittedEntryManagerTester(long index, long testTerm, Class throwClass) {
        this.index = index;
        this.testTerm = testTerm;
        this.throwClass = throwClass;
      }
    }
    List<Log> entries =
        new ArrayList<Log>() {
          {
            add(new EmptyContentLog(3, 3));
            add(new EmptyContentLog(4, 4));
            add(new EmptyContentLog(5, 5));
          }
        };
    List<CommittedEntryManagerTester> tests =
        new ArrayList<CommittedEntryManagerTester>() {
          {
            add(new CommittedEntryManagerTester(3, 3, null));
            add(new CommittedEntryManagerTester(4, 4, null));
            add(new CommittedEntryManagerTester(5, 5, null));
            // entries that have been compacted;
            add(new CommittedEntryManagerTester(2, 0, EntryCompactedException.class));
            // entries that have not been committed;
            add(new CommittedEntryManagerTester(6, -1, null));
          }
        };
    for (CommittedEntryManagerTester test : tests) {
      CommittedEntryManager instance = new CommittedEntryManager(entries);
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
  public void getEntries() {
    class CommittedEntryManagerTester {

      public long low;
      public long high;
      public List<Log> testEntries;
      public Class throwClass;

      public CommittedEntryManagerTester(
          long low, long high, List<Log> testEntries, Class throwClass) {
        this.low = low;
        this.high = high;
        this.testEntries = testEntries;
        this.throwClass = throwClass;
      }
    }
    List<Log> entries =
        new ArrayList<Log>() {
          {
            add(new EmptyContentLog(3, 3));
            add(new EmptyContentLog(4, 4));
            add(new EmptyContentLog(5, 5));
            add(new EmptyContentLog(6, 6));
          }
        };
    List<CommittedEntryManagerTester> tests =
        new ArrayList<CommittedEntryManagerTester>() {
          {
            add(new CommittedEntryManagerTester(4, 4, new ArrayList<>(), null));
            add(
                new CommittedEntryManagerTester(
                    4,
                    5,
                    new ArrayList<Log>() {
                      {
                        add(new EmptyContentLog(4, 4));
                      }
                    },
                    null));
            add(
                new CommittedEntryManagerTester(
                    4,
                    6,
                    new ArrayList<Log>() {
                      {
                        add(new EmptyContentLog(4, 4));
                        add(new EmptyContentLog(5, 5));
                      }
                    },
                    null));
            add(
                new CommittedEntryManagerTester(
                    4,
                    7,
                    new ArrayList<Log>() {
                      {
                        add(new EmptyContentLog(4, 4));
                        add(new EmptyContentLog(5, 5));
                        add(new EmptyContentLog(6, 6));
                      }
                    },
                    null));
            // entries that have not been committed;
            add(
                new CommittedEntryManagerTester(
                    4,
                    8,
                    new ArrayList<Log>() {
                      {
                        add(new EmptyContentLog(4, 4));
                        add(new EmptyContentLog(5, 5));
                        add(new EmptyContentLog(6, 6));
                      }
                    },
                    null));
            // entries that have been compacted;
            add(new CommittedEntryManagerTester(2, 6, entries.subList(1, 3), null));
            add(new CommittedEntryManagerTester(3, 4, Collections.EMPTY_LIST, null));
            // illegal range
            add(new CommittedEntryManagerTester(5, 4, Collections.EMPTY_LIST, null));
          }
        };
    for (CommittedEntryManagerTester test : tests) {
      CommittedEntryManager instance = new CommittedEntryManager(entries);
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
  }

  @Test
  public void compactEntries() {
    class CommittedEntryManagerTester {

      public List<Log> entries;
      public long compactIndex;
      public List<Log> testEntries;
      public Class throwClass;

      public CommittedEntryManagerTester(
          List<Log> entries, long compactIndex, List<Log> testEntries, Class throwClass) {
        this.entries = entries;
        this.compactIndex = compactIndex;
        this.testEntries = testEntries;
        this.throwClass = throwClass;
      }
    }
    List<CommittedEntryManagerTester> tests =
        new ArrayList<CommittedEntryManagerTester>() {
          {
            add(
                new CommittedEntryManagerTester(
                    new ArrayList<Log>() {
                      {
                        add(new EmptyContentLog(3, 3));
                        add(new EmptyContentLog(4, 4));
                        add(new EmptyContentLog(5, 5));
                      }
                    },
                    2,
                    new ArrayList<Log>() {
                      {
                        add(new EmptyContentLog(3, 3));
                        add(new EmptyContentLog(4, 4));
                        add(new EmptyContentLog(5, 5));
                      }
                    },
                    null));
            add(
                new CommittedEntryManagerTester(
                    new ArrayList<Log>() {
                      {
                        add(new EmptyContentLog(3, 3));
                        add(new EmptyContentLog(4, 4));
                        add(new EmptyContentLog(5, 5));
                      }
                    },
                    3,
                    new ArrayList<Log>() {
                      {
                        add(new EmptyContentLog(3, 3));
                        add(new EmptyContentLog(4, 4));
                        add(new EmptyContentLog(5, 5));
                      }
                    },
                    null));
            add(
                new CommittedEntryManagerTester(
                    new ArrayList<Log>() {
                      {
                        add(new EmptyContentLog(3, 3));
                        add(new EmptyContentLog(4, 4));
                        add(new EmptyContentLog(5, 5));
                      }
                    },
                    4,
                    new ArrayList<Log>() {
                      {
                        add(new EmptyContentLog(4, 4));
                        add(new EmptyContentLog(5, 5));
                      }
                    },
                    null));
            add(
                new CommittedEntryManagerTester(
                    new ArrayList<Log>() {
                      {
                        add(new EmptyContentLog(3, 3));
                        add(new EmptyContentLog(4, 4));
                        add(new EmptyContentLog(5, 5));
                      }
                    },
                    5,
                    new ArrayList<Log>() {
                      {
                        add(new EmptyContentLog(5, 5));
                      }
                    },
                    null));
            add(
                new CommittedEntryManagerTester(
                    new ArrayList<Log>() {
                      {
                        add(new EmptyContentLog(3, 3));
                        add(new EmptyContentLog(4, 4));
                        add(new EmptyContentLog(5, 5));
                      }
                    },
                    6,
                    null,
                    EntryUnavailableException.class));
            add(
                new CommittedEntryManagerTester(
                    new ArrayList<Log>() {
                      {
                        add(new EmptyContentLog(3, 3));
                        add(new EmptyContentLog(4, 4));
                        add(new EmptyContentLog(5, 5));
                      }
                    },
                    10,
                    null,
                    EntryUnavailableException.class));
          }
        };
    for (CommittedEntryManagerTester test : tests) {
      CommittedEntryManager instance = new CommittedEntryManager(test.entries);
      try {
        instance.compactEntries(test.compactIndex);
        if (test.throwClass != null) {
          fail("The expected exception is not thrown");
        } else {
          assertEquals(test.testEntries, instance.getAllEntries());
        }
      } catch (Exception e) {
        if (!e.getClass().getName().equals(test.throwClass.getName())) {
          fail("An unexpected exception was thrown.");
        }
      }
    }
  }

  @Test
  public void append() {
    class CommittedEntryManagerTester {

      public List<Log> entries;
      public List<Log> toAppend;
      public List<Log> testEntries;
      public Class throwClass;

      public CommittedEntryManagerTester(
          List<Log> entries, List<Log> toAppend, List<Log> testEntries, Class throwClass) {
        this.entries = entries;
        this.toAppend = toAppend;
        this.testEntries = testEntries;
        this.throwClass = throwClass;
      }
    }
    List<CommittedEntryManagerTester> tests =
        new ArrayList<CommittedEntryManagerTester>() {
          {
            add(
                new CommittedEntryManagerTester(
                    new ArrayList<Log>() {
                      {
                        add(new EmptyContentLog(3, 3));
                        add(new EmptyContentLog(4, 4));
                        add(new EmptyContentLog(5, 5));
                      }
                    },
                    new ArrayList<Log>() {
                      {
                        add(new EmptyContentLog(3, 3));
                        add(new EmptyContentLog(4, 4));
                        add(new EmptyContentLog(5, 5));
                      }
                    },
                    null,
                    TruncateCommittedEntryException.class));
            add(
                new CommittedEntryManagerTester(
                    new ArrayList<Log>() {
                      {
                        add(new EmptyContentLog(3, 3));
                        add(new EmptyContentLog(4, 4));
                        add(new EmptyContentLog(5, 5));
                      }
                    },
                    new ArrayList<Log>() {
                      {
                        add(new EmptyContentLog(3, 3));
                        add(new EmptyContentLog(4, 6));
                        add(new EmptyContentLog(5, 6));
                      }
                    },
                    null,
                    TruncateCommittedEntryException.class));
            // direct append
            add(
                new CommittedEntryManagerTester(
                    new ArrayList<Log>() {
                      {
                        add(new EmptyContentLog(3, 3));
                        add(new EmptyContentLog(4, 4));
                        add(new EmptyContentLog(5, 5));
                      }
                    },
                    new ArrayList<Log>() {
                      {
                        add(new EmptyContentLog(6, 5));
                      }
                    },
                    new ArrayList<Log>() {
                      {
                        add(new EmptyContentLog(3, 3));
                        add(new EmptyContentLog(4, 4));
                        add(new EmptyContentLog(5, 5));
                        add(new EmptyContentLog(6, 5));
                      }
                    },
                    null));
          }
        };
    for (CommittedEntryManagerTester test : tests) {
      CommittedEntryManager instance = new CommittedEntryManager(test.entries);
      try {
        instance.append(test.toAppend);
        if (test.throwClass != null) {
          fail("The expected exception is not thrown");
        } else {
          assertEquals(test.testEntries, instance.getAllEntries());
        }
      } catch (Exception e) {
        if (!e.getClass().getName().equals(test.throwClass.getName())) {
          fail("An unexpected exception was thrown.");
        }
      }
    }
  }
}
