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

import org.apache.iotdb.cluster.exception.EntryCompactedException;
import org.apache.iotdb.cluster.exception.EntryUnavailableException;
import org.apache.iotdb.cluster.log.logtypes.PhysicalPlanLog;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class CommittedEntryManagerTest {

	@Test
	public void applyingSnapshot() {
		class CommittedEntryManagerTester {
			public List<Log> entries;
			public RaftSnapshot snapshot;
			public RaftSnapshot applyingSnapshot;
			public long testIndex;

			public CommittedEntryManagerTester(List<Log> entries, RaftSnapshot snapshot, RaftSnapshot applyingSnapshot, long testIndex) {
				this.entries = entries;
				this.snapshot = snapshot;
				this.applyingSnapshot = applyingSnapshot;
				this.testIndex = testIndex;
			}
		}
		List<CommittedEntryManagerTester> tests = new ArrayList<CommittedEntryManagerTester>() {{
			add(new CommittedEntryManagerTester(new ArrayList<Log>() {{
				add(new PhysicalPlanLog(3, 3));
				add(new PhysicalPlanLog(4, 4));
				add(new PhysicalPlanLog(5, 5));
			}}, new RaftSnapshot(new SnapshotMeta(3, 3)), new RaftSnapshot(new SnapshotMeta(3, 3)), 3));
			add(new CommittedEntryManagerTester(new ArrayList<Log>() {{
				add(new PhysicalPlanLog(3, 3));
				add(new PhysicalPlanLog(4, 4));
				add(new PhysicalPlanLog(5, 5));
			}}, new RaftSnapshot(new SnapshotMeta(3, 3)), new RaftSnapshot(new SnapshotMeta(4, 4)), 4));
			add(new CommittedEntryManagerTester(new ArrayList<Log>() {{
				add(new PhysicalPlanLog(3, 3));
				add(new PhysicalPlanLog(4, 4));
				add(new PhysicalPlanLog(5, 5));
			}}, new RaftSnapshot(new SnapshotMeta(3, 3)), new RaftSnapshot(new SnapshotMeta(5, 5)), 5));
			add(new CommittedEntryManagerTester(new ArrayList<Log>() {{
				add(new PhysicalPlanLog(3, 3));
				add(new PhysicalPlanLog(4, 4));
				add(new PhysicalPlanLog(5, 5));
			}}, new RaftSnapshot(new SnapshotMeta(3, 3)), new RaftSnapshot(new SnapshotMeta(7, 7)), 7));
		}};
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
		List<CommittedEntryManagerTester> tests = new ArrayList<CommittedEntryManagerTester>() {{
			add(new CommittedEntryManagerTester(new ArrayList<Log>() {{
				add(new PhysicalPlanLog(1, 1));
			}}, 1));
			add(new CommittedEntryManagerTester(new ArrayList<Log>() {{
				add(new PhysicalPlanLog(3, 3));
				add(new PhysicalPlanLog(4, 4));
				add(new PhysicalPlanLog(5, 5));
			}}, 3));
		}};
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
		List<CommittedEntryManagerTester> tests = new ArrayList<CommittedEntryManagerTester>() {{
			add(new CommittedEntryManagerTester(new ArrayList<Log>() {{
				add(new PhysicalPlanLog(1, 1));
			}}, 2));
			add(new CommittedEntryManagerTester(new ArrayList<Log>() {{
				add(new PhysicalPlanLog(3, 3));
				add(new PhysicalPlanLog(4, 4));
				add(new PhysicalPlanLog(5, 5));
			}}, 4));
		}};
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
		List<CommittedEntryManagerTester> tests = new ArrayList<CommittedEntryManagerTester>() {{
			add(new CommittedEntryManagerTester(new ArrayList<Log>() {{
				add(new PhysicalPlanLog(1, 1));
			}}, 1));
			add(new CommittedEntryManagerTester(new ArrayList<Log>() {{
				add(new PhysicalPlanLog(3, 3));
				add(new PhysicalPlanLog(4, 4));
				add(new PhysicalPlanLog(5, 5));
			}}, 5));
		}};
		for (CommittedEntryManagerTester test : tests) {
			CommittedEntryManager instance = new CommittedEntryManager(test.entries);
			long index = instance.getLastIndex();
			assertEquals(test.testIndex, index);
		}
	}

	@Test
	public void maybeTerm() {
		class CommittedEntryManagerTester {
			public List<Log> entries;
			public long index;
			public long testTerm;

			public CommittedEntryManagerTester(List<Log> entries, long index, long testTerm) {
				this.entries = entries;
				this.index = index;
				this.testTerm = testTerm;
			}
		}
		List<Log> entries = new ArrayList<Log>() {{
			add(new PhysicalPlanLog(3, 3));
			add(new PhysicalPlanLog(4, 4));
			add(new PhysicalPlanLog(5, 5));
		}};
		List<CommittedEntryManagerTester> tests = new ArrayList<CommittedEntryManagerTester>() {{
			add(new CommittedEntryManagerTester(entries, 2, -1));
			add(new CommittedEntryManagerTester(entries, 3, 3));
			add(new CommittedEntryManagerTester(entries, 4, 4));
			add(new CommittedEntryManagerTester(entries, 5, 5));
			add(new CommittedEntryManagerTester(entries, 6, -1));
		}};
		for (CommittedEntryManagerTester test : tests) {
			CommittedEntryManager instance = new CommittedEntryManager(test.entries);
			long term = instance.maybeTerm(test.index);
			assertEquals(test.testTerm, term);
		}
	}

	@Test
	public void getEntries() {
		class CommittedEntryManagerTester {
			public List<Log> entries;
			public long low;
			public long high;
			public List<Log> testEntries;
			public Class throwClass;

			public CommittedEntryManagerTester(List<Log> entries, long low, long high, List<Log> testEntries, Class throwClass) {
				this.entries = entries;
				this.low = low;
				this.high = high;
				this.testEntries = testEntries;
				this.throwClass = throwClass;
			}
		}
		List<Log> entries = new ArrayList<Log>() {{
			add(new PhysicalPlanLog(3, 3));
			add(new PhysicalPlanLog(4, 4));
			add(new PhysicalPlanLog(5, 5));
			add(new PhysicalPlanLog(6, 6));
		}};
		List<CommittedEntryManagerTester> tests = new ArrayList<CommittedEntryManagerTester>() {{
			add(new CommittedEntryManagerTester(entries, 4, 5, new ArrayList<Log>() {{
				add(new PhysicalPlanLog(4, 4));
			}}, null));
			add(new CommittedEntryManagerTester(entries, 4, 6, new ArrayList<Log>() {{
				add(new PhysicalPlanLog(4, 4));
				add(new PhysicalPlanLog(5, 5));
			}}, null));
			add(new CommittedEntryManagerTester(entries, 4, 7, new ArrayList<Log>() {{
				add(new PhysicalPlanLog(4, 4));
				add(new PhysicalPlanLog(5, 5));
				add(new PhysicalPlanLog(6, 6));
			}}, null));
			add(new CommittedEntryManagerTester(entries, 4, 8, new ArrayList<Log>() {{
				add(new PhysicalPlanLog(4, 4));
				add(new PhysicalPlanLog(5, 5));
				add(new PhysicalPlanLog(6, 6));
			}}, null));
			add(new CommittedEntryManagerTester(entries, 2, 6, null, EntryCompactedException.class));
			add(new CommittedEntryManagerTester(entries, 3, 4, null, EntryCompactedException.class));
		}};
		for (CommittedEntryManagerTester test : tests) {
			CommittedEntryManager instance = new CommittedEntryManager(test.entries);
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

			public CommittedEntryManagerTester(List<Log> entries, long compactIndex, List<Log> testEntries, Class throwClass) {
				this.entries = entries;
				this.compactIndex = compactIndex;
				this.testEntries = testEntries;
				this.throwClass = throwClass;
			}
		}
		List<Log> entries = new ArrayList<Log>() {{
			add(new PhysicalPlanLog(3, 3));
			add(new PhysicalPlanLog(4, 4));
			add(new PhysicalPlanLog(5, 5));
		}};
		List<CommittedEntryManagerTester> tests = new ArrayList<CommittedEntryManagerTester>() {{
			add(new CommittedEntryManagerTester(entries, 2, entries, null));
			add(new CommittedEntryManagerTester(entries, 3, entries, null));
			add(new CommittedEntryManagerTester(entries, 4, new ArrayList<Log>() {{
				add(new PhysicalPlanLog(4, 4));
				add(new PhysicalPlanLog(5, 5));
			}}, null));
			add(new CommittedEntryManagerTester(entries, 5, new ArrayList<Log>() {{
				add(new PhysicalPlanLog(5, 5));
			}}, null));
			add(new CommittedEntryManagerTester(entries, 6, null, EntryUnavailableException.class));
			add(new CommittedEntryManagerTester(entries, 10, null, EntryUnavailableException.class));
		}};
		for (CommittedEntryManagerTester test : tests) {
			CommittedEntryManager instance = new CommittedEntryManager(test.entries);
			try {
				instance.compactEntries(test.compactIndex);
				if (test.throwClass != null) {
					fail("The expected exception is not thrown");
				} else {
					assertEquals(test.testEntries, test.entries);
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

			public CommittedEntryManagerTester(List<Log> entries, List<Log> toAppend, List<Log> testEntries) {
				this.entries = entries;
				this.toAppend = toAppend;
				this.testEntries = testEntries;
			}
		}
		List<CommittedEntryManagerTester> tests = new ArrayList<CommittedEntryManagerTester>() {{
			add(new CommittedEntryManagerTester(new ArrayList<Log>() {{
				add(new PhysicalPlanLog(3, 3));
				add(new PhysicalPlanLog(4, 4));
				add(new PhysicalPlanLog(5, 5));
			}}, new ArrayList<Log>() {{
				add(new PhysicalPlanLog(1, 1));
				add(new PhysicalPlanLog(2, 2));
			}}, new ArrayList<Log>() {{
				add(new PhysicalPlanLog(3, 3));
				add(new PhysicalPlanLog(4, 4));
				add(new PhysicalPlanLog(5, 5));
			}}));
			add(new CommittedEntryManagerTester(new ArrayList<Log>() {{
				add(new PhysicalPlanLog(3, 3));
				add(new PhysicalPlanLog(4, 4));
				add(new PhysicalPlanLog(5, 5));
			}}, new ArrayList<Log>() {{
				add(new PhysicalPlanLog(3, 3));
				add(new PhysicalPlanLog(4, 4));
				add(new PhysicalPlanLog(5, 5));
			}}, new ArrayList<Log>() {{
				add(new PhysicalPlanLog(3, 3));
				add(new PhysicalPlanLog(4, 4));
				add(new PhysicalPlanLog(5, 5));
			}}));
			add(new CommittedEntryManagerTester(new ArrayList<Log>() {{
				add(new PhysicalPlanLog(3, 3));
				add(new PhysicalPlanLog(4, 4));
				add(new PhysicalPlanLog(5, 5));
			}}, new ArrayList<Log>() {{
				add(new PhysicalPlanLog(3, 3));
				add(new PhysicalPlanLog(4, 6));
				add(new PhysicalPlanLog(5, 6));
			}}, new ArrayList<Log>() {{
				add(new PhysicalPlanLog(3, 3));
				add(new PhysicalPlanLog(4, 6));
				add(new PhysicalPlanLog(5, 6));
			}}));
			add(new CommittedEntryManagerTester(new ArrayList<Log>() {{
				add(new PhysicalPlanLog(3, 3));
				add(new PhysicalPlanLog(4, 4));
				add(new PhysicalPlanLog(5, 5));
			}}, new ArrayList<Log>() {{
				add(new PhysicalPlanLog(3, 3));
				add(new PhysicalPlanLog(4, 4));
				add(new PhysicalPlanLog(5, 5));
				add(new PhysicalPlanLog(6, 5));
			}}, new ArrayList<Log>() {{
				add(new PhysicalPlanLog(3, 3));
				add(new PhysicalPlanLog(4, 4));
				add(new PhysicalPlanLog(5, 5));
				add(new PhysicalPlanLog(6, 5));
			}}));
			// truncate incoming entries, truncate the existing entries and append
			add(new CommittedEntryManagerTester(new ArrayList<Log>() {{
				add(new PhysicalPlanLog(3, 3));
				add(new PhysicalPlanLog(4, 4));
				add(new PhysicalPlanLog(5, 5));
			}}, new ArrayList<Log>() {{
				add(new PhysicalPlanLog(2, 3));
				add(new PhysicalPlanLog(3, 3));
				add(new PhysicalPlanLog(4, 5));
			}}, new ArrayList<Log>() {{
				add(new PhysicalPlanLog(3, 3));
				add(new PhysicalPlanLog(4, 5));
			}}));
			// truncate the existing entries and append
			add(new CommittedEntryManagerTester(new ArrayList<Log>() {{
				add(new PhysicalPlanLog(3, 3));
				add(new PhysicalPlanLog(4, 4));
				add(new PhysicalPlanLog(5, 5));
			}}, new ArrayList<Log>() {{
				add(new PhysicalPlanLog(4, 5));
			}}, new ArrayList<Log>() {{
				add(new PhysicalPlanLog(3, 3));
				add(new PhysicalPlanLog(4, 5));
			}}));
			// direct append
			add(new CommittedEntryManagerTester(new ArrayList<Log>() {{
				add(new PhysicalPlanLog(3, 3));
				add(new PhysicalPlanLog(4, 4));
				add(new PhysicalPlanLog(5, 5));
			}}, new ArrayList<Log>() {{
				add(new PhysicalPlanLog(6, 5));
			}}, new ArrayList<Log>() {{
				add(new PhysicalPlanLog(3, 3));
				add(new PhysicalPlanLog(4, 4));
				add(new PhysicalPlanLog(5, 5));
				add(new PhysicalPlanLog(6, 5));
			}}));
		}};
		for (CommittedEntryManagerTester test : tests) {
			CommittedEntryManager instance = new CommittedEntryManager(test.entries);
			instance.append(test.toAppend);
			assertEquals(test.testEntries, instance.getAllEntries());
		}
	}
}