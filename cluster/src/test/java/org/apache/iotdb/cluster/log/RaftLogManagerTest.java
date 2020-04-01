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

import org.apache.iotdb.cluster.exception.TruncateCommittedEntryException;
import org.apache.iotdb.cluster.log.logtypes.PhysicalPlanLog;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class RaftLogManagerTest {

	@Test
	public void getTerm() {
	}

	@Test
	public void getFirstIndex() {
	}

	@Test
	public void getLastIndex() {
	}

	@Test
	public void getLastTerm() {
	}

	@Test
	public void maybeAppend() {
	}

	@Test
	public void append() {
		class RaftLogManagerTester {
			public List<Log> previousEntries;
			public List<Log> appendingEntries;
			public long testLastIndexAfterAppend;
			public List<Log> testEntries;
			public long testOffset;

			public RaftLogManagerTester(List<Log> previousEntries,List<Log> appendingEntries,long testLastIndexAfterAppend,List<Log> testEntries, long testOffset) {
				this.previousEntries = previousEntries;
				this.appendingEntries = appendingEntries;
				this.testLastIndexAfterAppend = testLastIndexAfterAppend;
				this.testEntries = testEntries;
				this.testOffset = testOffset;
			}
		}
		List<RaftLogManagerTester> tests = new ArrayList<RaftLogManagerTester>() {{
			add(new RaftLogManagerTester(new ArrayList<Log>() {{
				add(new PhysicalPlanLog(1, 1));
				add(new PhysicalPlanLog(2, 2));
			}},new ArrayList<>(),2,new ArrayList<Log>() {{
				add(new PhysicalPlanLog(1, 1));
				add(new PhysicalPlanLog(2, 2));
			}},3));
			add(new RaftLogManagerTester(new ArrayList<Log>() {{
				add(new PhysicalPlanLog(1, 1));
				add(new PhysicalPlanLog(2, 2));
			}},new ArrayList<Log>(){{
				add(new PhysicalPlanLog(3, 2));
			}},3,new ArrayList<Log>() {{
				add(new PhysicalPlanLog(1, 1));
				add(new PhysicalPlanLog(2, 2));
				add(new PhysicalPlanLog(3, 2));
			}},3));
			// conflicts with index 1
			add(new RaftLogManagerTester(new ArrayList<Log>() {{
				add(new PhysicalPlanLog(1, 1));
				add(new PhysicalPlanLog(2, 2));
			}},new ArrayList<Log>(){{
				add(new PhysicalPlanLog(1, 2));
			}},1,new ArrayList<Log>() {{
				add(new PhysicalPlanLog(1, 2));
			}},1));
			add(new RaftLogManagerTester(new ArrayList<Log>() {{
				add(new PhysicalPlanLog(1, 1));
				add(new PhysicalPlanLog(2, 2));
			}},new ArrayList<Log>(){{
				add(new PhysicalPlanLog(2, 3));
				add(new PhysicalPlanLog(3, 3));
			}},3,new ArrayList<Log>() {{
				add(new PhysicalPlanLog(1, 1));
				add(new PhysicalPlanLog(2, 3));
				add(new PhysicalPlanLog(3, 3));
			}},2));
		}};
		for (RaftLogManagerTester test : tests) {
			CommittedEntryManager committedEntryManager = new CommittedEntryManager();
			committedEntryManager.append(test.previousEntries);
			RaftLogManager instance = null;
			try {
				instance = new RaftLogManager(committedEntryManager,new StableEntryManager());
				instance.append(test.appendingEntries);
			} catch (TruncateCommittedEntryException e) {
				fail("An unexpected exception was thrown.");
			}
			try {
				List<Log> entries = instance.getEntries(1, instance.getLastIndex());
			}
			assertEquals(test.isUpToDate, instance.isLogUpToDate(test.lastTerm,test.lastIndex));
		}
	}

	@Test
	public void commitTo() {
	}

	@Test
	public void getCommitIndex() {
	}

	@Test
	public void logValid() {
	}

	@Test
	public void getEntries() {
	}

	@Test
	public void getLogByIndex() {
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
			add(new PhysicalPlanLog(1, 1));
			add(new PhysicalPlanLog(2, 2));
			add(new PhysicalPlanLog(3, 3));
		}};
		RaftLogManager instance = null;
		try {
			instance = new RaftLogManager();
			instance.append(previousEntries);
		} catch (TruncateCommittedEntryException e) {
			fail("An unexpected exception was thrown.");
		}
		List<RaftLogManagerTester> tests = new ArrayList<RaftLogManagerTester>() {{
			// no conflict, empty ent
			add(new RaftLogManagerTester(new ArrayList<>(), 0));
			// no conflict
			add(new RaftLogManagerTester(new ArrayList<Log>() {{
				add(new PhysicalPlanLog(1, 1));
				add(new PhysicalPlanLog(2, 2));
				add(new PhysicalPlanLog(3, 3));
			}}, 0));
			add(new RaftLogManagerTester(new ArrayList<Log>() {{
				add(new PhysicalPlanLog(2, 2));
				add(new PhysicalPlanLog(3, 3));
			}}, 0));
			add(new RaftLogManagerTester(new ArrayList<Log>() {{
				add(new PhysicalPlanLog(3, 3));
			}}, 0));
			// no conflict, but has new entries
			add(new RaftLogManagerTester(new ArrayList<Log>() {{
				add(new PhysicalPlanLog(1, 1));
				add(new PhysicalPlanLog(2, 2));
				add(new PhysicalPlanLog(3, 3));
				add(new PhysicalPlanLog(4, 4));
				add(new PhysicalPlanLog(5, 4));
			}}, 4));
			add(new RaftLogManagerTester(new ArrayList<Log>() {{
				add(new PhysicalPlanLog(2, 2));
				add(new PhysicalPlanLog(3, 3));
				add(new PhysicalPlanLog(4, 4));
				add(new PhysicalPlanLog(5, 4));
			}}, 4));
			add(new RaftLogManagerTester(new ArrayList<Log>() {{
				add(new PhysicalPlanLog(3, 3));
				add(new PhysicalPlanLog(4, 4));
				add(new PhysicalPlanLog(5, 4));
			}}, 4));
			add(new RaftLogManagerTester(new ArrayList<Log>() {{
				add(new PhysicalPlanLog(4, 4));
				add(new PhysicalPlanLog(5, 4));
			}}, 4));
			// conflicts with existing entries
			add(new RaftLogManagerTester(new ArrayList<Log>() {{
				add(new PhysicalPlanLog(1, 4));
				add(new PhysicalPlanLog(2, 4));
			}}, 1));
			add(new RaftLogManagerTester(new ArrayList<Log>() {{
				add(new PhysicalPlanLog(2, 1));
				add(new PhysicalPlanLog(3, 4));
				add(new PhysicalPlanLog(4, 4));
			}}, 2));
			add(new RaftLogManagerTester(new ArrayList<Log>() {{
				add(new PhysicalPlanLog(3, 1));
				add(new PhysicalPlanLog(4, 2));
				add(new PhysicalPlanLog(5, 4));
				add(new PhysicalPlanLog(6, 4));
			}}, 3));
		}};
		for (RaftLogManagerTester test : tests) {
			assertEquals(test.testConflict, instance.findConflict(test.conflictEntries));
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
			add(new PhysicalPlanLog(1, 1));
			add(new PhysicalPlanLog(2, 2));
			add(new PhysicalPlanLog(3, 3));
		}};
		RaftLogManager instance = null;
		try {
			instance = new RaftLogManager();
			instance.append(previousEntries);
		} catch (TruncateCommittedEntryException e) {
			fail("An unexpected exception was thrown.");
		}
		RaftLogManager finalInstance = instance;
		List<RaftLogManagerTester> tests = new ArrayList<RaftLogManagerTester>() {{
			// greater term, ignore lastIndex
			add(new RaftLogManagerTester(finalInstance.getLastIndex() - 1,4,true));
			add(new RaftLogManagerTester(finalInstance.getLastIndex() ,4,true));
			add(new RaftLogManagerTester(finalInstance.getLastIndex() + 1,4,true));
			// smaller term, ignore lastIndex
			add(new RaftLogManagerTester(finalInstance.getLastIndex() - 1,2,false));
			add(new RaftLogManagerTester(finalInstance.getLastIndex() ,2,false));
			add(new RaftLogManagerTester(finalInstance.getLastIndex() + 1,2,false));
			// equal term, equal or lager lastIndex wins
			add(new RaftLogManagerTester(finalInstance.getLastIndex() - 1,3,false));
			add(new RaftLogManagerTester(finalInstance.getLastIndex() ,3,true));
			add(new RaftLogManagerTester(finalInstance.getLastIndex() + 1,3,true));
		}};
		for (RaftLogManagerTester test : tests) {
			assertEquals(test.isUpToDate, instance.isLogUpToDate(test.lastTerm,test.lastIndex));
		}
	}
}