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

package org.apache.iotdb.db.pipe.event;

import org.apache.iotdb.commons.consensus.index.ProgressIndex;
import org.apache.iotdb.commons.consensus.index.impl.MinimumProgressIndex;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeTaskMeta;
import org.apache.iotdb.commons.pipe.datastructure.pattern.TablePattern;
import org.apache.iotdb.commons.pipe.datastructure.pattern.TreePattern;
import org.apache.iotdb.commons.pipe.event.EnrichedEvent;
import org.apache.iotdb.db.pipe.event.realtime.PipeRealtimeEvent;

import org.junit.Assert;
import org.junit.Test;

public class PipeRealtimeEventTest {

  @Test
  public void testSkipReportOnCommitIsDelegatedToInnerEvent() {
    final TestEnrichedEvent innerEvent = new TestEnrichedEvent();
    final PipeRealtimeEvent realtimeEvent = new PipeRealtimeEvent(innerEvent, null, null);

    Assert.assertTrue(innerEvent.isShouldReportOnCommit());
    Assert.assertTrue(realtimeEvent.isShouldReportOnCommit());

    realtimeEvent.skipReportOnCommit();

    Assert.assertFalse(innerEvent.isShouldReportOnCommit());
    Assert.assertFalse(realtimeEvent.isShouldReportOnCommit());
  }

  private static class TestEnrichedEvent extends EnrichedEvent {

    private TestEnrichedEvent() {
      super(
          null,
          0,
          null,
          null,
          null,
          null,
          null,
          null,
          false,
          Long.MIN_VALUE,
          Long.MAX_VALUE);
    }

    @Override
    public boolean internallyIncreaseResourceReferenceCount(final String holderMessage) {
      return true;
    }

    @Override
    public boolean internallyDecreaseResourceReferenceCount(final String holderMessage) {
      return true;
    }

    @Override
    public ProgressIndex getProgressIndex() {
      return MinimumProgressIndex.INSTANCE;
    }

    @Override
    public EnrichedEvent shallowCopySelfAndBindPipeTaskMetaForProgressReport(
        final String pipeName,
        final long creationTime,
        final PipeTaskMeta pipeTaskMeta,
        final TreePattern treePattern,
        final TablePattern tablePattern,
        final String userId,
        final String userName,
        final String cliHostname,
        final boolean skipIfNoPrivileges,
        final long startTime,
        final long endTime) {
      return this;
    }

    @Override
    public boolean isGeneratedByPipe() {
      return false;
    }

    @Override
    public boolean mayEventTimeOverlappedWithTimeRange() {
      return true;
    }

    @Override
    public boolean mayEventPathsOverlappedWithPattern() {
      return true;
    }
  }
}
