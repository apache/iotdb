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

package org.apache.iotdb.commons.pipe.datastructure;

import org.apache.iotdb.commons.consensus.index.ProgressIndex;
import org.apache.iotdb.commons.consensus.index.impl.IoTProgressIndex;
import org.apache.iotdb.commons.consensus.index.impl.MinimumProgressIndex;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeTaskMeta;
import org.apache.iotdb.commons.pipe.agent.task.progress.interval.PipeCommitQueue;
import org.apache.iotdb.commons.pipe.datastructure.pattern.TablePattern;
import org.apache.iotdb.commons.pipe.datastructure.pattern.TreePattern;
import org.apache.iotdb.commons.pipe.event.EnrichedEvent;

import org.junit.Assert;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

public class PipeCommitQueueTest {

  private final PipeTaskMeta pipeTaskMeta = new PipeTaskMeta(MinimumProgressIndex.INSTANCE, 1);
  private final Set<Integer> commitHookTestSet = new HashSet<>();

  @Test
  public void testCommitQueue() {
    final PipeCommitQueue pipeCommitQueue = new PipeCommitQueue();
    pipeCommitQueue.offer(event(1));
    pipeCommitQueue.offer(eventWithHook(3));
    Assert.assertEquals(1, pipeCommitQueue.size());
    assertProgressIndex(1);

    pipeCommitQueue.offer(eventWithHook(5, false));
    pipeCommitQueue.offer(eventWithHook(4));
    Assert.assertEquals(1, pipeCommitQueue.size());
    Assert.assertTrue(commitHookTestSet.isEmpty());

    pipeCommitQueue.offer(eventWithHook(2));
    Assert.assertEquals(0, pipeCommitQueue.size());
    assertProgressIndex(4);
    Assert.assertEquals(4, commitHookTestSet.size());
    Assert.assertTrue(commitHookTestSet.contains(2));
    Assert.assertTrue(commitHookTestSet.contains(3));
    Assert.assertTrue(commitHookTestSet.contains(4));
    Assert.assertTrue(commitHookTestSet.contains(5));

    pipeCommitQueue.offer(eventWithHook(6, null));
    Assert.assertEquals(0, pipeCommitQueue.size());
    assertProgressIndex(4);
    Assert.assertTrue(commitHookTestSet.contains(6));
    Assert.assertEquals(5, commitHookTestSet.size());
  }

  private TestEnrichedEvent event(final long commitId) {
    return event(commitId, new IoTProgressIndex(0, commitId));
  }

  private TestEnrichedEvent eventWithHook(final long commitId) {
    return eventWithHook(commitId, new IoTProgressIndex(0, commitId));
  }

  private TestEnrichedEvent eventWithHook(final long commitId, final boolean shouldReportOnCommit) {
    final TestEnrichedEvent event = eventWithHook(commitId);
    event.setShouldReportOnCommit(shouldReportOnCommit);
    return event;
  }

  private TestEnrichedEvent eventWithHook(final long commitId, final ProgressIndex progressIndex) {
    final TestEnrichedEvent event = event(commitId, progressIndex);
    event.addOnCommittedHook(() -> commitHookTestSet.add((int) commitId));
    return event;
  }

  private TestEnrichedEvent event(final long commitId, final ProgressIndex progressIndex) {
    return new TestEnrichedEvent(commitId, progressIndex);
  }

  private void assertProgressIndex(final long searchIndex) {
    Assert.assertEquals(new IoTProgressIndex(0, searchIndex), pipeTaskMeta.getProgressIndex());
  }

  private class TestEnrichedEvent extends EnrichedEvent {
    private final ProgressIndex progressIndex;

    private TestEnrichedEvent(final long commitId, final ProgressIndex progressIndex) {
      this(
          null,
          0,
          PipeCommitQueueTest.this.pipeTaskMeta,
          null,
          null,
          null,
          null,
          null,
          false,
          Long.MIN_VALUE,
          Long.MAX_VALUE);
      this.commitId = commitId;
      this.progressIndex = progressIndex;
    }

    @Override
    public ProgressIndex getProgressIndex() {
      return progressIndex;
    }

    /////////////////////////////// Useless logic ///////////////////////////////

    protected TestEnrichedEvent(
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
      super(
          pipeName,
          creationTime,
          pipeTaskMeta,
          treePattern,
          tablePattern,
          userId,
          userName,
          cliHostname,
          skipIfNoPrivileges,
          startTime,
          endTime);
    }

    @Override
    public boolean internallyIncreaseResourceReferenceCount(String holderMessage) {
      return false;
    }

    @Override
    public boolean internallyDecreaseResourceReferenceCount(String holderMessage) {
      return false;
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
      return null;
    }

    @Override
    public boolean isGeneratedByPipe() {
      return false;
    }

    @Override
    public boolean mayEventTimeOverlappedWithTimeRange() {
      return false;
    }

    @Override
    public boolean mayEventPathsOverlappedWithPattern() {
      return false;
    }
  }
}
