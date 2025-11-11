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
    pipeCommitQueue.offer(new TestEnrichedEvent(1, new IoTProgressIndex(0, 1L)));
    pipeCommitQueue.offer(new TestEnrichedEvent(3, new IoTProgressIndex(0, 3L)));
    Assert.assertEquals(1, pipeCommitQueue.size());
    Assert.assertEquals(new IoTProgressIndex(0, 1L), pipeTaskMeta.getProgressIndex());
    TestEnrichedEvent nextEvent = new TestEnrichedEvent(5, new IoTProgressIndex(0, 5L));
    nextEvent.setShouldReportOnCommit(false);
    pipeCommitQueue.offer(nextEvent);
    pipeCommitQueue.offer(new TestEnrichedEvent(4, new IoTProgressIndex(0, 4L)));
    Assert.assertEquals(1, pipeCommitQueue.size());
    nextEvent = new TestEnrichedEvent(2, new IoTProgressIndex(0, 2L));
    nextEvent.addOnCommittedHook(() -> commitHookTestSet.add(1));
    pipeCommitQueue.offer(nextEvent);
    Assert.assertEquals(0, pipeCommitQueue.size());
    Assert.assertEquals(new IoTProgressIndex(0, 4L), pipeTaskMeta.getProgressIndex());
    Assert.assertEquals(1, commitHookTestSet.size());
    // Test null progressIndex
    pipeCommitQueue.offer(new TestEnrichedEvent(6, null));
  }

  private class TestEnrichedEvent extends EnrichedEvent {
    private ProgressIndex progressIndex;

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
