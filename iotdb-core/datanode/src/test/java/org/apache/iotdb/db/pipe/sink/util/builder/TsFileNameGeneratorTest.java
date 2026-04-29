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

package org.apache.iotdb.db.pipe.sink.util.builder;

import org.apache.iotdb.commons.consensus.index.ProgressIndex;
import org.apache.iotdb.commons.consensus.index.impl.MinimumProgressIndex;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeTaskMeta;
import org.apache.iotdb.commons.pipe.agent.task.progress.CommitterKey;
import org.apache.iotdb.commons.pipe.datastructure.pattern.TablePattern;
import org.apache.iotdb.commons.pipe.datastructure.pattern.TreePattern;
import org.apache.iotdb.commons.pipe.event.EnrichedEvent;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;
import org.apache.iotdb.pipe.api.event.dml.insertion.TsFileInsertionEvent;

import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.util.Collections;

public class TsFileNameGeneratorTest {

  @Test
  public void testTargetNameForEventUsesStableCommitIdentity() {
    final TestTsFileEvent event = new TestTsFileEvent(new File("source.tsfile"));
    event.setCommitterKeyAndCommitId(new CommitterKey("pipe name/1", 100L, 7, 2), 11L);

    Assert.assertEquals(
        "source_pipe_name_1_7_100_2_11", TsFileNameGenerator.targetNameForEvent(event));
  }

  @Test
  public void testTargetNameForFileDistinguishesDifferentPaths() {
    final File first = new File("target/first/same.tsfile");
    final File second = new File("target/second/same.tsfile");

    Assert.assertNotEquals(
        TsFileNameGenerator.targetNameForFile(first),
        TsFileNameGenerator.targetNameForFile(second));
  }

  @Test
  public void testTargetNameForGeneratedFileStripsTsFileSuffix() {
    Assert.assertEquals(
        "tb_1_2_3", TsFileNameGenerator.targetNameForGeneratedFile(new File("tb_1_2_3.tsfile")));
  }

  private static class TestTsFileEvent extends EnrichedEvent implements TsFileInsertionEvent {

    private final File tsFile;

    private TestTsFileEvent(final File tsFile) {
      super(
          "pipe",
          1L,
          (PipeTaskMeta) null,
          (TreePattern) null,
          (TablePattern) null,
          null,
          null,
          null,
          true,
          Long.MIN_VALUE,
          Long.MAX_VALUE);
      this.tsFile = tsFile;
    }

    @Override
    public Iterable<TabletInsertionEvent> toTabletInsertionEvents() {
      return Collections.emptyList();
    }

    @Override
    public File getTsFile() {
      return tsFile;
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

    @Override
    public void close() {
      // do nothing
    }
  }
}
