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

package org.apache.iotdb.db.pipe.event.common.tsfile;

import org.apache.iotdb.commons.consensus.index.ProgressIndex;
import org.apache.iotdb.commons.consensus.index.impl.MinimumProgressIndex;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeTaskMeta;
import org.apache.iotdb.commons.pipe.agent.task.progress.CommitterKey;
import org.apache.iotdb.commons.pipe.datastructure.pattern.TablePattern;
import org.apache.iotdb.commons.pipe.datastructure.pattern.TreePattern;
import org.apache.iotdb.commons.pipe.event.EnrichedEvent;
import org.apache.iotdb.db.pipe.source.dataregion.realtime.assigner.PipeTsFileEpochProgressIndexKeeper;
import org.apache.iotdb.db.storageengine.dataregion.memtable.TsFileProcessor;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;

import java.io.File;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public class PipeCompactedTsFileInsertionEvent extends PipeTsFileInsertionEvent {

  private final String dataRegionId;
  private final Set<String> originFilePaths;
  private final List<Long> commitIds;

  public PipeCompactedTsFileInsertionEvent(
      final CommitterKey committerKey,
      final Set<PipeTsFileInsertionEvent> originalEvents,
      final PipeTsFileInsertionEvent anyOfOriginalEvents,
      final TsFileResource tsFileResource,
      final boolean shouldReportProgress) {
    super(
        anyOfOriginalEvents.isTableModelEvent(),
        anyOfOriginalEvents.getTreeModelDatabaseName(),
        tsFileResource,
        null,
        bindIsWithMod(originalEvents),
        bindIsLoaded(originalEvents),
        bindIsGeneratedByHistoricalExtractor(originalEvents),
        // The table name shall not be used anymore when compacted
        null,
        committerKey.getPipeName(),
        committerKey.getCreationTime(),
        anyOfOriginalEvents.getPipeTaskMeta(),
        anyOfOriginalEvents.getTreePattern(),
        anyOfOriginalEvents.getTablePattern(),
        anyOfOriginalEvents.getUserId(),
        anyOfOriginalEvents.getUserName(),
        anyOfOriginalEvents.getCliHostname(),
        anyOfOriginalEvents.isSkipIfNoPrivileges(),
        anyOfOriginalEvents.getStartTime(),
        anyOfOriginalEvents.getEndTime());

    this.dataRegionId = String.valueOf(committerKey.getRegionId());
    this.originFilePaths =
        originalEvents.stream()
            .map(PipeTsFileInsertionEvent::getTsFile)
            .map(File::getPath)
            .collect(Collectors.toSet());
    this.commitIds =
        originalEvents.stream()
            .map(PipeTsFileInsertionEvent::getCommitId)
            .distinct()
            .collect(Collectors.toList());

    // init fields of EnrichedEvent
    this.committerKey = committerKey;
    isPatternParsed = bindIsPatternParsed(originalEvents);
    isTimeParsed = bindIsTimeParsed(originalEvents);
    this.shouldReportOnCommit = shouldReportProgress;

    // init fields of PipeTsFileInsertionEvent
    flushPointCount = bindFlushPointCount(originalEvents);
    overridingProgressIndex = bindOverridingProgressIndex(originalEvents);
  }

  private static boolean bindIsWithMod(Set<PipeTsFileInsertionEvent> originalEvents) {
    return originalEvents.stream().anyMatch(PipeTsFileInsertionEvent::isWithMod);
  }

  private static boolean bindIsLoaded(Set<PipeTsFileInsertionEvent> originalEvents) {
    return originalEvents.stream().anyMatch(PipeTsFileInsertionEvent::isLoaded);
  }

  private static boolean bindIsGeneratedByHistoricalExtractor(
      Set<PipeTsFileInsertionEvent> originalEvents) {
    return originalEvents.stream()
        .anyMatch(PipeTsFileInsertionEvent::isGeneratedByHistoricalExtractor);
  }

  private static boolean bindIsTimeParsed(Set<PipeTsFileInsertionEvent> originalEvents) {
    return originalEvents.stream().noneMatch(EnrichedEvent::shouldParseTime);
  }

  private static boolean bindIsPatternParsed(Set<PipeTsFileInsertionEvent> originalEvents) {
    return originalEvents.stream().noneMatch(EnrichedEvent::shouldParsePattern);
  }

  private static long bindFlushPointCount(Set<PipeTsFileInsertionEvent> originalEvents) {
    return originalEvents.stream()
        .mapToLong(
            e ->
                e.getFlushPointCount() == TsFileProcessor.FLUSH_POINT_COUNT_NOT_SET
                    ? 0
                    : e.getFlushPointCount())
        .sum();
  }

  private ProgressIndex bindOverridingProgressIndex(Set<PipeTsFileInsertionEvent> originalEvents) {
    ProgressIndex overridingProgressIndex = MinimumProgressIndex.INSTANCE;
    for (PipeTsFileInsertionEvent originalEvent : originalEvents) {
      if (originalEvent.overridingProgressIndex != null) {
        overridingProgressIndex =
            overridingProgressIndex.updateToMinimumEqualOrIsAfterProgressIndex(
                originalEvent.overridingProgressIndex);
      }
    }
    return overridingProgressIndex != null
            && !overridingProgressIndex.equals(MinimumProgressIndex.INSTANCE)
        ? overridingProgressIndex
        : null;
  }

  @Override
  public int getRebootTimes() {
    throw new UnsupportedOperationException(
        "PipeCompactedTsFileInsertionEvent does not support getRebootTimes.");
  }

  @Override
  public boolean hasMultipleCommitIds() {
    return true;
  }

  @Override
  public long getCommitId() {
    // max of commitIds is used as the commit id for this event
    return commitIds.stream()
        .max(Long::compareTo)
        .orElseThrow(
            () ->
                new IllegalStateException(
                    "No commit IDs found in PipeCompactedTsFileInsertionEvent."));
  }

  // return dummy events for each commit ID (except the max one)
  @Override
  public List<EnrichedEvent> getDummyEventsForCommitIds() {
    return commitIds.stream()
        .filter(commitId -> commitId != getCommitId())
        .map(PipeCompactedTsFileInsertionDummyEvent::new)
        .collect(Collectors.toList());
  }

  @Override
  public List<Long> getCommitIds() {
    return commitIds;
  }

  @Override
  public boolean equalsInPipeConsensus(final Object o) {
    throw new UnsupportedOperationException(
        "PipeCompactedTsFileInsertionEvent does not support equalsInPipeConsensus.");
  }

  @Override
  public void eliminateProgressIndex() {
    if (Objects.isNull(overridingProgressIndex)) {
      for (final String originFilePath : originFilePaths) {
        PipeTsFileEpochProgressIndexKeeper.getInstance()
            .eliminateProgressIndex(dataRegionId, pipeName, originFilePath);
      }
    }
  }

  public class PipeCompactedTsFileInsertionDummyEvent extends EnrichedEvent {

    private final long commitId;

    public PipeCompactedTsFileInsertionDummyEvent(final long commitId) {
      super(
          PipeCompactedTsFileInsertionEvent.this.pipeName,
          PipeCompactedTsFileInsertionEvent.this.creationTime,
          PipeCompactedTsFileInsertionEvent.this.pipeTaskMeta,
          null, // PipePattern is not needed for dummy event
          null,
          null,
          null,
          null,
          true,
          Long.MIN_VALUE,
          Long.MAX_VALUE);
      this.commitId = commitId; // Use the commitId passed in
      this.shouldReportOnCommit = false; // Dummy events do not report progress
    }

    @Override
    public long getCommitId() {
      return commitId;
    }

    @Override
    public boolean internallyIncreaseResourceReferenceCount(String holderMessage) {
      return true;
    }

    @Override
    public boolean internallyDecreaseResourceReferenceCount(String holderMessage) {
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

    @Override
    public String coreReportMessage() {
      return "PipeCompactedTsFileInsertionDummyEvent";
    }
  }
}
