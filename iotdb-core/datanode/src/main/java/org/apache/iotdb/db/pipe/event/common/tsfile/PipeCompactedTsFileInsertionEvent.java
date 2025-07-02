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
import org.apache.iotdb.commons.pipe.agent.task.progress.CommitterKey;
import org.apache.iotdb.db.pipe.extractor.dataregion.realtime.assigner.PipeTsFileEpochProgressIndexKeeper;
import org.apache.iotdb.db.storageengine.dataregion.memtable.TsFileProcessor;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;

import java.io.File;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public class PipeCompactedTsFileInsertionEvent extends PipeTsFileInsertionEvent {

  private final String dataRegionId;
  private final Set<String> originFilePaths;

  public PipeCompactedTsFileInsertionEvent(
      final CommitterKey committerKey,
      final Set<PipeTsFileInsertionEvent> originalEvents,
      final PipeTsFileInsertionEvent anyOfOriginalEvents,
      final TsFileResource tsFileResource,
      final boolean shouldReportProgress) {
    super(
        tsFileResource,
        bindIsWithMod(originalEvents),
        bindIsLoaded(originalEvents),
        bindIsGeneratedByHistoricalExtractor(originalEvents),
        committerKey.getPipeName(),
        committerKey.getCreationTime(),
        anyOfOriginalEvents.getPipeTaskMeta(),
        anyOfOriginalEvents.getPipePattern(),
        anyOfOriginalEvents.getStartTime(),
        anyOfOriginalEvents.getEndTime());

    this.dataRegionId = String.valueOf(committerKey.getRegionId());
    this.originFilePaths =
        originalEvents.stream()
            .map(PipeTsFileInsertionEvent::getTsFile)
            .map(File::getPath)
            .collect(Collectors.toSet());

    // init fields of EnrichedEvent

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
  public void eliminateProgressIndex() {
    if (Objects.isNull(overridingProgressIndex)) {
      for (final String originFilePath : originFilePaths) {
        PipeTsFileEpochProgressIndexKeeper.getInstance()
            .eliminateProgressIndex(dataRegionId, originFilePath);
      }
    }
  }
}
