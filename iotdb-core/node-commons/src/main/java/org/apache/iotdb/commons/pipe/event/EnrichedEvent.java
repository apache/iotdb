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

package org.apache.iotdb.commons.pipe.event;

import org.apache.iotdb.commons.consensus.index.ProgressIndex;
import org.apache.iotdb.commons.consensus.index.impl.MinimumProgressIndex;
import org.apache.iotdb.commons.pipe.pattern.PipePattern;
import org.apache.iotdb.commons.pipe.progress.committer.PipeEventCommitManager;
import org.apache.iotdb.commons.pipe.task.meta.PipeTaskMeta;
import org.apache.iotdb.pipe.api.event.Event;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * {@link EnrichedEvent} is an {@link Event} that can be enriched with additional runtime
 * information. The additional information mainly includes the {@link EnrichedEvent#referenceCount}
 * of the {@link Event}.
 */
public abstract class EnrichedEvent implements Event {

  private static final Logger LOGGER = LoggerFactory.getLogger(EnrichedEvent.class);

  protected final AtomicInteger referenceCount;

  protected final String pipeName;
  protected final PipeTaskMeta pipeTaskMeta;

  protected String committerKey;
  public static final long NO_COMMIT_ID = -1;
  protected long commitId = NO_COMMIT_ID;

  protected final PipePattern pipePattern;

  protected final long startTime;
  protected final long endTime;

  protected boolean isPatternParsed;
  protected boolean isTimeParsed;

  protected boolean shouldReportOnCommit = false;

  protected EnrichedEvent(
      String pipeName,
      PipeTaskMeta pipeTaskMeta,
      PipePattern pipePattern,
      long startTime,
      long endTime) {
    referenceCount = new AtomicInteger(0);
    this.pipeName = pipeName;
    this.pipeTaskMeta = pipeTaskMeta;
    this.pipePattern = pipePattern;
    this.startTime = startTime;
    this.endTime = endTime;
    isPatternParsed = this.pipePattern == null || this.pipePattern.isRoot();
    isTimeParsed = Long.MIN_VALUE == startTime && Long.MAX_VALUE == endTime;
  }

  /**
   * Increase the {@link EnrichedEvent#referenceCount} of this event. When the {@link
   * EnrichedEvent#referenceCount} is positive, the data in the resource of this {@link
   * EnrichedEvent} should be safe to use.
   *
   * @param holderMessage the message of the invoker
   * @return {@code true} if the {@link EnrichedEvent#referenceCount} is increased successfully,
   *     {@code false} if the {@link EnrichedEvent} is not
   */
  public boolean increaseReferenceCount(String holderMessage) {
    boolean isSuccessful = true;
    synchronized (this) {
      if (referenceCount.get() == 0) {
        isSuccessful = internallyIncreaseResourceReferenceCount(holderMessage);
      }
      referenceCount.incrementAndGet();
    }
    return isSuccessful;
  }

  /**
   * Increase the {@link EnrichedEvent#referenceCount} of the resource of this {@link
   * EnrichedEvent}.
   *
   * @param holderMessage the message of the invoker
   * @return {@code true} if the {@link EnrichedEvent#referenceCount} is increased successfully,
   *     {@code false} if the {@link EnrichedEvent} is not controlled by the invoker, which means
   *     the data stored in the event is not safe to use
   */
  public abstract boolean internallyIncreaseResourceReferenceCount(String holderMessage);

  /**
   * Decrease the {@link EnrichedEvent#referenceCount} of this {@link EnrichedEvent} by 1. If the
   * {@link EnrichedEvent#referenceCount} is decreased to 0, the {@link EnrichedEvent} can be
   * recycled and the data stored in the {@link EnrichedEvent} is not safe to use, the processing
   * {@link ProgressIndex} of the event should be reported to the {@link PipeTaskMeta}.
   *
   * @param holderMessage the message of the invoker
   * @return {@code true} if the {@link EnrichedEvent#referenceCount} is decreased successfully, v
   *     otherwise
   */
  public boolean decreaseReferenceCount(String holderMessage, boolean shouldReport) {
    boolean isSuccessful = true;
    synchronized (this) {
      if (referenceCount.get() == 1) {
        isSuccessful = internallyDecreaseResourceReferenceCount(holderMessage);
        if (shouldReport) {
          shouldReportOnCommit = true;
        }
        PipeEventCommitManager.getInstance().commit(this, committerKey);
      }
      final int newReferenceCount = referenceCount.decrementAndGet();
      if (newReferenceCount < 0) {
        LOGGER.warn("reference count is decreased to {}.", newReferenceCount);
      }
    }
    return isSuccessful;
  }

  /**
   * Decrease the {@link EnrichedEvent#referenceCount} of this {@link EnrichedEvent} to 0, to
   * release the {@link EnrichedEvent} directly. The {@link EnrichedEvent} can be recycled and the
   * data stored in the {@link EnrichedEvent} may not be safe to use.
   *
   * @param holderMessage the message of the invoker
   * @return {@code true} if the {@link EnrichedEvent#referenceCount} is decreased successfully,
   *     {@code true} otherwise
   */
  public boolean clearReferenceCount(String holderMessage) {
    boolean isSuccessful = true;
    synchronized (this) {
      if (referenceCount.get() >= 1) {
        isSuccessful = internallyDecreaseResourceReferenceCount(holderMessage);
      }
      referenceCount.set(0);
    }
    return isSuccessful;
  }

  /**
   * Decrease the {@link EnrichedEvent#referenceCount} of this {@link EnrichedEvent}. If the {@link
   * EnrichedEvent#referenceCount} is decreased to 0, the {@link EnrichedEvent} can be recycled and
   * the data stored in the {@link EnrichedEvent} may not be safe to use.
   *
   * @param holderMessage the message of the invoker
   * @return {@code true} if the {@link EnrichedEvent#referenceCount} is decreased successfully,
   *     {@code true} otherwise
   */
  public abstract boolean internallyDecreaseResourceReferenceCount(String holderMessage);

  protected void reportProgress() {
    if (pipeTaskMeta != null) {
      final ProgressIndex progressIndex = getProgressIndex();
      pipeTaskMeta.updateProgressIndex(
          progressIndex == null ? MinimumProgressIndex.INSTANCE : progressIndex);
    }
  }

  public void bindProgressIndex(ProgressIndex progressIndex) {
    throw new UnsupportedOperationException("This event does not support binding progressIndex.");
  }

  public abstract ProgressIndex getProgressIndex();

  /**
   * Get the {@link EnrichedEvent#referenceCount} of this {@link EnrichedEvent}.
   *
   * @return the {@link EnrichedEvent#referenceCount}
   */
  public int getReferenceCount() {
    return referenceCount.get();
  }

  public final String getPipeName() {
    return pipeName;
  }

  /**
   * Get the pattern string of this {@link EnrichedEvent}.
   *
   * @return the pattern string
   */
  public final String getPatternString() {
    return pipePattern != null ? pipePattern.getPattern() : null;
  }

  public final PipePattern getPipePattern() {
    return pipePattern;
  }

  public final long getStartTime() {
    return startTime;
  }

  public final long getEndTime() {
    return endTime;
  }

  /**
   * If pipe's pattern is database-level, then no need to parse {@link EnrichedEvent} by pattern
   * cause pipes are data-region-level.
   */
  public void skipParsingPattern() {
    isPatternParsed = true;
  }

  public void skipParsingTime() {
    isTimeParsed = true;
  }

  public boolean shouldParseTimeOrPattern() {
    return shouldParseTime() || shouldParsePattern();
  }

  public boolean shouldParsePattern() {
    return !isPatternParsed;
  }

  public boolean shouldParseTime() {
    return !isTimeParsed;
  }

  public abstract EnrichedEvent shallowCopySelfAndBindPipeTaskMetaForProgressReport(
      String pipeName,
      PipeTaskMeta pipeTaskMeta,
      PipePattern pattern,
      long startTime,
      long endTime);

  public PipeTaskMeta getPipeTaskMeta() {
    return pipeTaskMeta;
  }

  public abstract boolean isGeneratedByPipe();

  /** Whether the {@link EnrichedEvent} need to be committed in order. */
  public boolean needToCommit() {
    return true;
  }

  public abstract boolean mayEventTimeOverlappedWithTimeRange();

  public void setCommitterKeyAndCommitId(String committerKey, long commitId) {
    this.committerKey = committerKey;
    this.commitId = commitId;
  }

  public String getCommitterKey() {
    return committerKey;
  }

  public long getCommitId() {
    return commitId;
  }

  public void onCommitted() {
    if (shouldReportOnCommit) {
      reportProgress();
    }
  }

  @Override
  public String toString() {
    return "EnrichedEvent{"
        + "referenceCount="
        + referenceCount.get()
        + ", pipeName='"
        + pipeName
        + "', pipeTaskMeta="
        + pipeTaskMeta
        + ", committerKey='"
        + committerKey
        + "', commitId="
        + commitId
        + ", pattern='"
        + pipePattern
        + "', startTime="
        + startTime
        + ", endTime="
        + endTime
        + ", isPatternParsed="
        + isPatternParsed
        + ", isTimeParsed="
        + isTimeParsed
        + ", shouldReportOnCommit="
        + shouldReportOnCommit
        + '}';
  }

  public String coreReportMessage() {
    return "EnrichedEvent{"
        + "referenceCount="
        + referenceCount.get()
        + ", pipeName='"
        + pipeName
        + "', committerKey='"
        + committerKey
        + "', commitId="
        + commitId
        + ", pattern='"
        + pipePattern
        + "', startTime="
        + startTime
        + ", endTime="
        + endTime
        + ", isPatternParsed="
        + isPatternParsed
        + ", isTimeParsed="
        + isTimeParsed
        + ", shouldReportOnCommit="
        + shouldReportOnCommit
        + '}';
  }
}
