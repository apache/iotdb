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
import org.apache.iotdb.commons.pipe.progress.PipeEventCommitManager;
import org.apache.iotdb.commons.pipe.task.meta.PipeTaskMeta;
import org.apache.iotdb.pipe.api.event.Event;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * {@link EnrichedEvent} is an {@link Event} that can be enriched with additional runtime
 * information. The additional information mainly includes the {@link EnrichedEvent#referenceCount}
 * of the {@link Event}.
 */
public abstract class EnrichedEvent implements Event {

  private static final Logger LOGGER = LoggerFactory.getLogger(EnrichedEvent.class);

  protected final AtomicInteger referenceCount;
  // This variable is used to indicate whether the event's reference count has ever been decreased
  // to zero.
  protected final AtomicBoolean isReleased;

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

  protected boolean shouldReportOnCommit = true;

  protected EnrichedEvent(
      final String pipeName,
      final PipeTaskMeta pipeTaskMeta,
      final PipePattern pipePattern,
      final long startTime,
      final long endTime) {
    referenceCount = new AtomicInteger(0);
    isReleased = new AtomicBoolean(false);
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
   *     {@code false} otherwise; {@link EnrichedEvent#referenceCount} will be incremented
   *     regardless of the circumstances
   */
  public boolean increaseReferenceCount(final String holderMessage) {
    boolean isSuccessful = true;
    synchronized (this) {
      if (isReleased.get()) {
        LOGGER.warn(
            "re-increase reference count to event that has already been released: {}, stack trace: {}",
            coreReportMessage(),
            Thread.currentThread().getStackTrace());
        isSuccessful = false;
        // Here we still increase the reference count, to remain consistent with the behavior after
        // internal increase failure.
        referenceCount.incrementAndGet();
      } else {
        if (referenceCount.get() == 0) {
          // We assume that this function will not throw any exceptions.
          isSuccessful = internallyIncreaseResourceReferenceCount(holderMessage);
        }
        referenceCount.incrementAndGet();
      }
    }
    if (!isSuccessful) {
      LOGGER.warn("increase reference count failed, EnrichedEvent: {}", coreReportMessage());
    }
    return isSuccessful;
  }

  /**
   * Increase the {@link EnrichedEvent#referenceCount} of the resource of this {@link
   * EnrichedEvent}.
   *
   * <p>We assume that this function will not throw any exceptions.
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
   * @return {@code true} if the {@link EnrichedEvent#referenceCount} is decreased successfully,
   *     {@code false} otherwise; {@link EnrichedEvent#referenceCount} will be decremented
   *     regardless of the circumstances
   */
  public boolean decreaseReferenceCount(final String holderMessage, final boolean shouldReport) {
    boolean isSuccessful = true;
    synchronized (this) {
      if (referenceCount.get() == 1 && !isReleased.get()) {
        // We assume that this function will not throw any exceptions.
        isSuccessful = internallyDecreaseResourceReferenceCount(holderMessage);
        if (!shouldReport) {
          shouldReportOnCommit = false;
        }
        PipeEventCommitManager.getInstance().commit(this, committerKey);
      }
      final int newReferenceCount = referenceCount.decrementAndGet();
      if (newReferenceCount == 0) {
        isReleased.set(true);
      }
      if (newReferenceCount < 0) {
        LOGGER.warn(
            "reference count is decreased to {}, event: {}, stack trace: {}",
            newReferenceCount,
            coreReportMessage(),
            Thread.currentThread().getStackTrace());
      }
    }
    if (!isSuccessful) {
      LOGGER.warn("decrease reference count failed, EnrichedEvent: {}", coreReportMessage());
    }
    return isSuccessful;
  }

  /**
   * Decrease the {@link EnrichedEvent#referenceCount} of this {@link EnrichedEvent} to 0, to
   * release the {@link EnrichedEvent} directly. The {@link EnrichedEvent} can be recycled and the
   * data stored in the {@link EnrichedEvent} may not be safe to use.
   *
   * @param holderMessage the message of the invoker
   * @return {@code true} if the {@link EnrichedEvent#referenceCount} is cleared successfully,
   *     {@code false} otherwise; {@link EnrichedEvent#referenceCount} will be reset to zero
   *     regardless of the circumstances
   */
  public boolean clearReferenceCount(final String holderMessage) {
    boolean isSuccessful = true;
    synchronized (this) {
      if (referenceCount.get() >= 1 && !isReleased.get()) {
        // We assume that this function will not throw any exceptions.
        isSuccessful = internallyDecreaseResourceReferenceCount(holderMessage);
        isReleased.set(true);
      }
      referenceCount.set(0);
    }
    if (!isSuccessful) {
      LOGGER.warn("clear reference count failed, EnrichedEvent: {}", coreReportMessage());
    }
    return isSuccessful;
  }

  /**
   * Decrease the {@link EnrichedEvent#referenceCount} of this {@link EnrichedEvent}. If the {@link
   * EnrichedEvent#referenceCount} is decreased to 0, the {@link EnrichedEvent} can be recycled and
   * the data stored in the {@link EnrichedEvent} may not be safe to use.
   *
   * <p>We assume that this function will not throw any exceptions.
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

  /**
   * Externally skip the report of the processing {@link ProgressIndex} of this {@link
   * EnrichedEvent} when committed. Report by generated events are still allowed.
   */
  public void skipReportOnCommit() {
    shouldReportOnCommit = false;
  }

  public void bindProgressIndex(final ProgressIndex progressIndex) {
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

  public void setCommitterKeyAndCommitId(final String committerKey, final long commitId) {
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

  public boolean isReleased() {
    return isReleased.get();
  }

  @Override
  public String toString() {
    return "EnrichedEvent{"
        + "referenceCount="
        + referenceCount.get()
        + ", isReleased="
        + isReleased.get()
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
        + ", isReleased="
        + isReleased.get()
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
