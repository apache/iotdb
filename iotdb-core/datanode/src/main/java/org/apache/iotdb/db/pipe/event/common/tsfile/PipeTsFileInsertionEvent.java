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
import org.apache.iotdb.commons.pipe.event.EnrichedEvent;
import org.apache.iotdb.commons.pipe.pattern.PipePattern;
import org.apache.iotdb.commons.pipe.task.meta.PipeTaskMeta;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeRawTabletInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.tsfile.container.TsFileInsertionDataContainer;
import org.apache.iotdb.db.pipe.event.common.tsfile.container.TsFileInsertionDataContainerProvider;
import org.apache.iotdb.db.pipe.extractor.dataregion.realtime.assigner.PipeTimePartitionProgressIndexKeeper;
import org.apache.iotdb.db.pipe.resource.PipeDataNodeResourceManager;
import org.apache.iotdb.db.pipe.resource.tsfile.PipeTsFileResourceManager;
import org.apache.iotdb.db.storageengine.dataregion.memtable.TsFileProcessor;
import org.apache.iotdb.db.storageengine.dataregion.modification.ModificationFile;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;
import org.apache.iotdb.pipe.api.event.dml.insertion.TsFileInsertionEvent;
import org.apache.iotdb.pipe.api.exception.PipeException;

import org.apache.tsfile.file.metadata.IDeviceID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

public class PipeTsFileInsertionEvent extends EnrichedEvent implements TsFileInsertionEvent {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeTsFileInsertionEvent.class);

  private final TsFileResource resource;
  private File tsFile;

  // This is true iff the modFile exists and should be transferred
  private boolean isWithMod;
  private File modFile;

  private final boolean isLoaded;
  private final boolean isGeneratedByPipe;
  private final boolean isGeneratedByPipeConsensus;
  private final boolean isGeneratedByHistoricalExtractor;

  private final AtomicBoolean isClosed;
  private TsFileInsertionDataContainer dataContainer;

  // The point count of the TsFile. Used for metrics on PipeConsensus' receiver side.
  // May be updated after it is flushed. Should be negative if not set.
  private long flushPointCount = TsFileProcessor.FLUSH_POINT_COUNT_NOT_SET;

  private ProgressIndex overridingProgressIndex;

  public PipeTsFileInsertionEvent(
      final TsFileResource resource,
      final boolean isLoaded,
      final boolean isGeneratedByPipe,
      final boolean isGeneratedByHistoricalExtractor) {
    // The modFile must be copied before the event is assigned to the listening pipes
    this(
        resource,
        true,
        isLoaded,
        isGeneratedByPipe,
        isGeneratedByHistoricalExtractor,
        null,
        0,
        null,
        null,
        Long.MIN_VALUE,
        Long.MAX_VALUE);
  }

  public PipeTsFileInsertionEvent(
      final TsFileResource resource,
      final boolean isWithMod,
      final boolean isLoaded,
      final boolean isGeneratedByPipe,
      final boolean isGeneratedByHistoricalExtractor,
      final String pipeName,
      final long creationTime,
      final PipeTaskMeta pipeTaskMeta,
      final PipePattern pattern,
      final long startTime,
      final long endTime) {
    super(pipeName, creationTime, pipeTaskMeta, pattern, startTime, endTime);

    this.resource = resource;
    tsFile = resource.getTsFile();

    final ModificationFile modFile = resource.getModFile();
    this.isWithMod = isWithMod && modFile.exists();
    this.modFile = this.isWithMod ? new File(modFile.getFilePath()) : null;

    this.isLoaded = isLoaded;
    this.isGeneratedByPipe = isGeneratedByPipe;
    this.isGeneratedByPipeConsensus = resource.isGeneratedByPipeConsensus();
    this.isGeneratedByHistoricalExtractor = isGeneratedByHistoricalExtractor;

    isClosed = new AtomicBoolean(resource.isClosed());
    // Register close listener if TsFile is not closed
    if (!isClosed.get()) {
      final TsFileProcessor processor = resource.getProcessor();
      if (processor != null) {
        processor.addCloseFileListener(
            o -> {
              synchronized (isClosed) {
                isClosed.set(true);
                isClosed.notifyAll();

                // Update flushPointCount after TsFile is closed
                flushPointCount = processor.getMemTableFlushPointCount();
              }
            });
      }
    }
    // Check again after register close listener in case TsFile is closed during the process
    // TsFile flushing steps:
    // 1. Flush tsFile
    // 2. First listener (Set resource status "closed" -> Set processor == null -> processor == null
    // is seen)
    // 3. Other listeners (Set "closed" status for events)
    // Then we can imply that:
    // 1. If the listener cannot be executed because all listeners passed, then resources status is
    // set "closed" and can be set here
    // 2. If the listener cannot be executed because processor == null is seen, then resources
    // status is set "closed" and can be set here
    // Then we know:
    // 1. The status in the event can be closed eventually.
    // 2. If the status is "closed", then the resource status is "closed".
    // Then we know:
    // If the status is "closed", then the resource status is "closed", the tsFile won't be altered
    // and can be sent.
    isClosed.set(resource.isClosed());
  }

  /**
   * @return {@code false} if this file can't be sent by pipe because it is empty. {@code true}
   *     otherwise.
   */
  public boolean waitForTsFileClose() throws InterruptedException {
    if (!isClosed.get()) {
      isClosed.set(resource.isClosed());

      synchronized (isClosed) {
        while (!isClosed.get()) {
          isClosed.wait(100);

          final boolean isClosedNow = resource.isClosed();
          if (isClosedNow) {
            isClosed.set(true);
            isClosed.notifyAll();

            // Update flushPointCount after TsFile is closed
            final TsFileProcessor processor = resource.getProcessor();
            if (processor != null) {
              flushPointCount = processor.getMemTableFlushPointCount();
            }

            break;
          }
        }
      }
    }

    // From illustrations above we know If the status is "closed", then the tsFile is flushed
    // And here we guarantee that the isEmpty() is set before flushing if tsFile is empty
    // Then we know: "isClosed" --> tsFile flushed --> (isEmpty() <--> tsFile is empty)
    return !resource.isEmpty();
  }

  public File getTsFile() {
    return tsFile;
  }

  public File getModFile() {
    return modFile;
  }

  public boolean isWithMod() {
    return isWithMod;
  }

  // If the previous "isWithMod" is false, the modFile has been set to "null", then the isWithMod
  // can't be set to true
  public void disableMod4NonTransferPipes(final boolean isWithMod) {
    this.isWithMod = isWithMod && this.isWithMod;
  }

  public boolean isLoaded() {
    return isLoaded;
  }

  public long getFileStartTime() {
    return resource.getFileStartTime();
  }

  /**
   * Only used for metrics on PipeConsensus' receiver side. If the event is recovered after data
   * node's restart, the flushPointCount can be not set. It's totally fine for the PipeConsensus'
   * receiver side. The receiver side will count the actual point count from the TsFile.
   *
   * <p>If you want to get the actual point count with no risk, you can call {@link
   * #count(boolean)}.
   */
  public long getFlushPointCount() {
    return flushPointCount;
  }

  public long getTimePartitionId() {
    return resource.getTimePartition();
  }

  /////////////////////////// EnrichedEvent ///////////////////////////

  @Override
  public boolean internallyIncreaseResourceReferenceCount(final String holderMessage) {
    try {
      tsFile = PipeDataNodeResourceManager.tsfile().increaseFileReference(tsFile, true, resource);
      if (isWithMod) {
        modFile = PipeDataNodeResourceManager.tsfile().increaseFileReference(modFile, false, null);
      }
      return true;
    } catch (final Exception e) {
      LOGGER.warn(
          String.format(
              "Increase reference count for TsFile %s or modFile %s error. Holder Message: %s",
              tsFile, modFile, holderMessage),
          e);
      return false;
    }
  }

  @Override
  public boolean internallyDecreaseResourceReferenceCount(final String holderMessage) {
    try {
      PipeDataNodeResourceManager.tsfile().decreaseFileReference(tsFile);
      if (isWithMod) {
        PipeDataNodeResourceManager.tsfile().decreaseFileReference(modFile);
      }
      return true;
    } catch (final Exception e) {
      LOGGER.warn(
          String.format(
              "Decrease reference count for TsFile %s error. Holder Message: %s",
              tsFile.getPath(), holderMessage),
          e);
      return false;
    }
  }

  @Override
  public void bindProgressIndex(final ProgressIndex overridingProgressIndex) {
    this.overridingProgressIndex = overridingProgressIndex;
  }

  @Override
  public ProgressIndex getProgressIndex() {
    try {
      if (!waitForTsFileClose()) {
        LOGGER.warn(
            "Skipping temporary TsFile {}'s progressIndex, will report MinimumProgressIndex",
            tsFile);
        return MinimumProgressIndex.INSTANCE;
      }
      if (Objects.nonNull(overridingProgressIndex)) {
        return overridingProgressIndex;
      }
      return resource.getMaxProgressIndexAfterClose();
    } catch (final InterruptedException e) {
      LOGGER.warn(
          String.format(
              "Interrupted when waiting for closing TsFile %s.", resource.getTsFilePath()));
      Thread.currentThread().interrupt();
      return MinimumProgressIndex.INSTANCE;
    }
  }

  @Override
  protected void reportProgress() {
    super.reportProgress();
    if (Objects.isNull(overridingProgressIndex)) {
      PipeTimePartitionProgressIndexKeeper.getInstance()
          .eliminateProgressIndex(
              resource.getDataRegionId(),
              resource.getTimePartition(),
              resource.getMaxProgressIndexAfterClose());
    }
  }

  @Override
  public PipeTsFileInsertionEvent shallowCopySelfAndBindPipeTaskMetaForProgressReport(
      final String pipeName,
      final long creationTime,
      final PipeTaskMeta pipeTaskMeta,
      final PipePattern pattern,
      final long startTime,
      final long endTime) {
    return new PipeTsFileInsertionEvent(
        resource,
        isWithMod,
        isLoaded,
        isGeneratedByPipe,
        isGeneratedByHistoricalExtractor,
        pipeName,
        creationTime,
        pipeTaskMeta,
        pattern,
        startTime,
        endTime);
  }

  @Override
  public boolean isGeneratedByPipe() {
    return isGeneratedByPipe;
  }

  @Override
  public boolean mayEventTimeOverlappedWithTimeRange() {
    // If the tsFile is not closed the resource.getFileEndTime() will be Long.MIN_VALUE
    // In that case we only judge the resource.getFileStartTime() to avoid losing data
    return isClosed.get()
        ? startTime <= resource.getFileEndTime() && resource.getFileStartTime() <= endTime
        : resource.getFileStartTime() <= endTime;
  }

  @Override
  public boolean mayEventPathsOverlappedWithPattern() {
    if (!resource.isClosed()) {
      return true;
    }

    try {
      final Map<IDeviceID, Boolean> deviceIsAlignedMap =
          PipeDataNodeResourceManager.tsfile()
              .getDeviceIsAlignedMapFromCache(
                  PipeTsFileResourceManager.getHardlinkOrCopiedFileInPipeDir(resource.getTsFile()),
                  false);
      final Set<IDeviceID> deviceSet =
          Objects.nonNull(deviceIsAlignedMap) ? deviceIsAlignedMap.keySet() : resource.getDevices();
      return deviceSet.stream().anyMatch(pipePattern::mayOverlapWithDevice);
    } catch (final Exception e) {
      LOGGER.warn(
          "Pipe {}: failed to get devices from TsFile {}, extract it anyway",
          pipeName,
          resource.getTsFilePath(),
          e);
      return true;
    }
  }

  /////////////////////////// TsFileInsertionEvent ///////////////////////////

  @Override
  public Iterable<TabletInsertionEvent> toTabletInsertionEvents() {
    try {
      if (!waitForTsFileClose()) {
        LOGGER.warn(
            "Pipe skipping temporary TsFile's parsing which shouldn't be transferred: {}", tsFile);
        return Collections.emptyList();
      }
      return initDataContainer().toTabletInsertionEvents();
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      close();

      final String errorMsg =
          String.format(
              "Interrupted when waiting for closing TsFile %s.", resource.getTsFilePath());
      LOGGER.warn(errorMsg, e);
      throw new PipeException(errorMsg);
    }
  }

  /** The method is used to prevent circular replication in PipeConsensus */
  public boolean isGeneratedByPipeConsensus() {
    return isGeneratedByPipeConsensus;
  }

  public boolean isGeneratedByHistoricalExtractor() {
    return isGeneratedByHistoricalExtractor;
  }

  private TsFileInsertionDataContainer initDataContainer() {
    try {
      if (dataContainer == null) {
        dataContainer =
            new TsFileInsertionDataContainerProvider(
                    tsFile, pipePattern, startTime, endTime, pipeTaskMeta, this)
                .provide();
      }
      return dataContainer;
    } catch (final IOException e) {
      close();

      final String errorMsg = String.format("Read TsFile %s error.", resource.getTsFilePath());
      LOGGER.warn(errorMsg, e);
      throw new PipeException(errorMsg);
    }
  }

  public long count(final boolean skipReportOnCommit) throws IOException {
    long count = 0;

    if (shouldParseTime()) {
      try {
        for (final TabletInsertionEvent event : toTabletInsertionEvents()) {
          final PipeRawTabletInsertionEvent rawEvent = ((PipeRawTabletInsertionEvent) event);
          count += rawEvent.count();
          if (skipReportOnCommit) {
            rawEvent.skipReportOnCommit();
          }
        }
        return count;
      } finally {
        close();
      }
    }

    try (final TsFileInsertionPointCounter counter =
        new TsFileInsertionPointCounter(tsFile, pipePattern)) {
      return counter.count();
    }
  }

  /** Release the resource of {@link TsFileInsertionDataContainer}. */
  @Override
  public void close() {
    if (dataContainer != null) {
      dataContainer.close();
      dataContainer = null;
    }
  }

  /////////////////////////// Object ///////////////////////////

  @Override
  public String toString() {
    return String.format(
            "PipeTsFileInsertionEvent{resource=%s, tsFile=%s, isLoaded=%s, isGeneratedByPipe=%s, isClosed=%s, dataContainer=%s}",
            resource, tsFile, isLoaded, isGeneratedByPipe, isClosed.get(), dataContainer)
        + " - "
        + super.toString();
  }

  @Override
  public String coreReportMessage() {
    return String.format(
            "PipeTsFileInsertionEvent{resource=%s, tsFile=%s, isLoaded=%s, isGeneratedByPipe=%s, isClosed=%s}",
            resource, tsFile, isLoaded, isGeneratedByPipe, isClosed.get())
        + " - "
        + super.coreReportMessage();
  }
}
