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
import org.apache.iotdb.commons.exception.pipe.PipeRuntimeOutOfMemoryCriticalException;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeTaskMeta;
import org.apache.iotdb.commons.pipe.config.PipeConfig;
import org.apache.iotdb.commons.pipe.datastructure.pattern.PipePattern;
import org.apache.iotdb.commons.pipe.event.EnrichedEvent;
import org.apache.iotdb.commons.pipe.resource.log.PipeLogger;
import org.apache.iotdb.commons.pipe.resource.ref.PipePhantomReferenceManager.PipeEventResource;
import org.apache.iotdb.db.pipe.agent.task.subtask.processor.PipeProcessorSubtaskExecutionGuard;
import org.apache.iotdb.db.pipe.agent.task.subtask.processor.PipeProcessorSubtaskYieldException;
import org.apache.iotdb.db.pipe.event.ReferenceTrackableEvent;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeRawTabletInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.tsfile.container.TsFileInsertionDataContainer;
import org.apache.iotdb.db.pipe.event.common.tsfile.container.TsFileInsertionDataContainerProvider;
import org.apache.iotdb.db.pipe.metric.overview.PipeDataNodeSinglePipeMetrics;
import org.apache.iotdb.db.pipe.resource.PipeDataNodeResourceManager;
import org.apache.iotdb.db.pipe.resource.memory.PipeMemoryManager;
import org.apache.iotdb.db.pipe.resource.memory.PipeMemoryManager.TsFileParserMemoryReservation;
import org.apache.iotdb.db.pipe.resource.tsfile.PipeTsFileResourceManager;
import org.apache.iotdb.db.pipe.source.dataregion.realtime.assigner.PipeTsFileEpochProgressIndexKeeper;
import org.apache.iotdb.db.storageengine.dataregion.memtable.TsFileProcessor;
import org.apache.iotdb.db.storageengine.dataregion.modification.ModificationFile;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;
import org.apache.iotdb.pipe.api.event.dml.insertion.TsFileInsertionEvent;
import org.apache.iotdb.pipe.api.exception.PipeException;

import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.PlainDeviceID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class PipeTsFileInsertionEvent extends EnrichedEvent
    implements TsFileInsertionEvent, ReferenceTrackableEvent {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeTsFileInsertionEvent.class);

  protected final TsFileResource resource;
  private final String dataRegionId;
  protected File tsFile;
  protected long extractTime = 0;

  // This is true iff the modFile exists and should be transferred
  protected boolean isWithMod;
  protected File modFile;

  protected final boolean isLoaded;
  protected final boolean isGeneratedByPipe;
  protected final boolean isGeneratedByPipeConsensus;
  protected final boolean isGeneratedByHistoricalExtractor;

  // Realtime TsFile events are created after TsFileProcessor#endFile(), so the file is already
  // immutable even if TsFileResource status is still UNCLOSED.
  private final boolean isTsFileSealed;

  protected final AtomicBoolean isClosed;
  protected final AtomicReference<TsFileInsertionDataContainer> dataContainer;
  private final AtomicBoolean isTsFileParserMemoryReserved = new AtomicBoolean(false);
  private final TsFileParserMemoryReservation tsFileParserMemoryReservationKey =
      new TsFileParserMemoryReservation();
  private final AtomicReference<Iterator<TabletInsertionEvent>> tabletInsertionEventIterator =
      new AtomicReference<>();
  private final AtomicReference<PipeRawTabletInsertionEvent> pendingTabletInsertionEvent =
      new AtomicReference<>();
  private final AtomicInteger parsedTabletInsertionEventCount = new AtomicInteger(0);
  private final AtomicBoolean isTsFileParsingCompleted = new AtomicBoolean(false);
  private final AtomicLong parsedPointCountForCount = new AtomicLong(0);

  // The point count of the TsFile. Used for metrics on PipeConsensus' receiver side.
  // May be updated after it is flushed. Should be negative if not set.
  protected long flushPointCount = TsFileProcessor.FLUSH_POINT_COUNT_NOT_SET;

  protected volatile ProgressIndex overridingProgressIndex;
  private Set<String> tableNames;
  private String tsFileParser;

  public PipeTsFileInsertionEvent(final TsFileResource resource, final boolean isLoaded) {
    // The modFile must be copied before the event is assigned to the listening pipes
    this(
        resource,
        null,
        true,
        isLoaded,
        false,
        null,
        0,
        null,
        null,
        Long.MIN_VALUE,
        Long.MAX_VALUE,
        true);
  }

  public PipeTsFileInsertionEvent(
      final TsFileResource resource,
      final File tsFile,
      final boolean isWithMod,
      final boolean isLoaded,
      final boolean isGeneratedByHistoricalExtractor,
      final String pipeName,
      final long creationTime,
      final PipeTaskMeta pipeTaskMeta,
      final PipePattern pattern,
      final long startTime,
      final long endTime) {
    this(
        resource,
        tsFile,
        isWithMod,
        isLoaded,
        isGeneratedByHistoricalExtractor,
        pipeName,
        creationTime,
        pipeTaskMeta,
        pattern,
        startTime,
        endTime,
        false);
  }

  private PipeTsFileInsertionEvent(
      final TsFileResource resource,
      final File tsFile,
      final boolean isWithMod,
      final boolean isLoaded,
      final boolean isGeneratedByHistoricalExtractor,
      final String pipeName,
      final long creationTime,
      final PipeTaskMeta pipeTaskMeta,
      final PipePattern pipePattern,
      final long startTime,
      final long endTime,
      final boolean isTsFileSealed) {
    super(pipeName, creationTime, pipeTaskMeta, pipePattern, startTime, endTime);

    this.resource = resource;
    this.dataRegionId = getDataRegionId(resource);

    // For events created at assigner or historical extractor, the tsFile is get from the resource
    // For events created for source, the tsFile is inherited from the assigner, because the
    // original tsFile may be gone, and we need to get the assigner's hard-linked tsFile to
    // hard-link it to each pipe dir
    this.tsFile = Objects.isNull(tsFile) ? resource.getTsFile() : tsFile;

    final ModificationFile modFile = resource.getModFile();
    this.isWithMod = isWithMod && modFile.exists();
    this.modFile = this.isWithMod ? new File(modFile.getFilePath()) : null;

    this.isLoaded = isLoaded;
    this.isGeneratedByPipe = resource.isGeneratedByPipe();
    this.isGeneratedByPipeConsensus = resource.isGeneratedByPipeConsensus();
    this.isGeneratedByHistoricalExtractor = isGeneratedByHistoricalExtractor;
    this.isTsFileSealed = isTsFileSealed;
    this.tableNames = tableNames;

    this.dataContainer = new AtomicReference<>(null);

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

    addOnCommittedHook(
        () -> {
          if (shouldReportOnCommit) {
            eliminateProgressIndex();
          }
        });
  }

  private static String getDataRegionId(final TsFileResource resource) {
    // TsFileResource#getDataRegionId assumes the storage-engine directory structure, while a
    // synthetic resource may wrap a standalone file.
    final File resourceTsFile = resource.getTsFile();
    final File timePartitionDir =
        Objects.isNull(resourceTsFile) ? null : resourceTsFile.getParentFile();
    final File dataRegionDir =
        Objects.isNull(timePartitionDir) ? null : timePartitionDir.getParentFile();
    return Objects.isNull(dataRegionDir) ? "" : dataRegionDir.getName();
  }

  /**
   * @return {@code false} if this file can't be sent by pipe because it is empty. {@code true}
   *     otherwise.
   */
  public boolean waitForTsFileClose() throws InterruptedException {
    return waitForTsFileClose(PipeProcessorSubtaskExecutionGuard.disabled());
  }

  public boolean waitForTsFileClose(
      final PipeProcessorSubtaskExecutionGuard processorExecutionGuard)
      throws InterruptedException {
    processorExecutionGuard.check();
    if (Objects.isNull(resource)) {
      return true;
    }

    if (isTsFileSealed) {
      return !resource.isEmpty();
    }

    if (!isClosed.get()) {
      isClosed.set(resource.isClosed());

      synchronized (isClosed) {
        while (!isClosed.get()) {
          processorExecutionGuard.check();
          isClosed.wait(100);
          processorExecutionGuard.check();

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

  @Override
  public File getTsFile() {
    return tsFile;
  }

  public String getDatabaseName() {
    return Objects.isNull(resource) ? null : resource.getDatabaseName();
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

  public long getExtractTime() {
    return extractTime;
  }

  /////////////////////////// EnrichedEvent ///////////////////////////

  @Override
  public boolean internallyIncreaseResourceReferenceCount(final String holderMessage) {
    extractTime = System.nanoTime();
    final String pipeTsFileResourcePipeName =
        PipeTsFileResourceManager.getPipeTsFileResourcePipeName(pipeName, creationTime);
    try {
      tsFile =
          PipeDataNodeResourceManager.tsfile()
              .increaseFileReference(tsFile, true, pipeTsFileResourcePipeName);
      if (isWithMod) {
        modFile =
            PipeDataNodeResourceManager.tsfile()
                .increaseFileReference(modFile, false, pipeTsFileResourcePipeName);
      }
      return true;
    } catch (final Exception e) {
      LOGGER.warn(
          String.format(
              "Increase reference count for TsFile %s or modFile %s error. Holder Message: %s",
              tsFile, modFile, holderMessage),
          e);
      return false;
    } finally {
      if (Objects.nonNull(pipeName)) {
        PipeDataNodeSinglePipeMetrics.getInstance()
            .increaseTsFileEventCount(pipeName, creationTime);
      }
    }
  }

  @Override
  public boolean internallyDecreaseResourceReferenceCount(final String holderMessage) {
    final String pipeTsFileResourcePipeName =
        PipeTsFileResourceManager.getPipeTsFileResourcePipeName(pipeName, creationTime);
    try {
      PipeDataNodeResourceManager.tsfile()
          .decreaseFileReference(tsFile, pipeTsFileResourcePipeName);
      if (isWithMod) {
        PipeDataNodeResourceManager.tsfile()
            .decreaseFileReference(modFile, pipeTsFileResourcePipeName);
      }
      close();
      return true;
    } catch (final Exception e) {
      LOGGER.warn(
          String.format(
              "Decrease reference count for TsFile %s error. Holder Message: %s",
              tsFile.getPath(), holderMessage),
          e);
      return false;
    } finally {
      if (Objects.nonNull(pipeName)) {
        PipeDataNodeSinglePipeMetrics.getInstance()
            .decreaseTsFileEventCount(
                pipeName,
                creationTime,
                shouldReportOnCommit ? System.nanoTime() - extractTime : -1);
      }
    }
  }

  @Override
  public void bindProgressIndex(final ProgressIndex overridingProgressIndex) {
    this.overridingProgressIndex = overridingProgressIndex;
  }

  @Override
  public ProgressIndex getProgressIndex() {
    return resource.getMaxProgressIndex();
  }

  /**
   * Get ProgressIndex without waiting for tsfile close. Can be used in getting progressIndex when
   * memTable becomes immutable.
   */
  public ProgressIndex forceGetProgressIndex() {
    if (resource.isEmpty()) {
      LOGGER.warn(
          "Skipping temporary TsFile {}'s progressIndex, will report MinimumProgressIndex", tsFile);
      return MinimumProgressIndex.INSTANCE;
    }
    if (Objects.nonNull(overridingProgressIndex)) {
      return overridingProgressIndex;
    }
    return resource.getMaxProgressIndex();
  }

  public void eliminateProgressIndex() {
    if (Objects.isNull(overridingProgressIndex) && Objects.nonNull(resource)) {
      PipeTsFileEpochProgressIndexKeeper.getInstance()
          .eliminateProgressIndex(resource.getDataRegionId(), pipeName, resource.getTsFilePath());
    }
  }

  public String getTsFileParser() {
    return tsFileParser;
  }

  public void setTsFileParser(final String tsFileParser) {
    this.tsFileParser = tsFileParser;
  }

  @Override
  public PipeTsFileInsertionEvent shallowCopySelfAndBindPipeTaskMetaForProgressReport(
      final String pipeName,
      final long creationTime,
      final PipeTaskMeta pipeTaskMeta,
      final PipePattern pattern,
      final long startTime,
      final long endTime) {
    final PipeTsFileInsertionEvent copiedEvent =
        new PipeTsFileInsertionEvent(
            resource,
            tsFile,
            isWithMod,
            isLoaded,
            isGeneratedByHistoricalExtractor,
            pipeName,
            creationTime,
            pipeTaskMeta,
            pattern,
            startTime,
            endTime,
            isTsFileSealed);
    copiedEvent.setTsFileParser(tsFileParser);
    return copiedEvent;
  }

  @Override
  public boolean isGeneratedByPipe() {
    return isGeneratedByPipe;
  }

  @Override
  public boolean mayEventTimeOverlappedWithTimeRange() {
    // Notice that this is only called at realtime extraction, and the tsFile is always closed
    // Thus we can use the end time to judge the overlap
    return Objects.isNull(resource)
        || startTime <= resource.getFileEndTime() && resource.getFileStartTime() <= endTime;
  }

  @Override
  public boolean shouldParseTime() {
    if (!isTimeParsed
        && Objects.nonNull(resource)
        && startTime <= resource.getFileStartTime()
        && resource.getFileEndTime() <= endTime) {
      isTimeParsed = true;
    }
    return !isTimeParsed;
  }

  @Override
  public boolean mayEventPathsOverlappedWithPattern() {
    if (Objects.isNull(resource) || !resource.isClosed()) {
      return true;
    }

    try {
      final Map<IDeviceID, Boolean> deviceIsAlignedMap =
          PipeDataNodeResourceManager.tsfile()
              .getDeviceIsAlignedMapFromCache(
                  PipeTsFileResourceManager.getHardlinkOrCopiedFileInPipeDir(
                      resource.getTsFile(),
                      PipeTsFileResourceManager.getPipeTsFileResourcePipeName(
                          pipeName, creationTime)),
                  false);
      final Set<IDeviceID> deviceSet =
          Objects.nonNull(deviceIsAlignedMap) ? deviceIsAlignedMap.keySet() : resource.getDevices();
      return deviceSet.stream()
          .anyMatch(
              // TODO: use IDeviceID
              deviceID ->
                  pipePattern.mayOverlapWithDevice(((PlainDeviceID) deviceID).toStringID()));
    } catch (final Exception e) {
      LOGGER.info(
          "Pipe {}: failed to get devices from TsFile {}, extract it anyway",
          pipeName,
          resource.getTsFilePath(),
          e);
      return true;
    }
  }

  /////////////////////////// TsFileInsertionEvent ///////////////////////////

  @FunctionalInterface
  public interface TabletInsertionEventConsumer {
    void consume(final PipeRawTabletInsertionEvent event);
  }

  public void consumeTabletInsertionEventsWithRetry(
      final TabletInsertionEventConsumer consumer, final String callerName) throws Exception {
    consumeTabletInsertionEventsWithRetry(
        consumer, callerName, PipeProcessorSubtaskExecutionGuard.disabled());
  }

  public void consumeTabletInsertionEventsWithRetry(
      final TabletInsertionEventConsumer consumer,
      final String callerName,
      final PipeProcessorSubtaskExecutionGuard processorExecutionGuard)
      throws Exception {
    try {
      while (true) {
        processorExecutionGuard.check();
        final PipeRawTabletInsertionEvent parsedEvent =
            getNextTabletInsertionEventFromSavedProgress(processorExecutionGuard);
        if (parsedEvent == null) {
          isTsFileParsingCompleted.set(true);
          releaseTsFileParserMemoryIfReserved();
          return;
        }
        processorExecutionGuard.check();
        consumeParsedTabletInsertionEventWithRetry(
            consumer,
            callerName,
            parsedTabletInsertionEventCount.get(),
            parsedEvent,
            processorExecutionGuard);
        pendingTabletInsertionEvent.compareAndSet(parsedEvent, null);
        processorExecutionGuard.check();
      }
    } catch (final PipeProcessorSubtaskYieldException e) {
      releaseTsFileParserMemoryIfReserved();
      if (!processorExecutionGuard.isCurrentInvocationValid()) {
        cancelTsFileParserMemoryReservationIfPending();
      }
      throw e;
    } catch (final PipeRuntimeOutOfMemoryCriticalException e) {
      // Yield the active parser slot to the next pipe while retaining the iterator and current
      // tablet. The next retry resumes from this exact tablet instead of reparsing the TsFile.
      releaseTsFileParserMemoryIfReserved();
      LOGGER.warn(
          "{}: failed to allocate memory for parsing TsFile {}, tablet event no. {}, will release parser memory and retry the TsFile event later.",
          callerName,
          getTsFile(),
          parsedTabletInsertionEventCount.get(),
          e);
      throw e;
    } catch (final Exception e) {
      releaseTsFileParserMemoryIfReserved();
      throw e;
    }
  }

  private PipeRawTabletInsertionEvent getNextTabletInsertionEventFromSavedProgress(
      final PipeProcessorSubtaskExecutionGuard processorExecutionGuard) throws Exception {
    if (isTsFileParsingCompleted.get()) {
      return null;
    }

    // Reacquire parser memory after a previous failure yielded the active parser slot. Processor
    // subtasks use non-blocking admission here, while other callers retain the bounded wait.
    reserveResource4Parsing(processorExecutionGuard);

    final PipeRawTabletInsertionEvent pendingEvent = pendingTabletInsertionEvent.get();
    if (pendingEvent != null) {
      return pendingEvent;
    }

    Iterator<TabletInsertionEvent> iterator = tabletInsertionEventIterator.get();
    if (iterator == null) {
      if (!waitForTsFileClose(processorExecutionGuard)) {
        LOGGER.warn(
            "Pipe skipping temporary TsFile's parsing which shouldn't be transferred: {}", tsFile);
        return null;
      }
      iterator = initDataContainer().toTabletInsertionEvents().iterator();
      tabletInsertionEventIterator.set(iterator);
    }

    if (!iterator.hasNext()) {
      return null;
    }

    final PipeRawTabletInsertionEvent nextEvent = (PipeRawTabletInsertionEvent) iterator.next();
    pendingTabletInsertionEvent.set(nextEvent);
    parsedTabletInsertionEventCount.incrementAndGet();
    return nextEvent;
  }

  private void consumeParsedTabletInsertionEventWithRetry(
      final TabletInsertionEventConsumer consumer,
      final String callerName,
      final int tabletEventCount,
      final TabletInsertionEvent parsedEvent,
      final PipeProcessorSubtaskExecutionGuard processorExecutionGuard)
      throws Exception {
    final PipeMemoryManager memoryManager = PipeDataNodeResourceManager.memory();
    long firstOutOfMemoryTimeInMs = Long.MIN_VALUE;
    int retryCount = 0;
    while (true) {
      processorExecutionGuard.check();
      try {
        consumer.consume((PipeRawTabletInsertionEvent) parsedEvent);
        return;
      } catch (final PipeRuntimeOutOfMemoryCriticalException e) {
        if (firstOutOfMemoryTimeInMs == Long.MIN_VALUE) {
          firstOutOfMemoryTimeInMs = System.currentTimeMillis();
        }
        if (memoryManager.shouldReleaseTsFileParserOnOutOfMemory(
            firstOutOfMemoryTimeInMs, ++retryCount)) {
          throw e;
        }
        logParserRetryOnOutOfMemory(callerName, tabletEventCount, retryCount, e);
        try {
          sleepForParserRetry(getParserRetryBackoffInMs(retryCount), processorExecutionGuard);
        } catch (final InterruptedException interruptedException) {
          Thread.currentThread().interrupt();
          throw e;
        }
      }
    }
  }

  private void sleepForParserRetry(
      final long sleepTimeInMs, final PipeProcessorSubtaskExecutionGuard processorExecutionGuard)
      throws InterruptedException {
    if (!processorExecutionGuard.isEnabled()) {
      Thread.sleep(sleepTimeInMs);
      return;
    }

    final long deadlineInMs = System.currentTimeMillis() + sleepTimeInMs;
    long remainingTimeInMs = sleepTimeInMs;
    while (remainingTimeInMs > 0) {
      processorExecutionGuard.check();
      Thread.sleep(Math.min(remainingTimeInMs, 100));
      processorExecutionGuard.check();
      remainingTimeInMs = deadlineInMs - System.currentTimeMillis();
    }
  }

  private long getParserRetryBackoffInMs(final int retryCount) {
    final long initialBackoffInMs =
        Math.max(1, PipeConfig.getInstance().getPipeMemoryAllocateRetryIntervalInMs());
    final int maxRetries = Math.max(1, PipeConfig.getInstance().getPipeMemoryAllocateMaxRetries());
    final long maxBackoffInMs =
        initialBackoffInMs > Long.MAX_VALUE / maxRetries
            ? Long.MAX_VALUE
            : initialBackoffInMs * maxRetries;
    long backoffInMs = initialBackoffInMs;
    for (int retry = 1; retry < retryCount && backoffInMs < maxBackoffInMs; retry++) {
      backoffInMs = backoffInMs >= maxBackoffInMs - backoffInMs ? maxBackoffInMs : backoffInMs << 1;
    }
    return backoffInMs;
  }

  private void logParserRetryOnOutOfMemory(
      final String callerName,
      final int tabletEventCount,
      final int retryCount,
      final PipeRuntimeOutOfMemoryCriticalException e) {
    if (retryCount != 1 && retryCount % 10 != 0) {
      return;
    }
    LOGGER.warn(
        "{}: failed to consume parsed tablet from TsFile {}, tablet event no. {}, retry count is {}, will keep parser and retry locally for a short time.",
        callerName,
        getTsFile(),
        tabletEventCount,
        retryCount,
        e);
  }

  private void releaseParsedTabletEvent(final TabletInsertionEvent parsedEvent) {
    if (parsedEvent instanceof PipeRawTabletInsertionEvent
        && ((PipeRawTabletInsertionEvent) parsedEvent).getReferenceCount() == 0
        && !((PipeRawTabletInsertionEvent) parsedEvent).isReleased()) {
      ((PipeRawTabletInsertionEvent) parsedEvent).clearReferenceCount(getClass().getName());
    }
  }

  @Override
  public Iterable<TabletInsertionEvent> toTabletInsertionEvents() throws PipeException {
    // 20 - 40 seconds for waiting
    // Can not be unlimited or will cause deadlock
    return toTabletInsertionEvents((long) ((1 + Math.random()) * 20 * 1000));
  }

  public Iterable<TabletInsertionEvent> toTabletInsertionEvents(final long timeoutMs)
      throws PipeException {
    try {
      if (!waitForTsFileClose()) {
        LOGGER.warn(
            "Pipe skipping temporary TsFile's parsing which shouldn't be transferred: {}", tsFile);
        return Collections.emptyList();
      }
      waitForResourceEnough4Parsing(timeoutMs);
      return initDataContainer().toTabletInsertionEvents();
    } catch (final Exception e) {
      close();

      // close() should be called before re-interrupting the thread
      if (e instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }

      final String errorMsg =
          e instanceof InterruptedException
              ? String.format(
                  "Interrupted when waiting for closing TsFile %s.", resource.getTsFilePath())
              : String.format(
                  "Parse TsFile %s error. Because: %s", resource.getTsFilePath(), e.getMessage());
      if (e instanceof PipeRuntimeOutOfMemoryCriticalException) {
        PipeLogger.log(LOGGER::warn, errorMsg);
      } else {
        PipeLogger.log(LOGGER::warn, e, errorMsg);
      }
      throw new PipeException(errorMsg, e);
    }
  }

  private void reserveResource4Parsing(
      final PipeProcessorSubtaskExecutionGuard processorExecutionGuard)
      throws InterruptedException {
    if (!processorExecutionGuard.isEnabled()) {
      waitForResourceEnough4Parsing((long) ((1 + Math.random()) * 20 * 1000));
      return;
    }

    processorExecutionGuard.check();
    final PipeMemoryManager memoryManager = PipeDataNodeResourceManager.memory();
    if (tryReserveTsFileParserMemory(memoryManager)) {
      try {
        processorExecutionGuard.check();
        return;
      } catch (final PipeProcessorSubtaskYieldException e) {
        releaseTsFileParserMemoryIfReserved();
        throw e;
      }
    }

    if (!processorExecutionGuard.isCurrentInvocationValid()) {
      cancelTsFileParserMemoryReservationIfPending();
      processorExecutionGuard.check();
    }
    processorExecutionGuard.yieldIfParserNotAdmitted();
  }

  private void waitForResourceEnough4Parsing(final long timeoutMs) throws InterruptedException {
    final PipeMemoryManager memoryManager = PipeDataNodeResourceManager.memory();
    if (tryReserveTsFileParserMemory(memoryManager)) {
      return;
    }

    final long startTime = System.currentTimeMillis();
    long lastRecordTime = startTime;

    while (!tryReserveTsFileParserMemory(memoryManager)) {
      final long currentTime = System.currentTimeMillis();
      final long elapsedRecordTimeInMs = currentTime - lastRecordTime;
      final long waitTimeInMs = currentTime - startTime;
      final double waitTimeSeconds = waitTimeInMs / 1000.0;
      if (elapsedRecordTimeInMs > 10_000) {
        LOGGER.info(
            "Wait for memory enough for parsing {} for {} seconds.",
            resource != null ? resource.getTsFilePath() : "tsfile",
            waitTimeSeconds);
        lastRecordTime = currentTime;
      } else if (LOGGER.isDebugEnabled()) {
        LOGGER.debug(
            "Wait for memory enough for parsing {} for {} seconds.",
            resource != null ? resource.getTsFilePath() : "tsfile",
            waitTimeSeconds);
      }

      if (waitTimeInMs > timeoutMs) {
        // should contain 'TimeoutException' in exception message
        throw new PipeRuntimeOutOfMemoryCriticalException(
            String.format(
                "TimeoutException: Waited %s seconds for memory to parse TsFile", waitTimeSeconds));
      }

      tsFileParserMemoryReservationKey.await(
          Math.max(
              1,
              Math.min(
                  timeoutMs - waitTimeInMs, 10_000 - Math.min(10_000, elapsedRecordTimeInMs))));
    }

    final long currentTime = System.currentTimeMillis();
    final double waitTimeSeconds = (currentTime - startTime) / 1000.0;
    LOGGER.info(
        "Wait for memory enough for parsing {} for {} seconds.",
        resource != null ? resource.getTsFilePath() : "tsfile",
        waitTimeSeconds);
  }

  private boolean tryReserveTsFileParserMemory(final PipeMemoryManager memoryManager) {
    synchronized (isTsFileParserMemoryReserved) {
      if (isTsFileParserMemoryReserved.get()) {
        return true;
      }

      if (!memoryManager.tryReserveTsFileParserMemory(
          pipeName, creationTime, dataRegionId, tsFileParserMemoryReservationKey)) {
        return false;
      }

      isTsFileParserMemoryReserved.set(true);
      return true;
    }
  }

  private void releaseTsFileParserMemoryIfReserved() {
    synchronized (isTsFileParserMemoryReserved) {
      if (isTsFileParserMemoryReserved.compareAndSet(true, false)) {
        PipeDataNodeResourceManager.memory()
            .releaseTsFileParserMemory(pipeName, creationTime, dataRegionId);
      }
    }
  }

  public void cancelTsFileParserMemoryReservationIfPending() {
    if (!isTsFileParserMemoryReserved.get()) {
      PipeDataNodeResourceManager.memory()
          .cancelTsFileParserMemoryReservation(
              pipeName, creationTime, dataRegionId, tsFileParserMemoryReservationKey);
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
      dataContainer.compareAndSet(
          null,
          new TsFileInsertionDataContainerProvider(
                  pipeName,
                  creationTime,
                  tsFile,
                  pipePattern,
                  startTime,
                  endTime,
                  pipeTaskMeta,
                  this,
                  tsFileParser)
              .provide(isWithMod));
      return dataContainer.get();
    } catch (final IOException e) {
      close();

      final String errorMsg = String.format("Read TsFile %s error.", tsFile.getPath());
      LOGGER.warn(errorMsg, e);
      throw new PipeException(errorMsg, e);
    }
  }

  public long count(final boolean skipReportOnCommit) throws Exception {
    if (shouldParseTime()) {
      try {
        consumeTabletInsertionEventsWithRetry(
            event -> {
              parsedPointCountForCount.addAndGet(event.count());
              if (skipReportOnCommit) {
                event.skipReportOnCommit();
              }
            },
            "PipeTsFileInsertionEvent::count");
        return parsedPointCountForCount.getAndSet(0);
      } finally {
        if (isTsFileParsingCompleted.get()) {
          close();
        }
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
    cancelTsFileParserMemoryReservationIfPending();
    tabletInsertionEventIterator.set(null);
    releaseParsedTabletEvent(pendingTabletInsertionEvent.getAndSet(null));
    parsedTabletInsertionEventCount.set(0);
    parsedPointCountForCount.set(0);
    isTsFileParsingCompleted.set(false);
    dataContainer.getAndUpdate(
        container -> {
          if (Objects.nonNull(container)) {
            container.close();
          }
          return null;
        });
    releaseTsFileParserMemoryIfReserved();
  }

  /////////////////////////// Object ///////////////////////////

  @Override
  public String toString() {
    return String.format(
            "PipeTsFileInsertionEvent{resource=%s, tsFile=%s, isLoaded=%s, isGeneratedByPipe=%s, dataContainer=%s}",
            resource, tsFile, isLoaded, isGeneratedByPipe, dataContainer)
        + " - "
        + super.toString();
  }

  @Override
  public String coreReportMessage() {
    return String.format(
            "PipeTsFileInsertionEvent{resource=%s, tsFile=%s, isLoaded=%s, isGeneratedByPipe=%s}",
            resource, tsFile, isLoaded, isGeneratedByPipe)
        + " - "
        + super.coreReportMessage();
  }

  /////////////////////////// ReferenceTrackableEvent ///////////////////////////

  @Override
  public void trackResource() {
    PipeDataNodeResourceManager.ref().trackPipeEventResource(this, eventResourceBuilder());
  }

  @Override
  public PipeEventResource eventResourceBuilder() {
    return new PipeTsFileInsertionEventResource(
        this.isReleased,
        this.referenceCount,
        this.pipeName,
        this.creationTime,
        this.dataRegionId,
        this.tsFile,
        this.isWithMod,
        this.modFile,
        this.dataContainer,
        this.isTsFileParserMemoryReserved,
        this.tsFileParserMemoryReservationKey);
  }

  private static class PipeTsFileInsertionEventResource extends PipeEventResource {

    private final File tsFile;
    private final boolean isWithMod;
    private final File modFile;
    private final AtomicReference<TsFileInsertionDataContainer> dataContainer;
    private final String pipeName;
    private final long creationTime;
    private final String dataRegionId;
    private final AtomicBoolean isTsFileParserMemoryReserved;
    private final TsFileParserMemoryReservation tsFileParserMemoryReservationKey;

    private PipeTsFileInsertionEventResource(
        final AtomicBoolean isReleased,
        final AtomicInteger referenceCount,
        final String pipeName,
        final long creationTime,
        final String dataRegionId,
        final File tsFile,
        final boolean isWithMod,
        final File modFile,
        final AtomicReference<TsFileInsertionDataContainer> dataContainer,
        final AtomicBoolean isTsFileParserMemoryReserved,
        final TsFileParserMemoryReservation tsFileParserMemoryReservationKey) {
      super(isReleased, referenceCount);
      this.pipeName = pipeName;
      this.creationTime = creationTime;
      this.dataRegionId = dataRegionId;
      this.tsFile = tsFile;
      this.isWithMod = isWithMod;
      this.modFile = modFile;
      this.dataContainer = dataContainer;
      this.isTsFileParserMemoryReserved = isTsFileParserMemoryReserved;
      this.tsFileParserMemoryReservationKey = tsFileParserMemoryReservationKey;
    }

    @Override
    protected void finalizeResource() {
      try {
        PipeDataNodeResourceManager.memory()
            .cancelTsFileParserMemoryReservation(
                pipeName, creationTime, dataRegionId, tsFileParserMemoryReservationKey);
        final String pipeTsFileResourcePipeName =
            PipeTsFileResourceManager.getPipeTsFileResourcePipeName(pipeName, creationTime);
        // decrease reference count
        PipeDataNodeResourceManager.tsfile()
            .decreaseFileReference(tsFile, pipeTsFileResourcePipeName);
        if (isWithMod) {
          PipeDataNodeResourceManager.tsfile()
              .decreaseFileReference(modFile, pipeTsFileResourcePipeName);
        }

        // close data container
        dataContainer.getAndUpdate(
            container -> {
              if (Objects.nonNull(container)) {
                container.close();
              }
              return null;
            });
        synchronized (isTsFileParserMemoryReserved) {
          if (isTsFileParserMemoryReserved.compareAndSet(true, false)) {
            PipeDataNodeResourceManager.memory()
                .releaseTsFileParserMemory(pipeName, creationTime, dataRegionId);
          }
        }
      } catch (final Exception e) {
        LOGGER.warn("Decrease reference count for TsFile {} error.", tsFile.getPath(), e);
      }
    }
  }
}
