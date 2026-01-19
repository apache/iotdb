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

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.audit.UserEntity;
import org.apache.iotdb.commons.auth.entity.PrivilegeType;
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.consensus.index.ProgressIndex;
import org.apache.iotdb.commons.consensus.index.impl.MinimumProgressIndex;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.exception.auth.AccessDeniedException;
import org.apache.iotdb.commons.exception.pipe.PipeRuntimeOutOfMemoryCriticalException;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeTaskMeta;
import org.apache.iotdb.commons.pipe.config.PipeConfig;
import org.apache.iotdb.commons.pipe.datastructure.pattern.TablePattern;
import org.apache.iotdb.commons.pipe.datastructure.pattern.TreePattern;
import org.apache.iotdb.commons.pipe.resource.ref.PipePhantomReferenceManager.PipeEventResource;
import org.apache.iotdb.db.auth.AuthorityChecker;
import org.apache.iotdb.db.pipe.event.ReferenceTrackableEvent;
import org.apache.iotdb.db.pipe.event.common.PipeInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeRawTabletInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.tsfile.aggregator.TsFileInsertionPointCounter;
import org.apache.iotdb.db.pipe.event.common.tsfile.parser.TsFileInsertionEventParser;
import org.apache.iotdb.db.pipe.event.common.tsfile.parser.TsFileInsertionEventParserProvider;
import org.apache.iotdb.db.pipe.metric.overview.PipeDataNodeSinglePipeMetrics;
import org.apache.iotdb.db.pipe.resource.PipeDataNodeResourceManager;
import org.apache.iotdb.db.pipe.resource.memory.PipeMemoryManager;
import org.apache.iotdb.db.pipe.resource.tsfile.PipeTsFileResourceManager;
import org.apache.iotdb.db.pipe.source.dataregion.realtime.assigner.PipeTsFileEpochProgressIndexKeeper;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.QualifiedObjectName;
import org.apache.iotdb.db.storageengine.dataregion.memtable.TsFileProcessor;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;
import org.apache.iotdb.pipe.api.event.dml.insertion.TsFileInsertionEvent;
import org.apache.iotdb.pipe.api.exception.PipeException;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.file.metadata.IDeviceID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class PipeTsFileInsertionEvent extends PipeInsertionEvent
    implements TsFileInsertionEvent, ReferenceTrackableEvent {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeTsFileInsertionEvent.class);

  private final TsFileResource resource;
  private File tsFile;
  private long extractTime = 0;

  // This is true iff the modFile exists and should be transferred
  private boolean isWithMod;
  private File modFile;
  private final File sharedModFile;

  protected final boolean isLoaded;
  protected final boolean isGeneratedByPipe;
  protected final boolean isGeneratedByPipeConsensus;
  protected final boolean isGeneratedByHistoricalExtractor;
  private final AtomicBoolean isClosed;
  private final AtomicReference<TsFileInsertionEventParser> eventParser;

  // The point count of the TsFile. Used for metrics on PipeConsensus' receiver side.
  // May be updated after it is flushed. Should be negative if not set.
  protected long flushPointCount = TsFileProcessor.FLUSH_POINT_COUNT_NOT_SET;

  protected volatile ProgressIndex overridingProgressIndex;
  private Set<String> tableNames;

  // This is set to check the tsFile paths by privilege
  private Map<IDeviceID, String[]> treeSchemaMap;

  public PipeTsFileInsertionEvent(
      final Boolean isTableModelEvent,
      final String databaseNameFromDataRegion,
      final TsFileResource resource,
      final boolean isLoaded) {
    // The modFile must be copied before the event is assigned to the listening pipes
    this(
        isTableModelEvent,
        databaseNameFromDataRegion,
        resource,
        null,
        true,
        isLoaded,
        false,
        null,
        null,
        0,
        null,
        null,
        null,
        null,
        null,
        null,
        true,
        Long.MIN_VALUE,
        Long.MAX_VALUE);
  }

  public PipeTsFileInsertionEvent(
      final Boolean isTableModelEvent,
      final String databaseNameFromDataRegion,
      final TsFileResource resource,
      final File tsFile,
      final boolean isWithMod,
      final boolean isLoaded,
      final boolean isGeneratedByHistoricalExtractor,
      final Set<String> tableNames,
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
        endTime,
        isTableModelEvent,
        databaseNameFromDataRegion);

    this.resource = resource;

    // For events created at assigner or historical extractor, the tsFile is get from the resource
    // For events created for source, the tsFile is inherited from the assigner, because the
    // original tsFile may be gone, and we need to get the assigner's hard-linked tsFile to
    // hard-link it to each pipe dir
    this.tsFile = Objects.isNull(tsFile) ? resource.getTsFile() : tsFile;

    this.isWithMod = isWithMod && resource.anyModFileExists();
    this.modFile = this.isWithMod ? resource.getExclusiveModFile().getFile() : null;
    // TODO: process the shared mod file
    this.sharedModFile =
        resource.getSharedModFile() != null ? resource.getSharedModFile().getFile() : null;

    this.isLoaded = isLoaded;
    this.isGeneratedByPipe = resource.isGeneratedByPipe();
    this.isGeneratedByPipeConsensus = resource.isGeneratedByPipeConsensus();
    this.isGeneratedByHistoricalExtractor = isGeneratedByHistoricalExtractor;
    this.tableNames = tableNames;

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

    this.eventParser = new AtomicReference<>(null);

    addOnCommittedHook(
        () -> {
          if (shouldReportOnCommit) {
            eliminateProgressIndex();
          }
        });
  }

  /**
   * @return {@code false} if this file can't be sent by pipe because it is empty. {@code true}
   *     otherwise.
   */
  public boolean waitForTsFileClose() throws InterruptedException {
    if (Objects.isNull(resource)) {
      return true;
    }

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

  @Override
  public File getTsFile() {
    return tsFile;
  }

  public File getModFile() {
    return modFile;
  }

  public File getSharedModFile() {
    return sharedModFile;
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
    try {
      tsFile = PipeDataNodeResourceManager.tsfile().increaseFileReference(tsFile, true, pipeName);
      if (isWithMod) {
        modFile =
            PipeDataNodeResourceManager.tsfile().increaseFileReference(modFile, false, pipeName);
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
    try {
      PipeDataNodeResourceManager.tsfile().decreaseFileReference(tsFile, pipeName);
      if (isWithMod) {
        PipeDataNodeResourceManager.tsfile().decreaseFileReference(modFile, pipeName);
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

  @Override
  public PipeTsFileInsertionEvent shallowCopySelfAndBindPipeTaskMetaForProgressReport(
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
    return new PipeTsFileInsertionEvent(
        getRawIsTableModelEvent(),
        getSourceDatabaseNameFromDataRegion(),
        resource,
        tsFile,
        isWithMod,
        isLoaded,
        isGeneratedByHistoricalExtractor,
        tableNames,
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
  public boolean isGeneratedByPipe() {
    return isGeneratedByPipe;
  }

  @Override
  public void throwIfNoPrivilege() {
    try {
      if (AuthorityChecker.SUPER_USER.equals(userName)) {
        return;
      }
      if (!waitForTsFileClose()) {
        LOGGER.info("Temporary tsFile {} detected, will skip its transfer.", tsFile);
        return;
      }
      if (isTableModelEvent()) {
        for (final String table : tableNames) {
          if (!tablePattern.matchesDatabase(getTableModelDatabaseName())
              || !tablePattern.matchesTable(table)) {
            continue;
          }
          if (!AuthorityChecker.getAccessControl()
              .checkCanSelectFromTable4Pipe(
                  userName,
                  new QualifiedObjectName(getTableModelDatabaseName(), table),
                  new UserEntity(Long.parseLong(userId), userName, cliHostname))) {
            if (skipIfNoPrivileges) {
              shouldParse4Privilege = true;
            } else {
              throw new AccessDeniedException(
                  String.format(
                      "No privilege for SELECT for user %s at table %s.%s",
                      userName, tableModelDatabaseName, table));
            }
          }
        }
      }
      // Real-time tsFiles
      else if (Objects.nonNull(treeSchemaMap)) {
        final List<MeasurementPath> measurementList = new ArrayList<>();
        for (final Map.Entry<IDeviceID, String[]> entry : treeSchemaMap.entrySet()) {
          final IDeviceID deviceID = entry.getKey();
          for (final String measurement : entry.getValue()) {
            if (treePattern.matchesMeasurement(deviceID, measurement)) {
              measurementList.add(new MeasurementPath(deviceID, measurement));
            }
          }
        }
        final TSStatus status =
            AuthorityChecker.getAccessControl()
                .checkSeriesPrivilege4Pipe(
                    new UserEntity(Long.parseLong(userId), userName, cliHostname),
                    measurementList,
                    PrivilegeType.READ_DATA);
        if (TSStatusCode.SUCCESS_STATUS.getStatusCode() != status.getCode()) {
          if (skipIfNoPrivileges) {
            shouldParse4Privilege = true;
          } else {
            throw new AccessDeniedException(status.getMessage());
          }
        }
      }
      // Historical tsFiles
      // Coarse filter, will be judged in inner class
      else {
        final Set<IDeviceID> devices = getDeviceSet();
        if (Objects.nonNull(devices)) {
          final List<MeasurementPath> measurementList = new ArrayList<>();
          for (final IDeviceID device : devices) {
            if (treePattern.mayOverlapWithDevice(device)) {
              measurementList.add(
                  new MeasurementPath(device, IoTDBConstant.ONE_LEVEL_PATH_WILDCARD));
            }
          }
          final TSStatus status =
              AuthorityChecker.getAccessControl()
                  .checkSeriesPrivilege4Pipe(
                      new UserEntity(Long.parseLong(userId), userName, cliHostname),
                      measurementList,
                      PrivilegeType.READ_DATA);
          if (TSStatusCode.SUCCESS_STATUS.getStatusCode() != status.getCode()) {
            // Note that this is only coarse filter, thus the exception cannot be thrown here.
            // The actual process is in the event parser
            shouldParse4Privilege = true;
          }
        } else {
          shouldParse4Privilege = true;
        }
      }
    } catch (final AccessDeniedException | PipeRuntimeOutOfMemoryCriticalException e) {
      throw e;
    } catch (final Exception e) {
      if (e instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }

      final String errorMsg =
          e instanceof InterruptedException
              ? String.format(
                  "Interrupted when waiting for parsing privilege for TsFile %s.",
                  resource.getTsFilePath())
              : String.format(
                  "Parse TsFile %s when checking privilege error. Because: %s",
                  resource.getTsFilePath(), e.getMessage());
      LOGGER.warn(errorMsg, e);
      throw new PipeException(errorMsg, e);
    } finally {
      // GC useless
      tableNames = null;
      treeSchemaMap = null;
    }
  }

  @Override
  public boolean mayEventTimeOverlappedWithTimeRange() {
    // Notice that this is only called at realtime extraction, and the tsFile is always closed
    // Thus we can use the end time to judge the overlap
    return Objects.isNull(resource)
        || startTime <= resource.getFileEndTime() && resource.getFileStartTime() <= endTime;
  }

  @Override
  public boolean mayEventPathsOverlappedWithPattern() {
    if (Objects.isNull(resource) || !resource.isClosed() || isTableModelEvent()) {
      return true;
    }

    try {
      return getDeviceSet().stream().anyMatch(treePattern::mayOverlapWithDevice);
    } catch (final Exception e) {
      LOGGER.info(
          "Pipe {}: failed to get devices from TsFile {}, extract it anyway",
          pipeName,
          resource.getTsFilePath(),
          e);
      return true;
    }
  }

  private Set<IDeviceID> getDeviceSet() throws IOException {
    final Map<IDeviceID, Boolean> deviceIsAlignedMap =
        PipeDataNodeResourceManager.tsfile()
            .getDeviceIsAlignedMapFromCache(
                PipeTsFileResourceManager.getHardlinkOrCopiedFileInPipeDir(
                    resource.getTsFile(), pipeName),
                false);
    if (Objects.nonNull(deviceIsAlignedMap)) {
      return deviceIsAlignedMap.keySet();
    }
    try {
      return resource.getDevices();
    } catch (final Exception e) {
      return null;
    }
  }

  public void setTableNames(final Set<String> tableNames) {
    this.tableNames = tableNames;
  }

  public void setTreeSchemaMap(final Map<IDeviceID, String[]> treeSchemaMap) {
    this.treeSchemaMap = treeSchemaMap;
  }

  /////////////////////////// PipeInsertionEvent ///////////////////////////

  @Override
  public boolean isTableModelEvent() {
    if (getRawIsTableModelEvent() == null) {
      if (getSourceDatabaseNameFromDataRegion() != null) {
        return super.isTableModelEvent();
      }
    }

    return getRawIsTableModelEvent();
  }

  /////////////////////////// TsFileInsertionEvent ///////////////////////////

  @FunctionalInterface
  public interface TabletInsertionEventConsumer {
    void consume(final PipeRawTabletInsertionEvent event) throws IllegalPathException;
  }

  public void consumeTabletInsertionEventsWithRetry(
      final TabletInsertionEventConsumer consumer, final String callerName) throws Exception {
    final Iterable<TabletInsertionEvent> iterable = toTabletInsertionEvents();
    final Iterator<TabletInsertionEvent> iterator = iterable.iterator();
    int tabletEventCount = 0;
    while (iterator.hasNext()) {
      final TabletInsertionEvent parsedEvent = iterator.next();
      tabletEventCount++;
      int retryCount = 0;
      while (true) {
        // If failed due do insufficient memory, retry until success to avoid race among multiple
        // processor threads
        try {
          consumer.consume((PipeRawTabletInsertionEvent) parsedEvent);
          break;
        } catch (final PipeRuntimeOutOfMemoryCriticalException e) {
          if (retryCount++ % 100 == 0) {
            LOGGER.warn(
                "{}: failed to allocate memory for parsing TsFile {}, tablet event no. {}, retry count is {}, will keep retrying.",
                callerName,
                getTsFile(),
                tabletEventCount,
                retryCount,
                e);
          } else if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(
                "{}: failed to allocate memory for parsing TsFile {}, tablet event no. {}, retry count is {}, will keep retrying.",
                callerName,
                getTsFile(),
                tabletEventCount,
                retryCount,
                e);
          }
        }
      }
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
      return initEventParser().toTabletInsertionEvents();
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
      LOGGER.warn(errorMsg, e);
      throw new PipeException(errorMsg, e);
    }
  }

  private void waitForResourceEnough4Parsing(final long timeoutMs) throws InterruptedException {
    final PipeMemoryManager memoryManager = PipeDataNodeResourceManager.memory();
    if (memoryManager.isEnough4TabletParsing()) {
      return;
    }

    final long startTime = System.currentTimeMillis();
    long lastRecordTime = startTime;

    final long memoryCheckIntervalMs =
        PipeConfig.getInstance().getPipeCheckMemoryEnoughIntervalMs();
    while (!memoryManager.isEnough4TabletParsing()) {
      Thread.sleep(memoryCheckIntervalMs);

      final long currentTime = System.currentTimeMillis();
      final double elapsedRecordTimeSeconds = (currentTime - lastRecordTime) / 1000.0;
      final double waitTimeSeconds = (currentTime - startTime) / 1000.0;
      if (elapsedRecordTimeSeconds > 10.0) {
        LOGGER.info(
            "Wait for resource enough for parsing {} for {} seconds.",
            resource != null ? resource.getTsFilePath() : "tsfile",
            waitTimeSeconds);
        lastRecordTime = currentTime;
      } else if (LOGGER.isDebugEnabled()) {
        LOGGER.debug(
            "Wait for resource enough for parsing {} for {} seconds.",
            resource != null ? resource.getTsFilePath() : "tsfile",
            waitTimeSeconds);
      }

      if (waitTimeSeconds * 1000 > timeoutMs) {
        // should contain 'TimeoutException' in exception message
        throw new PipeException(
            String.format("TimeoutException: Waited %s seconds", waitTimeSeconds));
      }
    }

    final long currentTime = System.currentTimeMillis();
    final double waitTimeSeconds = (currentTime - startTime) / 1000.0;
    LOGGER.info(
        "Wait for resource enough for parsing {} for {} seconds.",
        resource != null ? resource.getTsFilePath() : "tsfile",
        waitTimeSeconds);
  }

  /** The method is used to prevent circular replication in PipeConsensus */
  public boolean isGeneratedByPipeConsensus() {
    return isGeneratedByPipeConsensus;
  }

  public boolean isGeneratedByHistoricalExtractor() {
    return isGeneratedByHistoricalExtractor;
  }

  private TsFileInsertionEventParser initEventParser() {
    try {
      eventParser.compareAndSet(
          null,
          new TsFileInsertionEventParserProvider(
                  pipeName,
                  creationTime,
                  tsFile,
                  treePattern,
                  tablePattern,
                  startTime,
                  endTime,
                  pipeTaskMeta,
                  // Do not parse privilege if it should not be parsed
                  // To avoid renaming of the tsFile database
                  shouldParse4Privilege
                      ? new UserEntity(Long.parseLong(userId), userName, cliHostname)
                      : null,
                  this)
              .provide(isWithMod));
      return eventParser.get();
    } catch (final Exception e) {
      close();

      final String errorMsg = String.format("Read TsFile %s error.", tsFile.getPath());
      LOGGER.warn(errorMsg, e);
      throw new PipeException(errorMsg, e);
    }
  }

  public long count(final boolean skipReportOnCommit) throws Exception {
    AtomicLong count = new AtomicLong();

    if (shouldParseTime()) {
      try {
        consumeTabletInsertionEventsWithRetry(
            event -> {
              count.addAndGet(event.count());
              if (skipReportOnCommit) {
                event.skipReportOnCommit();
              }
            },
            "PipeTsFileInsertionEvent::count");
        return count.get();
      } finally {
        close();
      }
    }

    try (final TsFileInsertionPointCounter counter =
        new TsFileInsertionPointCounter(tsFile, treePattern)) {
      return counter.count();
    }
  }

  /** Release the resource of {@link TsFileInsertionEventParser}. */
  @Override
  public void close() {
    eventParser.getAndUpdate(
        parser -> {
          if (Objects.nonNull(parser)) {
            parser.close();
          }
          return null;
        });
  }

  /////////////////////////// Object ///////////////////////////

  @Override
  public String toString() {
    return String.format(
            "PipeTsFileInsertionEvent{resource=%s, tsFile=%s, isLoaded=%s, isGeneratedByPipe=%s, isClosed=%s, eventParser=%s}",
            resource, tsFile, isLoaded, isGeneratedByPipe, isClosed.get(), eventParser)
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
        this.tsFile,
        this.isWithMod,
        this.modFile,
        this.sharedModFile,
        this.eventParser);
  }

  private static class PipeTsFileInsertionEventResource extends PipeEventResource {

    private final File tsFile;
    private final boolean isWithMod;
    private final File modFile;
    private final File sharedModFile; // unused now
    private final AtomicReference<TsFileInsertionEventParser> eventParser;
    private final String pipeName;

    private PipeTsFileInsertionEventResource(
        final AtomicBoolean isReleased,
        final AtomicInteger referenceCount,
        final String pipeName,
        final File tsFile,
        final boolean isWithMod,
        final File modFile,
        final File sharedModFile,
        final AtomicReference<TsFileInsertionEventParser> eventParser) {
      super(isReleased, referenceCount);
      this.pipeName = pipeName;
      this.tsFile = tsFile;
      this.isWithMod = isWithMod;
      this.modFile = modFile;
      this.sharedModFile = sharedModFile;
      this.eventParser = eventParser;
    }

    @Override
    protected void finalizeResource() {
      try {
        // decrease reference count
        PipeDataNodeResourceManager.tsfile().decreaseFileReference(tsFile, pipeName);
        if (isWithMod) {
          PipeDataNodeResourceManager.tsfile().decreaseFileReference(modFile, pipeName);
        }

        // close event parser
        eventParser.getAndUpdate(
            parser -> {
              if (Objects.nonNull(parser)) {
                parser.close();
              }
              return null;
            });
      } catch (final Exception e) {
        LOGGER.warn("Decrease reference count for TsFile {} error.", tsFile.getPath(), e);
      }
    }
  }
}
