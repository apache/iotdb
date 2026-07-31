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
import org.apache.iotdb.commons.pipe.resource.log.PipeLogger;
import org.apache.iotdb.commons.pipe.resource.ref.PipePhantomReferenceManager.PipeEventResource;
import org.apache.iotdb.commons.queryengine.plan.relational.metadata.QualifiedObjectName;
import org.apache.iotdb.db.auth.AuthorityChecker;
import org.apache.iotdb.db.i18n.DataNodePipeMessages;
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
import java.util.concurrent.ThreadLocalRandom;
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
  protected final boolean isGeneratedByIoTConsensusV2;
  protected final boolean isGeneratedByHistoricalExtractor;
  // Realtime TsFile events are created after TsFileProcessor#endFile(), so the file is already
  // immutable even if TsFileResource status is still UNCLOSED.
  private final boolean isTsFileSealed;
  private final AtomicBoolean isClosed;
  private final AtomicReference<TsFileInsertionEventParser> eventParser;
  private final AtomicBoolean isTsFileParserMemoryReserved = new AtomicBoolean(false);
  private final AtomicReference<Iterator<TabletInsertionEvent>> tabletInsertionEventIterator =
      new AtomicReference<>();
  private final AtomicReference<PipeRawTabletInsertionEvent> pendingTabletInsertionEvent =
      new AtomicReference<>();
  private final AtomicInteger parsedTabletInsertionEventCount = new AtomicInteger(0);
  private final AtomicBoolean isTsFileParsingCompleted = new AtomicBoolean(false);
  private final AtomicLong parsedPointCountForCount = new AtomicLong(0);

  // The point count of the TsFile. Used for metrics on IoTConsensusV2' receiver side.
  // May be updated after it is flushed. Should be negative if not set.
  protected long flushPointCount = TsFileProcessor.FLUSH_POINT_COUNT_NOT_SET;

  protected volatile ProgressIndex overridingProgressIndex;
  private Set<String> tableNames;
  private String tsFileDedupScopeID;
  // False when generated tablet events should wait for an external progress report.
  private volatile boolean shouldReportGeneratedEventsOnCommit = true;

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
        Long.MAX_VALUE,
        true);
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
    this(
        isTableModelEvent,
        databaseNameFromDataRegion,
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
        endTime,
        false);
  }

  private PipeTsFileInsertionEvent(
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
      final long endTime,
      final boolean isTsFileSealed) {
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
    this.isGeneratedByIoTConsensusV2 = resource.isGeneratedByIoTConsensusV2();
    this.isGeneratedByHistoricalExtractor = isGeneratedByHistoricalExtractor;
    this.isTsFileSealed = isTsFileSealed;
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

    if (isTsFileSealed) {
      return !resource.isEmpty();
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
   * Only used for metrics on IoTConsensusV2' receiver side. If the event is recovered after data
   * node's restart, the flushPointCount can be not set. It's totally fine for the IoTConsensusV2'
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
              DataNodePipeMessages.INCREASE_REFERENCE_COUNT_TSFILE_OR_MODFILE_ERROR_HOLDER_FMT,
              tsFile,
              modFile,
              holderMessage),
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
              DataNodePipeMessages.DECREASE_REFERENCE_COUNT_TSFILE_ERROR_HOLDER_FMT,
              tsFile.getPath(),
              holderMessage),
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
          DataNodePipeMessages.SKIPPING_TEMPORARY_TSFILE_S_PROGRESSINDEX_WILL_REPORT, tsFile);
      return MinimumProgressIndex.INSTANCE;
    }
    if (Objects.nonNull(overridingProgressIndex)) {
      return overridingProgressIndex;
    }
    return resource.getMaxProgressIndex();
  }

  public PipeTsFileInsertionEvent skipReportOnCommitAndGeneratedEvents() {
    return setShouldReportGeneratedEventsOnCommit(false);
  }

  public boolean shouldReportGeneratedEventsOnCommit() {
    return shouldReportGeneratedEventsOnCommit;
  }

  private PipeTsFileInsertionEvent setShouldReportGeneratedEventsOnCommit(
      final boolean shouldReportGeneratedEventsOnCommit) {
    this.shouldReportGeneratedEventsOnCommit = shouldReportGeneratedEventsOnCommit;
    if (!shouldReportGeneratedEventsOnCommit) {
      skipReportOnCommit();
    }
    return this;
  }

  public void eliminateProgressIndex() {
    if (Objects.isNull(overridingProgressIndex)
        && Objects.nonNull(resource)
        && Objects.nonNull(tsFileDedupScopeID)) {
      PipeTsFileEpochProgressIndexKeeper.getInstance()
          .eliminateProgressIndex(
              Integer.parseInt(resource.getDataRegionId()),
              tsFileDedupScopeID,
              resource.getTsFilePath());
    }
  }

  public PipeTsFileInsertionEvent bindTsFileDedupScopeID(final String tsFileDedupScopeID) {
    this.tsFileDedupScopeID = tsFileDedupScopeID;
    return this;
  }

  public String getTsFileDedupScopeID() {
    return tsFileDedupScopeID;
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
            endTime,
            isTsFileSealed)
        .bindTsFileDedupScopeID(tsFileDedupScopeID)
        .setShouldReportGeneratedEventsOnCommit(shouldReportGeneratedEventsOnCommit);
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
        LOGGER.info(DataNodePipeMessages.TEMPORARY_TSFILE_DETECTED_WILL_SKIP_ITS_TRANSFER, tsFile);
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
                      DataNodePipeMessages
                          .PIPE_EXCEPTION_NO_PRIVILEGE_FOR_SELECT_FOR_USER_S_AT_TABLE_S_S_84B0C299,
                      userName,
                      tableModelDatabaseName,
                      table));
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
                  DataNodePipeMessages.INTERRUPTED_WHEN_WAITING_FOR_PARSING_PRIVILEGE_FOR_TSFILE,
                  resource.getTsFilePath())
              : String.format(
                  DataNodePipeMessages.PARSE_TSFILE_WHEN_CHECKING_PRIVILEGE_ERROR,
                  resource.getTsFilePath(),
                  e.getMessage());
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
    if (Objects.isNull(resource) || !resource.isClosed() || isTableModelEvent()) {
      return true;
    }

    try {
      return getDeviceSet().stream().anyMatch(treePattern::mayOverlapWithDevice);
    } catch (final Exception e) {
      LOGGER.info(
          DataNodePipeMessages.PIPE_FAILED_TO_GET_DEVICES_FROM_TSFILE,
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
                    resource.getTsFile(),
                    PipeTsFileResourceManager.getPipeTsFileResourcePipeName(
                        pipeName, creationTime)),
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
    try {
      while (true) {
        final PipeRawTabletInsertionEvent parsedEvent =
            getNextTabletInsertionEventFromSavedProgress();
        if (parsedEvent == null) {
          isTsFileParsingCompleted.set(true);
          releaseTsFileParserMemoryIfReserved();
          return;
        }
        consumeParsedTabletInsertionEventWithRetry(
            consumer, callerName, parsedTabletInsertionEventCount.get(), parsedEvent);
        pendingTabletInsertionEvent.compareAndSet(parsedEvent, null);
      }
    } catch (final PipeRuntimeOutOfMemoryCriticalException e) {
      // Yield the active parser slot to the next pipe while retaining the iterator and current
      // tablet. The next retry resumes from this exact tablet instead of reparsing the TsFile.
      releaseTsFileParserMemoryIfReserved();
      LOGGER.warn(
          DataNodePipeMessages.FAILED_TO_ALLOCATE_MEMORY_FOR_PARSING_TSFILE,
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

  private PipeRawTabletInsertionEvent getNextTabletInsertionEventFromSavedProgress()
      throws Exception {
    if (isTsFileParsingCompleted.get()) {
      return null;
    }

    // Reacquire parser memory after a previous failure yielded the active parser slot. This wait
    // is already bounded to 20-40 seconds, while the exponential backoff below is only for retrying
    // the current tablet without yielding its parser slot.
    waitForResourceEnough4Parsing((long) ((1 + Math.random()) * 20 * 1000));

    final PipeRawTabletInsertionEvent pendingEvent = pendingTabletInsertionEvent.get();
    if (pendingEvent != null) {
      return pendingEvent;
    }

    Iterator<TabletInsertionEvent> iterator = tabletInsertionEventIterator.get();
    if (iterator == null) {
      if (!waitForTsFileClose()) {
        LOGGER.warn(DataNodePipeMessages.PIPE_SKIPPING_TEMPORARY_TSFILE_S_PARSING_WHICH, tsFile);
        return null;
      }
      iterator = initEventParser().toTabletInsertionEvents().iterator();
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
      final TabletInsertionEvent parsedEvent)
      throws Exception {
    final PipeMemoryManager memoryManager = PipeDataNodeResourceManager.memory();
    long firstOutOfMemoryTimeInMs = Long.MIN_VALUE;
    int retryCount = 0;
    while (true) {
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
          Thread.sleep(getParserRetryBackoffInMs(retryCount));
        } catch (final InterruptedException interruptedException) {
          Thread.currentThread().interrupt();
          throw e;
        }
      }
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
        DataNodePipeMessages.FAILED_TO_CONSUME_PARSED_TABLET_FROM_TSFILE_KEEP_PARSER,
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
        LOGGER.warn(DataNodePipeMessages.PIPE_SKIPPING_TEMPORARY_TSFILE_S_PARSING_WHICH, tsFile);
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
                  DataNodePipeMessages.INTERRUPTED_WHEN_WAITING_FOR_CLOSING_TSFILE,
                  resource.getTsFilePath())
              : String.format(
                  DataNodePipeMessages.PARSE_TSFILE_ERROR_BECAUSE,
                  resource.getTsFilePath(),
                  e.getMessage());
      if (e instanceof PipeRuntimeOutOfMemoryCriticalException) {
        PipeLogger.log(LOGGER::warn, errorMsg);
      } else {
        PipeLogger.log(LOGGER::warn, e, errorMsg);
      }
      throw new PipeException(errorMsg, e);
    }
  }

  private void waitForResourceEnough4Parsing(final long timeoutMs) throws InterruptedException {
    final PipeMemoryManager memoryManager = PipeDataNodeResourceManager.memory();
    if (tryReserveTsFileParserMemory(memoryManager)) {
      return;
    }

    final long startTime = System.currentTimeMillis();
    long lastRecordTime = startTime;

    final long initialMemoryCheckIntervalMs =
        Math.max(1, PipeConfig.getInstance().getPipeCheckMemoryEnoughIntervalMs());
    final long maxMemoryCheckIntervalMs =
        getMaxMemoryCheckIntervalMs(
            initialMemoryCheckIntervalMs,
            PipeConfig.getInstance().getPipeMemoryAllocateMaxRetries());
    long memoryCheckIntervalMs = initialMemoryCheckIntervalMs;
    while (true) {
      final long elapsedTimeMs = Math.max(0, System.currentTimeMillis() - startTime);
      if (elapsedTimeMs >= timeoutMs) {
        // should contain 'TimeoutException' in exception message
        throw new PipeRuntimeOutOfMemoryCriticalException(
            String.format(
                DataNodePipeMessages
                    .PIPE_EXCEPTION_TIMEOUTEXCEPTION_WAITED_S_SECONDS_FOR_MEMORY_TO_PARSE_TSFILE_0E4EF8FD,
                elapsedTimeMs / 1000.0));
      }

      memoryManager.waitForTsFileParserMemory(
          Math.min(
              getMemoryCheckIntervalWithJitter(memoryCheckIntervalMs), timeoutMs - elapsedTimeMs));

      final long currentTime = System.currentTimeMillis();
      final double elapsedRecordTimeSeconds = (currentTime - lastRecordTime) / 1000.0;
      final double waitTimeSeconds = (currentTime - startTime) / 1000.0;
      if (elapsedRecordTimeSeconds > 10.0) {
        LOGGER.info(
            DataNodePipeMessages.WAIT_FOR_MEMORY_ENOUGH_FOR_PARSING_FOR,
            resource != null ? resource.getTsFilePath() : "tsfile",
            waitTimeSeconds);
        lastRecordTime = currentTime;
      } else if (LOGGER.isDebugEnabled()) {
        LOGGER.debug(
            DataNodePipeMessages.WAIT_FOR_MEMORY_ENOUGH_FOR_PARSING_FOR,
            resource != null ? resource.getTsFilePath() : "tsfile",
            waitTimeSeconds);
      }

      if (tryReserveTsFileParserMemory(memoryManager)) {
        LOGGER.info(
            DataNodePipeMessages.WAIT_FOR_MEMORY_ENOUGH_FOR_PARSING_FOR,
            resource != null ? resource.getTsFilePath() : "tsfile",
            waitTimeSeconds);
        return;
      }

      memoryCheckIntervalMs =
          getNextMemoryCheckIntervalMs(memoryCheckIntervalMs, maxMemoryCheckIntervalMs);
    }
  }

  static long getMaxMemoryCheckIntervalMs(final long initialIntervalMs, final int maxRetries) {
    final long multiplier = Math.max(1, maxRetries);
    return initialIntervalMs > Long.MAX_VALUE / multiplier
        ? Long.MAX_VALUE
        : initialIntervalMs * multiplier;
  }

  static long getNextMemoryCheckIntervalMs(final long currentIntervalMs, final long maxIntervalMs) {
    return currentIntervalMs >= maxIntervalMs - currentIntervalMs
        ? maxIntervalMs
        : currentIntervalMs << 1;
  }

  static long getMemoryCheckIntervalWithJitter(final long intervalMs) {
    return Math.max(
        1, (long) (intervalMs * (0.5 + ThreadLocalRandom.current().nextDouble() * 0.5)));
  }

  private boolean tryReserveTsFileParserMemory(final PipeMemoryManager memoryManager) {
    synchronized (isTsFileParserMemoryReserved) {
      if (isTsFileParserMemoryReserved.get()) {
        return true;
      }

      if (!memoryManager.tryReserveTsFileParserMemory()) {
        return false;
      }

      isTsFileParserMemoryReserved.set(true);
      return true;
    }
  }

  private void releaseTsFileParserMemoryIfReserved() {
    synchronized (isTsFileParserMemoryReserved) {
      if (isTsFileParserMemoryReserved.compareAndSet(true, false)) {
        PipeDataNodeResourceManager.memory().releaseTsFileParserMemory();
      }
    }
  }

  /** The method is used to prevent circular replication in IoTConsensusV2 */
  public boolean isGeneratedByIoTConsensusV2() {
    return isGeneratedByIoTConsensusV2;
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

      final String errorMsg =
          String.format(DataNodePipeMessages.READ_TSFILE_ERROR, tsFile.getPath());
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
        new TsFileInsertionPointCounter(tsFile, treePattern)) {
      return counter.count();
    }
  }

  /** Release the resource of {@link TsFileInsertionEventParser}. */
  @Override
  public void close() {
    tabletInsertionEventIterator.set(null);
    releaseParsedTabletEvent(pendingTabletInsertionEvent.getAndSet(null));
    parsedTabletInsertionEventCount.set(0);
    parsedPointCountForCount.set(0);
    isTsFileParsingCompleted.set(false);
    eventParser.getAndUpdate(
        parser -> {
          if (Objects.nonNull(parser)) {
            parser.close();
          }
          return null;
        });
    releaseTsFileParserMemoryIfReserved();
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
        this.creationTime,
        this.tsFile,
        this.isWithMod,
        this.modFile,
        this.sharedModFile,
        this.eventParser,
        this.isTsFileParserMemoryReserved);
  }

  private static class PipeTsFileInsertionEventResource extends PipeEventResource {

    private final File tsFile;
    private final boolean isWithMod;
    private final File modFile;
    private final File sharedModFile; // unused now
    private final AtomicReference<TsFileInsertionEventParser> eventParser;
    private final String pipeName;
    private final long creationTime;
    private final AtomicBoolean isTsFileParserMemoryReserved;

    private PipeTsFileInsertionEventResource(
        final AtomicBoolean isReleased,
        final AtomicInteger referenceCount,
        final String pipeName,
        final long creationTime,
        final File tsFile,
        final boolean isWithMod,
        final File modFile,
        final File sharedModFile,
        final AtomicReference<TsFileInsertionEventParser> eventParser,
        final AtomicBoolean isTsFileParserMemoryReserved) {
      super(isReleased, referenceCount);
      this.pipeName = pipeName;
      this.creationTime = creationTime;
      this.tsFile = tsFile;
      this.isWithMod = isWithMod;
      this.modFile = modFile;
      this.sharedModFile = sharedModFile;
      this.eventParser = eventParser;
      this.isTsFileParserMemoryReserved = isTsFileParserMemoryReserved;
    }

    @Override
    protected void finalizeResource() {
      try {
        final String pipeTsFileResourcePipeName =
            PipeTsFileResourceManager.getPipeTsFileResourcePipeName(pipeName, creationTime);
        // decrease reference count
        PipeDataNodeResourceManager.tsfile()
            .decreaseFileReference(tsFile, pipeTsFileResourcePipeName);
        if (isWithMod) {
          PipeDataNodeResourceManager.tsfile()
              .decreaseFileReference(modFile, pipeTsFileResourcePipeName);
        }

        // close event parser
        eventParser.getAndUpdate(
            parser -> {
              if (Objects.nonNull(parser)) {
                parser.close();
              }
              return null;
            });
        synchronized (isTsFileParserMemoryReserved) {
          if (isTsFileParserMemoryReserved.compareAndSet(true, false)) {
            PipeDataNodeResourceManager.memory().releaseTsFileParserMemory();
          }
        }
      } catch (final Exception e) {
        LOGGER.warn(
            DataNodePipeMessages.DECREASE_REFERENCE_COUNT_FOR_TSFILE_ERROR, tsFile.getPath(), e);
      }
    }
  }
}
