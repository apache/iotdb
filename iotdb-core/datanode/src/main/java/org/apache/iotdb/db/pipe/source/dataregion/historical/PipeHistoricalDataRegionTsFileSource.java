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

package org.apache.iotdb.db.pipe.source.dataregion.historical;

import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.commons.consensus.index.ProgressIndex;
import org.apache.iotdb.commons.consensus.index.ProgressIndexType;
import org.apache.iotdb.commons.consensus.index.impl.HybridProgressIndex;
import org.apache.iotdb.commons.consensus.index.impl.MinimumProgressIndex;
import org.apache.iotdb.commons.consensus.index.impl.StateProgressIndex;
import org.apache.iotdb.commons.consensus.index.impl.TimePartitionProgressIndex;
import org.apache.iotdb.commons.consensus.index.impl.TimeWindowStateProgressIndex;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.pipe.agent.task.PipeTaskAgent;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeStaticMeta;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeTaskMeta;
import org.apache.iotdb.commons.pipe.config.constant.SystemConstant;
import org.apache.iotdb.commons.pipe.config.plugin.env.PipeTaskSourceRuntimeEnvironment;
import org.apache.iotdb.commons.pipe.datastructure.pattern.PipePattern;
import org.apache.iotdb.commons.pipe.event.ProgressReportEvent;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.i18n.DataNodePipeMessages;
import org.apache.iotdb.db.pipe.event.common.terminate.PipeTerminateEvent;
import org.apache.iotdb.db.pipe.event.common.tsfile.PipeTsFileInsertionEvent;
import org.apache.iotdb.db.pipe.resource.PipeDataNodeResourceManager;
import org.apache.iotdb.db.pipe.resource.tsfile.PipeTsFileResourceManager;
import org.apache.iotdb.db.pipe.source.dataregion.DataRegionListeningFilter;
import org.apache.iotdb.db.storageengine.StorageEngine;
import org.apache.iotdb.db.storageengine.dataregion.DataRegion;
import org.apache.iotdb.db.storageengine.dataregion.memtable.TsFileProcessor;
import org.apache.iotdb.db.storageengine.dataregion.read.reader.common.MergeReaderPriority;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileManager;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.utils.DateTimeUtils;
import org.apache.iotdb.pipe.api.customizer.configuration.PipeExtractorRuntimeConfiguration;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameterValidator;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;
import org.apache.iotdb.pipe.api.event.Event;
import org.apache.iotdb.pipe.api.exception.PipeException;
import org.apache.iotdb.pipe.api.exception.PipeParameterNotValidException;

import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.PlainDeviceID;
import org.apache.tsfile.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.EXTRACTOR_END_TIME_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.EXTRACTOR_FORWARDING_PIPE_REQUESTS_DEFAULT_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.EXTRACTOR_FORWARDING_PIPE_REQUESTS_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.EXTRACTOR_HISTORY_ENABLE_DEFAULT_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.EXTRACTOR_HISTORY_ENABLE_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.EXTRACTOR_HISTORY_END_TIME_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.EXTRACTOR_HISTORY_LOOSE_RANGE_ALL_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.EXTRACTOR_HISTORY_LOOSE_RANGE_DEFAULT_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.EXTRACTOR_HISTORY_LOOSE_RANGE_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.EXTRACTOR_HISTORY_LOOSE_RANGE_PATH_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.EXTRACTOR_HISTORY_LOOSE_RANGE_TIME_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.EXTRACTOR_HISTORY_START_TIME_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.EXTRACTOR_HISTORY_TSFILE_ORDER_BY_QUERY_PRIORITY_DEFAULT_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.EXTRACTOR_HISTORY_TSFILE_ORDER_BY_QUERY_PRIORITY_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.EXTRACTOR_MODS_ENABLE_DEFAULT_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.EXTRACTOR_MODS_ENABLE_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.EXTRACTOR_START_TIME_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.EXTRACTOR_TSFILE_PARSER_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.SOURCE_END_TIME_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.SOURCE_FORWARDING_PIPE_REQUESTS_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.SOURCE_HISTORY_ENABLE_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.SOURCE_HISTORY_END_TIME_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.SOURCE_HISTORY_LOOSE_RANGE_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.SOURCE_HISTORY_START_TIME_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.SOURCE_HISTORY_TSFILE_ORDER_BY_QUERY_PRIORITY_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.SOURCE_MODS_ENABLE_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.SOURCE_START_TIME_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.SOURCE_TSFILE_PARSER_KEY;

public class PipeHistoricalDataRegionTsFileSource implements PipeHistoricalDataRegionSource {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(PipeHistoricalDataRegionTsFileSource.class);

  private String pipeName;
  private long creationTime;
  private String pipeNameWithCreationTime;

  private PipeTaskMeta pipeTaskMeta;
  private ProgressIndex startIndex;

  private int dataRegionId;

  private PipePattern pipePattern;
  private boolean isDbNameCoveredByPattern = false;

  private boolean isHistoricalSourceEnabled = false;
  private long historicalDataExtractionStartTime = Long.MIN_VALUE; // Event time
  private long historicalDataExtractionEndTime = Long.MAX_VALUE; // Event time

  private boolean sloppyTimeRange; // true to disable time range filter after extraction
  private boolean sloppyPattern; // true to disable pattern filter after extraction
  private boolean shouldOrderHistoricalTsFileByQueryPriority =
      EXTRACTOR_HISTORY_TSFILE_ORDER_BY_QUERY_PRIORITY_DEFAULT_VALUE;

  private Pair<Boolean, Boolean> listeningOptionPair;
  private boolean shouldExtractInsertion;
  private boolean shouldTransferModFile; // Whether to transfer mods
  private String tsFileParser;

  private boolean shouldTerminatePipeOnAllHistoricalEventsConsumed;
  private boolean isTerminateSignalSent = false;

  private boolean isForwardingPipeRequests;

  private volatile boolean hasBeenStarted = false;

  private Queue<TsFileResource> pendingQueue;
  private final Set<TsFileResource> filteredTsFileResources = new HashSet<>();
  private final Set<TsFileResource> resourcesWithPinnedTsFile = new HashSet<>();
  private final Set<TsFileResource> resourcesWithPinnedModFile = new HashSet<>();
  private final Set<TsFileResource> historicalProgressReportResources = new HashSet<>();
  private ProgressIndex maxHistoricalProgressIndex = MinimumProgressIndex.INSTANCE;
  private ProgressIndex maxSuppliedHistoricalProgressReportIndex = MinimumProgressIndex.INSTANCE;
  private ProgressIndex pendingHistoricalProgressIndexToReport;
  private boolean shouldReportMaxHistoricalProgressIndex = false;

  @Override
  public void validate(final PipeParameterValidator validator) {
    final PipeParameters parameters = validator.getParameters();

    try {
      listeningOptionPair =
          DataRegionListeningFilter.parseInsertionDeletionListeningOptionPair(parameters);
    } catch (final Exception e) {
      // compatible with the current validation framework
      throw new PipeParameterNotValidException(e.getMessage());
    }

    shouldOrderHistoricalTsFileByQueryPriority =
        parameters.getBooleanOrDefault(
            Arrays.asList(
                EXTRACTOR_HISTORY_TSFILE_ORDER_BY_QUERY_PRIORITY_KEY,
                SOURCE_HISTORY_TSFILE_ORDER_BY_QUERY_PRIORITY_KEY),
            EXTRACTOR_HISTORY_TSFILE_ORDER_BY_QUERY_PRIORITY_DEFAULT_VALUE);

    final String extractorHistoryLooseRangeValue =
        parameters
            .getStringOrDefault(
                Arrays.asList(EXTRACTOR_HISTORY_LOOSE_RANGE_KEY, SOURCE_HISTORY_LOOSE_RANGE_KEY),
                EXTRACTOR_HISTORY_LOOSE_RANGE_DEFAULT_VALUE)
            .trim();
    if (EXTRACTOR_HISTORY_LOOSE_RANGE_ALL_VALUE.equalsIgnoreCase(extractorHistoryLooseRangeValue)) {
      sloppyTimeRange = true;
      sloppyPattern = true;
    } else {
      final Set<String> sloppyOptionSet =
          Arrays.stream(extractorHistoryLooseRangeValue.split(","))
              .map(String::trim)
              .filter(s -> !s.isEmpty())
              .map(String::toLowerCase)
              .collect(Collectors.toSet());
      sloppyTimeRange = sloppyOptionSet.remove(EXTRACTOR_HISTORY_LOOSE_RANGE_TIME_VALUE);
      sloppyPattern = sloppyOptionSet.remove(EXTRACTOR_HISTORY_LOOSE_RANGE_PATH_VALUE);
      if (!sloppyOptionSet.isEmpty()) {
        throw new PipeParameterNotValidException(
            String.format(
                "Parameters in set %s are not allowed in 'history.loose-range'", sloppyOptionSet));
      }
    }

    // Historical data extraction is enabled in the following cases:
    // 1. System restarts the pipe. If the pipe is restarted but historical data extraction is not
    // enabled, the pipe will lose some historical data.
    // 2. Historical extraction is enabled by the user or by default.
    isHistoricalSourceEnabled =
        parameters.getBooleanOrDefault(
                SystemConstant.RESTART_OR_NEWLY_ADDED_KEY,
                SystemConstant.RESTART_OR_NEWLY_ADDED_DEFAULT_VALUE)
            || parameters.getBooleanOrDefault(
                Arrays.asList(EXTRACTOR_HISTORY_ENABLE_KEY, SOURCE_HISTORY_ENABLE_KEY),
                EXTRACTOR_HISTORY_ENABLE_DEFAULT_VALUE);

    if (parameters.hasAnyAttributes(
        SOURCE_START_TIME_KEY,
        EXTRACTOR_START_TIME_KEY,
        SOURCE_END_TIME_KEY,
        EXTRACTOR_END_TIME_KEY)) {
      try {
        historicalDataExtractionStartTime =
            parameters.hasAnyAttributes(SOURCE_START_TIME_KEY, EXTRACTOR_START_TIME_KEY)
                ? DateTimeUtils.convertTimestampOrDatetimeStrToLongWithDefaultZone(
                    parameters.getStringByKeys(SOURCE_START_TIME_KEY, EXTRACTOR_START_TIME_KEY))
                : Long.MIN_VALUE;
        historicalDataExtractionEndTime =
            parameters.hasAnyAttributes(SOURCE_END_TIME_KEY, EXTRACTOR_END_TIME_KEY)
                ? DateTimeUtils.convertTimestampOrDatetimeStrToLongWithDefaultZone(
                    parameters.getStringByKeys(SOURCE_END_TIME_KEY, EXTRACTOR_END_TIME_KEY))
                : Long.MAX_VALUE;
        if (historicalDataExtractionStartTime > historicalDataExtractionEndTime) {
          throw new PipeParameterNotValidException(
              String.format(
                  "%s (%s) [%s] should be less than or equal to %s (%s) [%s].",
                  SOURCE_START_TIME_KEY,
                  EXTRACTOR_START_TIME_KEY,
                  historicalDataExtractionStartTime,
                  SOURCE_END_TIME_KEY,
                  EXTRACTOR_END_TIME_KEY,
                  historicalDataExtractionEndTime));
        }
      } catch (final PipeParameterNotValidException e) {
        throw e;
      } catch (final Exception e) {
        // compatible with the current validation framework
        throw new PipeParameterNotValidException(e.getMessage());
      }

      // return here
      return;
    }

    try {
      historicalDataExtractionStartTime =
          parameters.hasAnyAttributes(
                  EXTRACTOR_HISTORY_START_TIME_KEY, SOURCE_HISTORY_START_TIME_KEY)
              ? DateTimeUtils.convertTimestampOrDatetimeStrToLongWithDefaultZone(
                  parameters.getStringByKeys(
                      EXTRACTOR_HISTORY_START_TIME_KEY, SOURCE_HISTORY_START_TIME_KEY))
              : Long.MIN_VALUE;
      historicalDataExtractionEndTime =
          parameters.hasAnyAttributes(EXTRACTOR_HISTORY_END_TIME_KEY, SOURCE_HISTORY_END_TIME_KEY)
              ? DateTimeUtils.convertTimestampOrDatetimeStrToLongWithDefaultZone(
                  parameters.getStringByKeys(
                      EXTRACTOR_HISTORY_END_TIME_KEY, SOURCE_HISTORY_END_TIME_KEY))
              : Long.MAX_VALUE;
      if (historicalDataExtractionStartTime > historicalDataExtractionEndTime) {
        throw new PipeParameterNotValidException(
            String.format(
                "%s (%s) [%s] should be less than or equal to %s (%s) [%s].",
                EXTRACTOR_HISTORY_START_TIME_KEY,
                SOURCE_HISTORY_START_TIME_KEY,
                historicalDataExtractionStartTime,
                EXTRACTOR_HISTORY_END_TIME_KEY,
                SOURCE_HISTORY_END_TIME_KEY,
                historicalDataExtractionEndTime));
      }
    } catch (final Exception e) {
      // Compatible with the current validation framework
      throw new PipeParameterNotValidException(e.getMessage());
    }
  }

  @Override
  public void customize(
      final PipeParameters parameters, final PipeExtractorRuntimeConfiguration configuration)
      throws IllegalPathException {
    shouldExtractInsertion = listeningOptionPair.getLeft();
    shouldOrderHistoricalTsFileByQueryPriority =
        parameters.getBooleanOrDefault(
            Arrays.asList(
                EXTRACTOR_HISTORY_TSFILE_ORDER_BY_QUERY_PRIORITY_KEY,
                SOURCE_HISTORY_TSFILE_ORDER_BY_QUERY_PRIORITY_KEY),
            EXTRACTOR_HISTORY_TSFILE_ORDER_BY_QUERY_PRIORITY_DEFAULT_VALUE);
    // Do nothing if only extract deletion
    if (!shouldExtractInsertion) {
      return;
    }

    final PipeTaskSourceRuntimeEnvironment environment =
        (PipeTaskSourceRuntimeEnvironment) configuration.getRuntimeEnvironment();

    pipeName = environment.getPipeName();
    creationTime = environment.getCreationTime();
    pipeNameWithCreationTime =
        PipeTsFileResourceManager.getPipeTsFileResourcePipeName(pipeName, creationTime);
    pipeTaskMeta = environment.getPipeTaskMeta();
    startIndex = environment.getPipeTaskMeta().getProgressIndex();

    dataRegionId = environment.getRegionId();
    pipePattern = PipePattern.parsePipePatternFromSourceParameters(parameters);
    tsFileParser =
        parameters.getStringByKeys(EXTRACTOR_TSFILE_PARSER_KEY, SOURCE_TSFILE_PARSER_KEY);

    final DataRegion dataRegion =
        StorageEngine.getInstance().getDataRegion(new DataRegionId(environment.getRegionId()));
    if (Objects.nonNull(dataRegion)) {
      final String databaseName = dataRegion.getDatabaseName();
      if (Objects.nonNull(databaseName)) {
        isDbNameCoveredByPattern = pipePattern.coversDb(databaseName);
      }
    }

    shouldTransferModFile =
        parameters.getBooleanOrDefault(
            Arrays.asList(SOURCE_MODS_ENABLE_KEY, EXTRACTOR_MODS_ENABLE_KEY),
            EXTRACTOR_MODS_ENABLE_DEFAULT_VALUE
                || // Should extract deletion
                listeningOptionPair.getRight());

    shouldTerminatePipeOnAllHistoricalEventsConsumed = PipeTaskAgent.isSnapshotMode(parameters);

    isForwardingPipeRequests =
        parameters.getBooleanOrDefault(
            Arrays.asList(
                EXTRACTOR_FORWARDING_PIPE_REQUESTS_KEY, SOURCE_FORWARDING_PIPE_REQUESTS_KEY),
            EXTRACTOR_FORWARDING_PIPE_REQUESTS_DEFAULT_VALUE);

    if (LOGGER.isInfoEnabled()) {
      LOGGER.info(
          "Pipe {}@{}: historical data extraction time range, start time {}({}), end time {}({}), sloppy pattern {}, sloppy time range {}, should transfer mod file {}, is forwarding pipe requests: {}",
          pipeName,
          dataRegionId,
          DateTimeUtils.convertLongToDate(historicalDataExtractionStartTime),
          historicalDataExtractionStartTime,
          DateTimeUtils.convertLongToDate(historicalDataExtractionEndTime),
          historicalDataExtractionEndTime,
          sloppyPattern,
          sloppyTimeRange,
          shouldTransferModFile,
          isForwardingPipeRequests);
    }
  }

  @Override
  public synchronized void start() {
    if (!shouldExtractInsertion) {
      hasBeenStarted = true;
      return;
    }
    if (!StorageEngine.getInstance().isReadyForNonReadWriteFunctions()) {
      LOGGER.info(
          "Pipe {}@{}: failed to start to extract historical TsFile, storage engine is not ready. Will retry later.",
          pipeName,
          dataRegionId);
      return;
    }
    hasBeenStarted = true;
    maxHistoricalProgressIndex = MinimumProgressIndex.INSTANCE;
    maxSuppliedHistoricalProgressReportIndex = MinimumProgressIndex.INSTANCE;
    pendingHistoricalProgressIndexToReport = null;
    shouldReportMaxHistoricalProgressIndex = false;
    historicalProgressReportResources.clear();
    filteredTsFileResources.clear();

    final DataRegion dataRegion =
        StorageEngine.getInstance().getDataRegion(new DataRegionId(dataRegionId));
    if (Objects.isNull(dataRegion)) {
      pendingQueue = new ArrayDeque<>();
      return;
    }

    final TsFileManager tsFileManager = dataRegion.getTsFileManager();
    List<TsFileResource> sequenceTsFileResourceSnapshot = Collections.emptyList();
    List<TsFileResource> unsequenceTsFileResourceSnapshot = Collections.emptyList();
    final long startHistoricalExtractionTime = System.currentTimeMillis();
    dataRegion.writeLock("Pipe: start to extract historical TsFile");
    try {
      if (dataRegion.isDeleted()) {
        pendingQueue = new ArrayDeque<>();
        return;
      }
      if (pipeName.startsWith(PipeStaticMeta.CONSENSUS_PIPE_PREFIX)
          || shouldUseHistoricalTsFileQueryPriorityOrder()) {
        dataRegion.syncCloseAllWorkingTsFileProcessors();
      } else {
        dataRegion.asyncCloseAllWorkingTsFileProcessors();
      }
      tsFileManager.readLock();
      try {
        final Pair<List<TsFileResource>, List<TsFileResource>> snapshot =
            tsFileManager.getAllTsFileListForQuery(null, null);
        sequenceTsFileResourceSnapshot = snapshot.left;
        unsequenceTsFileResourceSnapshot = snapshot.right;
      } finally {
        tsFileManager.readUnlock();
      }
    } finally {
      dataRegion.writeUnlock();
    }

    extractTsFiles(
        tsFileManager,
        sequenceTsFileResourceSnapshot,
        unsequenceTsFileResourceSnapshot,
        startHistoricalExtractionTime);
  }

  private void extractTsFiles(
      final TsFileManager tsFileManager,
      final List<TsFileResource> sequenceTsFileResourceSnapshot,
      final List<TsFileResource> unsequenceTsFileResourceSnapshot,
      final long startHistoricalExtractionTime) {
    final int originalSequenceTsFileCount = sequenceTsFileResourceSnapshot.size();
    final int originalUnSequenceTsFileCount = unsequenceTsFileResourceSnapshot.size();
    final Map<Long, TsFileResourcesByPartition> snapshotsByPartition = new TreeMap<>();
    sequenceTsFileResourceSnapshot.forEach(
        resource ->
            snapshotsByPartition
                .computeIfAbsent(
                    resource.getTimePartition(), ignored -> new TsFileResourcesByPartition())
                .sequenceResources
                .add(resource));
    unsequenceTsFileResourceSnapshot.forEach(
        resource ->
            snapshotsByPartition
                .computeIfAbsent(
                    resource.getTimePartition(), ignored -> new TsFileResourcesByPartition())
                .unsequenceResources
                .add(resource));

    final Map<Long, TsFileResourcesByPartition> finalSnapshotsByPartition = new TreeMap<>();
    final HistoricalTsFileExtractionStatistics statistics =
        new HistoricalTsFileExtractionStatistics();
    try {
      for (Map.Entry<Long, TsFileResourcesByPartition> entry : snapshotsByPartition.entrySet()) {
        final long timePartition = entry.getKey();
        TsFileResourcesByPartition snapshot = entry.getValue();
        HistoricalTsFileExtractionStatistics partitionStatistics =
            new HistoricalTsFileExtractionStatistics();
        extractTsFilesInPartition(snapshot, partitionStatistics);
        if (!isTsFilePartitionSnapshotCurrent(tsFileManager, timePartition, snapshot)) {
          final Exception unpinException = unpinTsFilesInPartition(snapshot);
          if (unpinException != null) {
            throw new PipeException(unpinException.getMessage(), unpinException);
          }

          snapshot = getTsFilePartitionSnapshot(tsFileManager, timePartition);
          partitionStatistics = new HistoricalTsFileExtractionStatistics();
          extractTsFilesInPartition(snapshot, partitionStatistics);
          if (!isTsFilePartitionSnapshotCurrent(tsFileManager, timePartition, snapshot)) {
            LOGGER.warn(
                String.format(
                    DataNodePipeMessages
                        .LOG_PIPE_ARG_ARG_TIME_PARTITION_ARG_CHANGED_DURING_HISTORICAL_TSFILE_EXTRACTION_AFTER_ONE_REFRESH_AF060BD3,
                    pipeName,
                    creationTime,
                    timePartition));
          }
        }
        finalSnapshotsByPartition.put(timePartition, snapshot);
        statistics.add(partitionStatistics);
      }

      final List<TsFileResource> originalResourceList = new ArrayList<>();
      for (TsFileResourcesByPartition snapshot : finalSnapshotsByPartition.values()) {
        originalResourceList.addAll(snapshot.sequenceResources);
        originalResourceList.addAll(snapshot.unsequenceResources);
      }
      if (shouldUseHistoricalTsFileQueryPriorityOrder()) {
        prepareResourcesForHistoricalTsFileQueryPriorityOrder(originalResourceList);
      }
      sortExtractedResources(originalResourceList);
      if (shouldUseHistoricalTsFileQueryPriorityOrder()) {
        prepareProgressReportResourcesForHistoricalTsFileQueryPriorityOrder(originalResourceList);
      }
      pendingQueue = new ArrayDeque<>(originalResourceList);
      PipeTerminateEvent.initializeHistoricalTransferSummary(
          pipeName, creationTime, dataRegionId, filteredTsFileResources.size(), 0);

      LOGGER.info(
          "Pipe {}@{}: finish to extract historical TsFile, extracted sequence file count {}/{}, "
              + "extracted unsequence file count {}/{}, extracted file count {}/{}, took {} ms",
          pipeName,
          dataRegionId,
          statistics.selectedSequenceTsFileCount,
          originalSequenceTsFileCount,
          statistics.selectedUnsequenceTsFileCount,
          originalUnSequenceTsFileCount,
          filteredTsFileResources.size(),
          originalSequenceTsFileCount + originalUnSequenceTsFileCount,
          System.currentTimeMillis() - startHistoricalExtractionTime);
      LOGGER.info(
          "Pipe {}@{}: historical TsFile selection summary, selected by progress uncovered {}, "
              + "selected by unclosed/closing {}, filtered by time/path {} (time {}, path {}), "
              + "skipped covered {}, skipped deleted {}, skipped generated by pipe {}, "
              + "pin failed {}",
          pipeName,
          dataRegionId,
          statistics.selectedByProgressUncoveredCount,
          statistics.selectedByUnclosedOrClosingCount,
          statistics.filteredByTimeOrPathCount,
          statistics.filteredByTimeCount,
          statistics.filteredByPathCount,
          statistics.skippedCoveredCount,
          statistics.skippedDeletedCount,
          statistics.skippedGeneratedByPipeCount,
          statistics.pinFailedCount);
    } catch (final RuntimeException | Error e) {
      releasePinnedResources(e);
      filteredTsFileResources.clear();
      pendingQueue = null;
      throw e;
    }
  }

  private void extractTsFilesInPartition(
      final TsFileResourcesByPartition snapshot,
      final HistoricalTsFileExtractionStatistics statistics) {
    for (final TsFileResource resource : snapshot.sequenceResources) {
      extractTsFileResource(resource, true, statistics);
    }
    for (final TsFileResource resource : snapshot.unsequenceResources) {
      extractTsFileResource(resource, false, statistics);
    }
  }

  private void extractTsFileResource(
      final TsFileResource resource,
      final boolean isSequence,
      final HistoricalTsFileExtractionStatistics statistics) {
    boolean resourceReadLockAcquired = false;
    try {
      resource.readLock();
      resourceReadLockAcquired = true;
      final boolean wasUnclosed = !resource.isClosed();
      if (!shouldExtractTsFileResource(resource, statistics)) {
        return;
      }
      if (isSequence) {
        ++statistics.selectedSequenceTsFileCount;
      } else {
        ++statistics.selectedUnsequenceTsFileCount;
      }
      try {
        final boolean modFilePinned =
            PipeDataNodeResourceManager.tsfile()
                .pinTsFileResource(resource, shouldTransferModFile, pipeNameWithCreationTime);
        resourcesWithPinnedTsFile.add(resource);
        if (modFilePinned) {
          resourcesWithPinnedModFile.add(resource);
        }
        filteredTsFileResources.add(resource);
      } catch (final IOException e) {
        ++statistics.pinFailedCount;
        LOGGER.warn("Pipe: failed to pin TsFileResource {}", resource.getTsFilePath(), e);
        if (wasUnclosed) {
          filteredTsFileResources.add(resource);
        }
      }
    } finally {
      if (resourceReadLockAcquired) {
        resource.readUnlock();
      }
    }
  }

  private TsFileResourcesByPartition getTsFilePartitionSnapshot(
      final TsFileManager tsFileManager, final long timePartition) {
    tsFileManager.readLock();
    try {
      final Pair<List<TsFileResource>, List<TsFileResource>> snapshot =
          tsFileManager.getAllTsFileListForQuery(Collections.singletonList(timePartition), null);
      final TsFileResourcesByPartition resources = new TsFileResourcesByPartition();
      resources.sequenceResources.addAll(snapshot.left);
      resources.unsequenceResources.addAll(snapshot.right);
      return resources;
    } finally {
      tsFileManager.readUnlock();
    }
  }

  private boolean isTsFilePartitionSnapshotCurrent(
      final TsFileManager tsFileManager,
      final long timePartition,
      final TsFileResourcesByPartition snapshot) {
    tsFileManager.readLock();
    try {
      final Pair<List<TsFileResource>, List<TsFileResource>> currentSnapshot =
          tsFileManager.getAllTsFileListForQuery(Collections.singletonList(timePartition), null);
      return areSameTsFileResources(snapshot.sequenceResources, currentSnapshot.left)
          && areSameTsFileResources(snapshot.unsequenceResources, currentSnapshot.right);
    } finally {
      tsFileManager.readUnlock();
    }
  }

  private boolean areSameTsFileResources(
      final List<TsFileResource> first, final List<TsFileResource> second) {
    if (first.size() != second.size()) {
      return false;
    }
    for (int i = 0; i < first.size(); ++i) {
      if (first.get(i) != second.get(i)) {
        return false;
      }
    }
    return true;
  }

  private Exception unpinTsFilesInPartition(final TsFileResourcesByPartition snapshot) {
    Exception firstException = null;
    for (final TsFileResource resource : snapshot.sequenceResources) {
      try {
        unpinHistoricalTsFileResource(resource);
      } catch (final IOException | RuntimeException e) {
        if (firstException == null) {
          firstException = e;
        } else {
          firstException.addSuppressed(e);
        }
      }
    }
    for (final TsFileResource resource : snapshot.unsequenceResources) {
      try {
        unpinHistoricalTsFileResource(resource);
      } catch (final IOException | RuntimeException e) {
        if (firstException == null) {
          firstException = e;
        } else {
          firstException.addSuppressed(e);
        }
      }
    }
    return firstException;
  }

  private void releasePinnedResources(final Throwable throwable) {
    final Set<TsFileResource> pinnedResources = new HashSet<>(resourcesWithPinnedTsFile);
    pinnedResources.addAll(resourcesWithPinnedModFile);
    for (final TsFileResource resource : pinnedResources) {
      try {
        unpinHistoricalTsFileResource(resource);
      } catch (final IOException | RuntimeException e) {
        throwable.addSuppressed(e);
      }
    }
  }

  private void unpinHistoricalTsFileResource(final TsFileResource resource) throws IOException {
    if (!resourcesWithPinnedTsFile.contains(resource)
        && !resourcesWithPinnedModFile.contains(resource)) {
      filteredTsFileResources.remove(resource);
      return;
    }
    PipeDataNodeResourceManager.tsfile()
        .unpinTsFileResource(
            resource, pipeNameWithCreationTime, resourcesWithPinnedModFile.contains(resource));
    resourcesWithPinnedTsFile.remove(resource);
    resourcesWithPinnedModFile.remove(resource);
    filteredTsFileResources.remove(resource);
  }

  private boolean shouldUseHistoricalTsFileQueryPriorityOrder() {
    return shouldOrderHistoricalTsFileByQueryPriority && shouldExtractInsertion;
  }

  private void prepareResourcesForHistoricalTsFileQueryPriorityOrder(
      final List<TsFileResource> resourceList) {
    // Query-priority order is intentionally not compatible with progressIndex order, so only
    // selected historical TsFiles should participate in query-order progress reports.
    resourceList.removeIf(resource -> !filteredTsFileResources.contains(resource));
    updateMaxHistoricalProgressIndex(resourceList);
    shouldReportMaxHistoricalProgressIndex = !resourceList.isEmpty();
  }

  private void prepareProgressReportResourcesForHistoricalTsFileQueryPriorityOrder(
      final List<TsFileResource> resourceList) {
    historicalProgressReportResources.clear();
    final Map<Long, List<ProgressIndex>> timePartitionId2RemainingMinimalProgressIndexes =
        new HashMap<>();
    for (int i = resourceList.size() - 1; i >= 0; --i) {
      final TsFileResource resource = resourceList.get(i);
      final ProgressIndex progressIndex = resource.getMaxProgressIndex();
      if (Objects.isNull(progressIndex)) {
        continue;
      }

      final List<ProgressIndex> remainingMinimalProgressIndexes =
          timePartitionId2RemainingMinimalProgressIndexes.computeIfAbsent(
              resource.getTimePartition(), ignored -> new ArrayList<>());
      // A query-priority report is persisted as a time-partition-scoped progress index. Recovery
      // only uses it to cover TsFiles from the same partition, so it does not rely on any global
      // ordering guarantee between partitions.
      if (remainingMinimalProgressIndexes.stream().noneMatch(progressIndex::isEqualOrAfter)) {
        historicalProgressReportResources.add(resource);
      }
      updateRemainingMinimalProgressIndexes(remainingMinimalProgressIndexes, progressIndex);
    }
  }

  private void updateRemainingMinimalProgressIndexes(
      final List<ProgressIndex> remainingMinimalProgressIndexes,
      final ProgressIndex progressIndex) {
    if (remainingMinimalProgressIndexes.stream().anyMatch(progressIndex::isEqualOrAfter)) {
      return;
    }

    // Keep only suffix minimal progress indexes. They are sufficient to test whether a new
    // progress index covers any remaining resource.
    remainingMinimalProgressIndexes.removeIf(
        minimalProgressIndex -> minimalProgressIndex.isEqualOrAfter(progressIndex));
    remainingMinimalProgressIndexes.add(progressIndex);
  }

  private void updateMaxHistoricalProgressIndex(final List<TsFileResource> resourceList) {
    for (final TsFileResource resource : resourceList) {
      final ProgressIndex progressIndex = resource.getMaxProgressIndex();
      if (Objects.nonNull(progressIndex)) {
        maxHistoricalProgressIndex =
            maxHistoricalProgressIndex.updateToMinimumEqualOrIsAfterProgressIndex(progressIndex);
      }
    }
  }

  private void sortExtractedResources(final List<TsFileResource> resourceList) {
    if (shouldUseHistoricalTsFileQueryPriorityOrder()) {
      // Send TsFiles from lower query/compaction priority to higher priority. For duplicated
      // points, covered files are loaded first on the receiver and covering files are loaded later
      // to preserve overwrite semantics.
      resourceList.sort(this::compareTsFileResourcesByQueryPriority);
      return;
    }

    resourceList.sort(
        (o1, o2) ->
            getTimeWindowStateProgressIndex(startIndex) != null
                ? Long.compare(o1.getFileStartTime(), o2.getFileStartTime())
                : o1.getMaxProgressIndex().topologicalCompareTo(o2.getMaxProgressIndex()));
  }

  private int compareTsFileResourcesByQueryPriority(
      final TsFileResource resource1, final TsFileResource resource2) {
    int result =
        new MergeReaderPriority(
                resource1.getTsFileID().timestamp, resource1.getVersion(), 0, resource1.isSeq())
            .compareTo(
                new MergeReaderPriority(
                    resource2.getTsFileID().timestamp,
                    resource2.getVersion(),
                    0,
                    resource2.isSeq()));
    if (result != 0) {
      return result;
    }

    result =
        Long.compare(
            resource1.getTsFileID().compactionVersion, resource2.getTsFileID().compactionVersion);
    if (result != 0) {
      return result;
    }

    return resource1.getTsFilePath().compareTo(resource2.getTsFilePath());
  }

  private boolean shouldExtractTsFileResource(
      final TsFileResource resource, final HistoricalTsFileExtractionStatistics statistics) {
    if (!isHistoricalSourceEnabled) {
      return false;
    }

    // Some resource is marked as deleted but not removed from the list.
    if (resource.isDeleted()) {
      ++statistics.skippedDeletedCount;
      return false;
    }

    // Some resource is generated by pipe. We ignore them if the pipe should not transfer pipe
    // requests.
    if (resource.isGeneratedByPipe() && !isForwardingPipeRequests) {
      ++statistics.skippedGeneratedByPipeCount;
      return false;
    }

    // Closing files may still have incomplete progress and time metadata. Select them
    // conservatively; the event waits for the file to close before parsing or transfer.
    if (!resource.isClosed()
        && Optional.ofNullable(resource.getProcessor())
            .map(TsFileProcessor::alreadyMarkedClosing)
            .orElse(true)) {
      ++statistics.selectedByUnclosedOrClosingCount;
      return true;
    }

    if (!mayTsFileContainUnprocessedData(resource)) {
      ++statistics.skippedCoveredCount;
      return false;
    }

    if (!isTsFileResourceOverlappedWithTimeRange(resource)) {
      ++statistics.filteredByTimeOrPathCount;
      ++statistics.filteredByTimeCount;
      return false;
    }

    if (!mayTsFileResourceOverlappedWithPattern(resource)) {
      ++statistics.filteredByTimeOrPathCount;
      ++statistics.filteredByPathCount;
      return false;
    }

    ++statistics.selectedByProgressUncoveredCount;
    return true;
  }

  private boolean mayTsFileContainUnprocessedData(final TsFileResource resource) {
    final ProgressIndex innerStartIndex = getInnerProgressIndex(startIndex);
    final TimeWindowStateProgressIndex timeWindowStateProgressIndex =
        getTimeWindowStateProgressIndex(startIndex);
    if (Objects.nonNull(timeWindowStateProgressIndex)) {
      // The resource is closed thus the TsFileResource#getFileEndTime() is safe to use
      return timeWindowStateProgressIndex.getMinTime() <= resource.getFileEndTime();
    }

    final ProgressIndex resourceProgressIndex = resource.getMaxProgressIndex();
    // Ratis writes do not carry Pipe progress indexes. Minimum means unknown,
    // so replay conservatively after a leader change.
    if (resourceProgressIndex instanceof MinimumProgressIndex
        && ConsensusFactory.RATIS_CONSENSUS.equals(
            IoTDBDescriptor.getInstance().getConfig().getDataRegionConsensusProtocolClass())) {
      return true;
    }
    if (innerStartIndex.isEqualOrAfter(resourceProgressIndex)
        || isProgressIndexCoveredByTimePartitionProgressIndex(
            resource, resourceProgressIndex, innerStartIndex)) {
      return false;
    }

    LOGGER.info(
        "Pipe {}@{}: file {} meets mayTsFileContainUnprocessedData condition, extractor progressIndex: {}, resource ProgressIndex: {}",
        pipeName,
        dataRegionId,
        resource.getTsFilePath(),
        innerStartIndex,
        resourceProgressIndex);
    return true;
  }

  private ProgressIndex getInnerProgressIndex(final ProgressIndex progressIndex) {
    return progressIndex instanceof StateProgressIndex
        ? ((StateProgressIndex) progressIndex).getInnerProgressIndex()
        : Objects.isNull(progressIndex) ? MinimumProgressIndex.INSTANCE : progressIndex;
  }

  private boolean isProgressIndexCoveredByTimePartitionProgressIndex(
      final TsFileResource resource,
      final ProgressIndex progressIndex,
      final ProgressIndex startIndex) {
    final TimePartitionProgressIndex timePartitionProgressIndex =
        getTimePartitionProgressIndex(startIndex);
    // Keep this check strictly partition-local, matching the reporting side. This is what makes
    // query-priority historical transfer restart-safe even when different partitions are sent in an
    // order that conflicts with the global ProgressIndex order.
    return Objects.nonNull(timePartitionProgressIndex)
        && timePartitionProgressIndex.isProgressIndexEqualOrAfter(
            resource.getTimePartition(), progressIndex);
  }

  private TimePartitionProgressIndex getTimePartitionProgressIndex(
      final ProgressIndex progressIndex) {
    final ProgressIndex innerProgressIndex = getInnerProgressIndex(progressIndex);
    if (innerProgressIndex instanceof TimePartitionProgressIndex) {
      return (TimePartitionProgressIndex) innerProgressIndex;
    }

    if (innerProgressIndex instanceof HybridProgressIndex) {
      final ProgressIndex timePartitionProgressIndex =
          ((HybridProgressIndex) innerProgressIndex)
              .getType2Index()
              .get(ProgressIndexType.TIME_PARTITION_PROGRESS_INDEX.getType());
      if (timePartitionProgressIndex instanceof TimePartitionProgressIndex) {
        return (TimePartitionProgressIndex) timePartitionProgressIndex;
      }
    }
    return null;
  }

  private TimeWindowStateProgressIndex getTimeWindowStateProgressIndex(
      final ProgressIndex progressIndex) {
    return Objects.isNull(progressIndex)
        ? null
        : progressIndex.getProgressIndexByType(TimeWindowStateProgressIndex.class).orElse(null);
  }

  private static class TsFileResourcesByPartition {
    private final List<TsFileResource> sequenceResources = new ArrayList<>();
    private final List<TsFileResource> unsequenceResources = new ArrayList<>();
  }

  private static class HistoricalTsFileExtractionStatistics {

    private int selectedSequenceTsFileCount;
    private int selectedUnsequenceTsFileCount;
    private int selectedByProgressUncoveredCount;
    private int selectedByUnclosedOrClosingCount;
    private int filteredByTimeOrPathCount;
    private int filteredByTimeCount;
    private int filteredByPathCount;
    private int skippedCoveredCount;
    private int skippedDeletedCount;
    private int skippedGeneratedByPipeCount;
    private int pinFailedCount;

    private void add(final HistoricalTsFileExtractionStatistics statistics) {
      selectedSequenceTsFileCount += statistics.selectedSequenceTsFileCount;
      selectedUnsequenceTsFileCount += statistics.selectedUnsequenceTsFileCount;
      selectedByProgressUncoveredCount += statistics.selectedByProgressUncoveredCount;
      selectedByUnclosedOrClosingCount += statistics.selectedByUnclosedOrClosingCount;
      filteredByTimeOrPathCount += statistics.filteredByTimeOrPathCount;
      filteredByTimeCount += statistics.filteredByTimeCount;
      filteredByPathCount += statistics.filteredByPathCount;
      skippedCoveredCount += statistics.skippedCoveredCount;
      skippedDeletedCount += statistics.skippedDeletedCount;
      skippedGeneratedByPipeCount += statistics.skippedGeneratedByPipeCount;
      pinFailedCount += statistics.pinFailedCount;
    }
  }

  private boolean mayTsFileResourceOverlappedWithPattern(final TsFileResource resource) {
    // Trimming to avoid unnecessary file device getter
    if (isDbNameCoveredByPattern) {
      return true;
    }

    final Set<IDeviceID> deviceSet;
    try {
      final Map<IDeviceID, Boolean> deviceIsAlignedMap =
          PipeDataNodeResourceManager.tsfile()
              .getDeviceIsAlignedMapFromCache(resource.getTsFile(), false);
      deviceSet =
          Objects.nonNull(deviceIsAlignedMap) ? deviceIsAlignedMap.keySet() : resource.getDevices();
    } catch (final IOException | RuntimeException e) {
      LOGGER.warn(
          "Pipe {}@{}: failed to get devices from TsFile {}, extract it anyway",
          pipeName,
          dataRegionId,
          resource.getTsFilePath(),
          e);
      return true;
    }

    return deviceSet.stream()
        .anyMatch(
            deviceID -> pipePattern.mayOverlapWithDevice(((PlainDeviceID) deviceID).toStringID()));
  }

  private boolean isTsFileResourceCoveredByPattern(final TsFileResource resource) {
    final Set<IDeviceID> deviceSet;
    try {
      final Map<IDeviceID, Boolean> deviceIsAlignedMap =
          PipeDataNodeResourceManager.tsfile()
              .getDeviceIsAlignedMapFromCache(resource.getTsFile(), false);
      deviceSet =
          Objects.nonNull(deviceIsAlignedMap) ? deviceIsAlignedMap.keySet() : resource.getDevices();
    } catch (final IOException | RuntimeException e) {
      return false;
    }

    return !deviceSet.isEmpty()
        && deviceSet.stream()
            .allMatch(
                deviceID -> pipePattern.coversDevice(((PlainDeviceID) deviceID).toStringID()));
  }

  private boolean isTsFileResourceOverlappedWithTimeRange(final TsFileResource resource) {
    return !(resource.getFileEndTime() < historicalDataExtractionStartTime
        || historicalDataExtractionEndTime < resource.getFileStartTime());
  }

  private boolean isTsFileResourceCoveredByTimeRange(final TsFileResource resource) {
    // In particular, a sequence file's end time is not final until it is closed. Do not disable
    // row-level filtering based on the bounds observed while an asynchronous close is in flight.
    return resource.isClosed()
        && historicalDataExtractionStartTime <= resource.getFileStartTime()
        && historicalDataExtractionEndTime >= resource.getFileEndTime();
  }

  @Override
  public synchronized Event supply() {
    if (!hasBeenStarted && StorageEngine.getInstance().isReadyForNonReadWriteFunctions()) {
      start();
    }

    if (Objects.nonNull(pendingHistoricalProgressIndexToReport)) {
      final ProgressIndex progressIndex = pendingHistoricalProgressIndexToReport;
      pendingHistoricalProgressIndexToReport = null;
      return supplyHistoricalProgressReportEvent(progressIndex);
    }

    if (Objects.isNull(pendingQueue)) {
      return null;
    }

    while (true) {
      final TsFileResource resource = pendingQueue.peek();
      if (resource == null) {
        if (shouldReportMaxHistoricalProgressIndex) {
          shouldReportMaxHistoricalProgressIndex = false;
          if (!maxSuppliedHistoricalProgressReportIndex.isEqualOrAfter(
              maxHistoricalProgressIndex)) {
            return supplyHistoricalProgressReportEvent(maxHistoricalProgressIndex);
          }
        }
        return supplyTerminateEvent();
      }

      if (!filteredTsFileResources.contains(resource)) {
        pendingQueue.poll();
        if (shouldUseHistoricalTsFileQueryPriorityOrder()) {
          if (shouldReportHistoricalProgressAfterResource(resource)) {
            return supplyHistoricalProgressReportEvent(
                getHistoricalProgressIndexAfterResource(resource));
          }
          continue;
        }
        return supplyProgressReportEvent(resource.getMaxProgressIndex());
      }

      final Event event = supplyTsFileEvent(resource);
      pendingQueue.poll();
      if (Objects.nonNull(event) && shouldReportHistoricalProgressAfterResource(resource)) {
        pendingHistoricalProgressIndexToReport = getHistoricalProgressIndexAfterResource(resource);
      }
      return event;
    }
  }

  private boolean shouldReportHistoricalProgressAfterResource(final TsFileResource resource) {
    return shouldUseHistoricalTsFileQueryPriorityOrder()
        && historicalProgressReportResources.remove(resource);
  }

  private ProgressIndex getHistoricalProgressIndexAfterResource(final TsFileResource resource) {
    return new TimePartitionProgressIndex(
        resource.getTimePartition(), resource.getMaxProgressIndex());
  }

  private Event supplyTerminateEvent() {
    final PipeTerminateEvent.HistoricalTransferSummary historicalTransferSummary =
        PipeTerminateEvent.snapshotHistoricalTransferSummary(pipeName, creationTime, dataRegionId);
    if (Objects.nonNull(historicalTransferSummary)) {
      LOGGER.info(
          "Pipe {}@{}: historical source has supplied all events, emitting terminate event. {}",
          pipeName,
          dataRegionId,
          historicalTransferSummary.toReportMessage());
    }

    final PipeTerminateEvent terminateEvent =
        new PipeTerminateEvent(
            pipeName,
            creationTime,
            pipeTaskMeta,
            dataRegionId,
            shouldTerminatePipeOnAllHistoricalEventsConsumed);
    if (!terminateEvent.increaseReferenceCount(
        PipeHistoricalDataRegionTsFileSource.class.getName())) {
      LOGGER.warn(
          "Pipe {}@{}: failed to increase reference count for terminate event, will resend it",
          pipeName,
          dataRegionId);
      return null;
    }
    isTerminateSignalSent = true;
    return terminateEvent;
  }

  private Event supplyProgressReportEvent(final ProgressIndex progressIndex) {
    final ProgressReportEvent progressReportEvent =
        new ProgressReportEvent(pipeName, creationTime, pipeTaskMeta);
    progressReportEvent.bindProgressIndex(progressIndex);
    final boolean isReferenceCountIncreased =
        progressReportEvent.increaseReferenceCount(
            PipeHistoricalDataRegionTsFileSource.class.getName());
    if (!isReferenceCountIncreased) {
      LOGGER.warn(
          "The reference count of the event {} cannot be increased, skipping it.",
          progressReportEvent);
    }
    return isReferenceCountIncreased ? progressReportEvent : null;
  }

  private Event supplyHistoricalProgressReportEvent(final ProgressIndex progressIndex) {
    maxSuppliedHistoricalProgressReportIndex =
        maxSuppliedHistoricalProgressReportIndex.updateToMinimumEqualOrIsAfterProgressIndex(
            progressIndex);
    return supplyProgressReportEvent(progressIndex);
  }

  private Event supplyTsFileEvent(final TsFileResource resource) {
    final PipeTsFileInsertionEvent event =
        new PipeTsFileInsertionEvent(
            resource,
            null,
            shouldTransferModFile,
            false,
            true,
            pipeName,
            creationTime,
            pipeTaskMeta,
            pipePattern,
            historicalDataExtractionStartTime,
            historicalDataExtractionEndTime);
    if (shouldUseHistoricalTsFileQueryPriorityOrder()) {
      event.skipReportOnCommitAndGeneratedEvents();
    }

    event.setTsFileParser(tsFileParser);
    if (sloppyPattern || isDbNameCoveredByPattern || isTsFileResourceCoveredByPattern(resource)) {
      event.skipParsingPattern();
    }

    if (sloppyTimeRange || isTsFileResourceCoveredByTimeRange(resource)) {
      event.skipParsingTime();
    }

    final boolean isReferenceCountIncreased =
        event.increaseReferenceCount(PipeHistoricalDataRegionTsFileSource.class.getName());
    if (!isReferenceCountIncreased) {
      // Unlike realtime supply, which already holds a queued event reference, this is the
      // event's first reference acquisition. The source's pin keeps the file available for retry.
      // Keep the queue entry and pin until an event owns the resource. A failed reference
      // acquisition must not advance historical progress past this file.
      throw new PipeException(
          String.format(
              DataNodePipeMessages
                  .EXCEPTION_INCREASE_REFERENCE_COUNT_ERROR_HOLDER_MESSAGE_ARG_7E1D032D,
              event.coreReportMessage()));
    }
    filteredTsFileResources.remove(resource);
    try {
      return event;
    } finally {
      try {
        unpinHistoricalTsFileResource(resource);
      } catch (final IOException | RuntimeException e) {
        LOGGER.warn(
            "Pipe {}@{}: failed to unpin TsFileResource after creating event, original path: {}",
            pipeName,
            dataRegionId,
            resource.getTsFilePath());
      }
    }
  }

  @Override
  public synchronized boolean hasConsumedAll() {
    // If the pendingQueue is null when the function is called, it implies that the extractor only
    // extracts deletion thus the historical event has nothing to consume.
    return hasBeenStarted
        && (Objects.isNull(pendingQueue) || pendingQueue.isEmpty() && isTerminateSignalSent);
  }

  @Override
  public int getPendingQueueSize() {
    return Objects.nonNull(pendingQueue) ? pendingQueue.size() : 0;
  }

  @Override
  public synchronized void close() {
    if (!isTerminateSignalSent) {
      PipeTerminateEvent.clearHistoricalTransferSummary(pipeName, creationTime, dataRegionId);
    }
    final Set<TsFileResource> pinnedResources = new HashSet<>(resourcesWithPinnedTsFile);
    pinnedResources.addAll(resourcesWithPinnedModFile);
    pinnedResources.forEach(
        resource -> {
          try {
            unpinHistoricalTsFileResource(resource);
          } catch (final IOException | RuntimeException e) {
            LOGGER.warn(
                "Pipe {}@{}: failed to unpin TsFileResource after dropping pipe, original path: {}",
                pipeName,
                dataRegionId,
                resource.getTsFilePath(),
                e);
          }
        });
    if (Objects.nonNull(pendingQueue)) {
      pendingQueue.clear();
      pendingQueue = null;
    }
    filteredTsFileResources.clear();
    historicalProgressReportResources.clear();
    pendingHistoricalProgressIndexToReport = null;
  }
}
