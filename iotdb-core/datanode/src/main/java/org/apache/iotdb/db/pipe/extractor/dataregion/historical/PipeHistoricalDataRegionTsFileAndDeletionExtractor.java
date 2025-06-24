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

package org.apache.iotdb.db.pipe.extractor.dataregion.historical;

import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.commons.consensus.index.ProgressIndex;
import org.apache.iotdb.commons.consensus.index.ProgressIndexType;
import org.apache.iotdb.commons.consensus.index.impl.HybridProgressIndex;
import org.apache.iotdb.commons.consensus.index.impl.RecoverProgressIndex;
import org.apache.iotdb.commons.consensus.index.impl.StateProgressIndex;
import org.apache.iotdb.commons.consensus.index.impl.TimeWindowStateProgressIndex;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeStaticMeta;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeTaskMeta;
import org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant;
import org.apache.iotdb.commons.pipe.config.constant.SystemConstant;
import org.apache.iotdb.commons.pipe.config.plugin.env.PipeTaskExtractorRuntimeEnvironment;
import org.apache.iotdb.commons.pipe.datastructure.pattern.TablePattern;
import org.apache.iotdb.commons.pipe.datastructure.pattern.TreePattern;
import org.apache.iotdb.commons.pipe.datastructure.resource.PersistentResource;
import org.apache.iotdb.commons.utils.PathUtils;
import org.apache.iotdb.consensus.pipe.PipeConsensus;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.consensus.DataRegionConsensusImpl;
import org.apache.iotdb.db.pipe.consensus.ReplicateProgressDataNodeManager;
import org.apache.iotdb.db.pipe.consensus.deletion.DeletionResource;
import org.apache.iotdb.db.pipe.consensus.deletion.DeletionResourceManager;
import org.apache.iotdb.db.pipe.event.common.deletion.PipeDeleteDataNodeEvent;
import org.apache.iotdb.db.pipe.event.common.terminate.PipeTerminateEvent;
import org.apache.iotdb.db.pipe.event.common.tsfile.PipeTsFileInsertionEvent;
import org.apache.iotdb.db.pipe.extractor.dataregion.DataRegionListeningFilter;
import org.apache.iotdb.db.pipe.processor.pipeconsensus.PipeConsensusProcessor;
import org.apache.iotdb.db.pipe.resource.PipeDataNodeResourceManager;
import org.apache.iotdb.db.pipe.resource.tsfile.PipeTsFileResourceManager;
import org.apache.iotdb.db.storageengine.StorageEngine;
import org.apache.iotdb.db.storageengine.dataregion.DataRegion;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileManager;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.generator.TsFileNameGenerator;
import org.apache.iotdb.db.utils.DateTimeUtils;
import org.apache.iotdb.pipe.api.customizer.configuration.PipeExtractorRuntimeConfiguration;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameterValidator;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;
import org.apache.iotdb.pipe.api.event.Event;
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
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.EXTRACTOR_END_TIME_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.EXTRACTOR_HISTORY_ENABLE_DEFAULT_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.EXTRACTOR_HISTORY_ENABLE_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.EXTRACTOR_HISTORY_END_TIME_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.EXTRACTOR_HISTORY_LOOSE_RANGE_ALL_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.EXTRACTOR_HISTORY_LOOSE_RANGE_DEFAULT_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.EXTRACTOR_HISTORY_LOOSE_RANGE_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.EXTRACTOR_HISTORY_LOOSE_RANGE_PATH_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.EXTRACTOR_HISTORY_LOOSE_RANGE_TIME_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.EXTRACTOR_HISTORY_START_TIME_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.EXTRACTOR_MODE_STRICT_DEFAULT_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.EXTRACTOR_MODE_STRICT_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.EXTRACTOR_MODS_DEFAULT_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.EXTRACTOR_MODS_ENABLE_DEFAULT_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.EXTRACTOR_MODS_ENABLE_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.EXTRACTOR_MODS_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.EXTRACTOR_START_TIME_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.SOURCE_END_TIME_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.SOURCE_HISTORY_ENABLE_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.SOURCE_HISTORY_END_TIME_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.SOURCE_HISTORY_LOOSE_RANGE_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.SOURCE_HISTORY_START_TIME_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.SOURCE_MODE_STRICT_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.SOURCE_MODS_ENABLE_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.SOURCE_MODS_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.SOURCE_START_TIME_KEY;
import static org.apache.iotdb.commons.pipe.extractor.IoTDBExtractor.getSkipIfNoPrivileges;
import static org.apache.tsfile.common.constant.TsFileConstant.PATH_ROOT;
import static org.apache.tsfile.common.constant.TsFileConstant.PATH_SEPARATOR;

public class PipeHistoricalDataRegionTsFileAndDeletionExtractor
    implements PipeHistoricalDataRegionExtractor {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(PipeHistoricalDataRegionTsFileAndDeletionExtractor.class);

  private static final Map<Integer, Long> DATA_REGION_ID_TO_PIPE_FLUSHED_TIME_MAP = new HashMap<>();
  private static final long PIPE_MIN_FLUSH_INTERVAL_IN_MS = 2000;

  private static final String TREE_MODEL_EVENT_TABLE_NAME_PREFIX = PATH_ROOT + PATH_SEPARATOR;

  private String pipeName;
  private long creationTime;

  private PipeTaskMeta pipeTaskMeta;
  private ProgressIndex startIndex;

  private int dataRegionId;

  private TreePattern treePattern;
  private TablePattern tablePattern;

  private boolean isModelDetected = false;
  private boolean isTableModel;
  private boolean isDbNameCoveredByPattern = false;

  private boolean isHistoricalExtractorEnabled = false;
  private long historicalDataExtractionStartTime = Long.MIN_VALUE; // Event time
  private long historicalDataExtractionEndTime = Long.MAX_VALUE; // Event time
  private long historicalDataExtractionTimeLowerBound; // Arrival time

  private boolean sloppyTimeRange; // true to disable time range filter after extraction
  private boolean sloppyPattern; // true to disable pattern filter after extraction

  private Pair<Boolean, Boolean> listeningOptionPair;
  private boolean shouldExtractInsertion;
  private boolean shouldExtractDeletion;
  private boolean shouldTransferModFile; // Whether to transfer mods
  protected String userName;
  protected boolean skipIfNoPrivileges = true;
  private boolean isTerminateSignalSent = false;

  private volatile boolean hasBeenStarted = false;

  private Queue<PersistentResource> pendingQueue;

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

    if (parameters.hasAnyAttributes(EXTRACTOR_MODE_STRICT_KEY, SOURCE_MODE_STRICT_KEY)) {
      final boolean isStrictMode =
          parameters.getBooleanOrDefault(
              Arrays.asList(EXTRACTOR_MODE_STRICT_KEY, SOURCE_MODE_STRICT_KEY),
              EXTRACTOR_MODE_STRICT_DEFAULT_VALUE);
      sloppyTimeRange = !isStrictMode;
      sloppyPattern = !isStrictMode;
    } else {
      final String extractorHistoryLooseRangeValue =
          parameters
              .getStringOrDefault(
                  Arrays.asList(EXTRACTOR_HISTORY_LOOSE_RANGE_KEY, SOURCE_HISTORY_LOOSE_RANGE_KEY),
                  EXTRACTOR_HISTORY_LOOSE_RANGE_DEFAULT_VALUE)
              .trim();
      if (EXTRACTOR_HISTORY_LOOSE_RANGE_ALL_VALUE.equalsIgnoreCase(
          extractorHistoryLooseRangeValue)) {
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
                  "Parameters in set %s are not allowed in 'history.loose-range'",
                  sloppyOptionSet));
        }
      }
    }

    if (parameters.hasAnyAttributes(
        SOURCE_START_TIME_KEY,
        EXTRACTOR_START_TIME_KEY,
        SOURCE_END_TIME_KEY,
        EXTRACTOR_END_TIME_KEY)) {
      isHistoricalExtractorEnabled = true;

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

    // Historical data extraction is enabled in the following cases:
    // 1. System restarts the pipe. If the pipe is restarted but historical data extraction is not
    // enabled, the pipe will lose some historical data.
    // 2. User may set the EXTRACTOR_HISTORY_START_TIME and EXTRACTOR_HISTORY_END_TIME without
    // enabling the historical data extraction, which may affect the realtime data extraction.
    isHistoricalExtractorEnabled =
        parameters.getBooleanOrDefault(
                SystemConstant.RESTART_KEY, SystemConstant.RESTART_DEFAULT_VALUE)
            || parameters.getBooleanOrDefault(
                Arrays.asList(EXTRACTOR_HISTORY_ENABLE_KEY, SOURCE_HISTORY_ENABLE_KEY),
                EXTRACTOR_HISTORY_ENABLE_DEFAULT_VALUE);

    try {
      historicalDataExtractionStartTime =
          isHistoricalExtractorEnabled
                  && parameters.hasAnyAttributes(
                      EXTRACTOR_HISTORY_START_TIME_KEY, SOURCE_HISTORY_START_TIME_KEY)
              ? DateTimeUtils.convertTimestampOrDatetimeStrToLongWithDefaultZone(
                  parameters.getStringByKeys(
                      EXTRACTOR_HISTORY_START_TIME_KEY, SOURCE_HISTORY_START_TIME_KEY))
              : Long.MIN_VALUE;
      historicalDataExtractionEndTime =
          isHistoricalExtractorEnabled
                  && parameters.hasAnyAttributes(
                      EXTRACTOR_HISTORY_END_TIME_KEY, SOURCE_HISTORY_END_TIME_KEY)
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
    shouldExtractDeletion = listeningOptionPair.getRight();
    // Do nothing if extract deletion
    if (!shouldExtractInsertion) {
      return;
    }

    final PipeTaskExtractorRuntimeEnvironment environment =
        (PipeTaskExtractorRuntimeEnvironment) configuration.getRuntimeEnvironment();

    pipeName = environment.getPipeName();
    creationTime = environment.getCreationTime();
    pipeTaskMeta = environment.getPipeTaskMeta();

    // progressIndex is immutable in `updateToMinimumEqualOrIsAfterProgressIndex`, so data
    // consistency in `environment.getPipeTaskMeta().getProgressIndex()` is ensured.
    startIndex = environment.getPipeTaskMeta().restoreProgressIndex();
    if (pipeName.startsWith(PipeStaticMeta.CONSENSUS_PIPE_PREFIX)) {
      startIndex = tryToExtractLocalProgressIndexForIoTV2(startIndex);
    }

    dataRegionId = environment.getRegionId();
    synchronized (DATA_REGION_ID_TO_PIPE_FLUSHED_TIME_MAP) {
      DATA_REGION_ID_TO_PIPE_FLUSHED_TIME_MAP.putIfAbsent(dataRegionId, 0L);
    }

    treePattern = TreePattern.parsePipePatternFromSourceParameters(parameters);
    tablePattern = TablePattern.parsePipePatternFromSourceParameters(parameters);

    final DataRegion dataRegion =
        StorageEngine.getInstance().getDataRegion(new DataRegionId(environment.getRegionId()));
    if (Objects.nonNull(dataRegion)) {
      final String databaseName = dataRegion.getDatabaseName();
      if (Objects.nonNull(databaseName)) {
        isTableModel = PathUtils.isTableModelDatabase(databaseName);
        isModelDetected = true;
        if (isTableModel) {
          isDbNameCoveredByPattern = tablePattern.coversDb(databaseName);
        } else {
          isDbNameCoveredByPattern = treePattern.coversDb(databaseName);
        }
      }
    }

    // Enable historical extractor by default
    historicalDataExtractionTimeLowerBound =
        isHistoricalExtractorEnabled
            ? Long.MIN_VALUE
            // We define the realtime data as the data generated after the creation time
            // of the pipe from user's perspective. But we still need to use
            // PipeHistoricalDataRegionExtractor to extract the realtime data generated between the
            // creation time of the pipe and the time when the pipe starts, because those data
            // can not be listened by PipeRealtimeDataRegionExtractor, and should be extracted by
            // PipeHistoricalDataRegionExtractor from implementation perspective.
            : environment.getCreationTime();

    // Only invoke flushDataRegionAllTsFiles() when the pipe runs in the realtime only mode.
    // realtime only mode -> (historicalDataExtractionTimeLowerBound != Long.MIN_VALUE)
    //
    // Ensure that all data in the data region is flushed to disk before extracting data.
    // This ensures the generation time of all newly generated TsFiles (realtime data) after the
    // invocation of flushDataRegionAllTsFiles() is later than the creationTime of the pipe
    // (historicalDataExtractionTimeLowerBound).
    //
    // Note that: the generation time of the TsFile is the time when the TsFile is created, not
    // the time when the data is flushed to the TsFile.
    //
    // Then we can use the generation time of the TsFile to determine whether the data in the
    // TsFile should be extracted by comparing the generation time of the TsFile with the
    // historicalDataExtractionTimeLowerBound when starting the pipe in realtime only mode.
    //
    // If we don't invoke flushDataRegionAllTsFiles() in the realtime only mode, the data generated
    // between the creation time of the pipe the time when the pipe starts will be lost.
    if (historicalDataExtractionTimeLowerBound != Long.MIN_VALUE) {
      synchronized (DATA_REGION_ID_TO_PIPE_FLUSHED_TIME_MAP) {
        final long lastFlushedByPipeTime =
            DATA_REGION_ID_TO_PIPE_FLUSHED_TIME_MAP.get(dataRegionId);
        if (System.currentTimeMillis() - lastFlushedByPipeTime >= PIPE_MIN_FLUSH_INTERVAL_IN_MS) {
          flushDataRegionAllTsFiles();
          DATA_REGION_ID_TO_PIPE_FLUSHED_TIME_MAP.replace(dataRegionId, System.currentTimeMillis());
        }
      }
    }

    if (parameters.hasAnyAttributes(EXTRACTOR_MODS_KEY, SOURCE_MODS_KEY)) {
      shouldTransferModFile =
          parameters.getBooleanOrDefault(
              Arrays.asList(EXTRACTOR_MODS_KEY, SOURCE_MODS_KEY),
              EXTRACTOR_MODS_DEFAULT_VALUE
                  || // Should extract deletion
                  listeningOptionPair.getRight());
    } else {
      shouldTransferModFile =
          parameters.getBooleanOrDefault(
              Arrays.asList(SOURCE_MODS_ENABLE_KEY, EXTRACTOR_MODS_ENABLE_KEY),
              EXTRACTOR_MODS_ENABLE_DEFAULT_VALUE
                  || // Should extract deletion
                  listeningOptionPair.getRight());
    }

    userName =
        parameters.getStringByKeys(
            PipeExtractorConstant.EXTRACTOR_IOTDB_USER_KEY,
            PipeExtractorConstant.SOURCE_IOTDB_USER_KEY,
            PipeExtractorConstant.EXTRACTOR_IOTDB_USERNAME_KEY,
            PipeExtractorConstant.SOURCE_IOTDB_USERNAME_KEY);

    skipIfNoPrivileges = getSkipIfNoPrivileges(parameters);

    if (LOGGER.isInfoEnabled()) {
      LOGGER.info(
          "Pipe {}@{}: historical data extraction time range, start time {}({}), end time {}({}), sloppy pattern {}, sloppy time range {}, should transfer mod file {}, username: {}, skip if no privileges: {}",
          pipeName,
          dataRegionId,
          DateTimeUtils.convertLongToDate(historicalDataExtractionStartTime),
          historicalDataExtractionStartTime,
          DateTimeUtils.convertLongToDate(historicalDataExtractionEndTime),
          historicalDataExtractionEndTime,
          sloppyPattern,
          sloppyTimeRange,
          shouldTransferModFile,
          userName,
          skipIfNoPrivileges);
    }
  }

  private void flushDataRegionAllTsFiles() {
    final DataRegion dataRegion =
        StorageEngine.getInstance().getDataRegion(new DataRegionId(dataRegionId));
    if (Objects.isNull(dataRegion)) {
      return;
    }

    dataRegion.writeLock("Pipe: create historical TsFile extractor");
    try {
      dataRegion.syncCloseAllWorkingTsFileProcessors();
    } finally {
      dataRegion.writeUnlock();
    }
  }

  /**
   * IoTV2 will only resend event that contains un-replicated local write data. So we only extract
   * ProgressIndex containing local writes for comparison to prevent misjudgment on whether
   * high-level tsFiles with mixed progressIndexes need to be retransmitted
   *
   * @return recoverProgressIndex dedicated in local DataNodeId or origin for fallback.
   */
  private ProgressIndex tryToExtractLocalProgressIndexForIoTV2(ProgressIndex origin) {
    // There are only 2 cases:
    // 1. origin is RecoverProgressIndex
    if (origin instanceof RecoverProgressIndex) {
      RecoverProgressIndex toBeTransformed = (RecoverProgressIndex) origin;
      return extractRecoverProgressIndex(toBeTransformed);
    }
    // 2. origin is HybridProgressIndex
    else if (origin instanceof HybridProgressIndex) {
      HybridProgressIndex toBeTransformed = (HybridProgressIndex) origin;
      // if hybridProgressIndex contains recoverProgressIndex, which is what we expected.
      if (toBeTransformed
          .getType2Index()
          .containsKey(ProgressIndexType.RECOVER_PROGRESS_INDEX.getType())) {
        // 2.1. transform recoverProgressIndex
        RecoverProgressIndex specificToBeTransformed =
            (RecoverProgressIndex)
                toBeTransformed
                    .getType2Index()
                    .get(ProgressIndexType.RECOVER_PROGRESS_INDEX.getType());
        return extractRecoverProgressIndex(specificToBeTransformed);
      }
      // if hybridProgressIndex doesn't contain recoverProgressIndex, which is not what we expected,
      // fallback.
      return origin;
    } else {
      // fallback
      LOGGER.warn(
          "Pipe {}@{}: unexpected ProgressIndex type {}, fallback to origin {}.",
          pipeName,
          dataRegionId,
          origin.getType(),
          origin);
      return origin;
    }
  }

  private ProgressIndex extractRecoverProgressIndex(RecoverProgressIndex toBeTransformed) {
    return new RecoverProgressIndex(
        toBeTransformed.getDataNodeId2LocalIndex().entrySet().stream()
            .filter(
                entry ->
                    entry
                        .getKey()
                        .equals(IoTDBDescriptor.getInstance().getConfig().getDataNodeId()))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));
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

    final DataRegion dataRegion =
        StorageEngine.getInstance().getDataRegion(new DataRegionId(dataRegionId));
    if (Objects.isNull(dataRegion)) {
      pendingQueue = new ArrayDeque<>();
      return;
    }

    final long startHistoricalExtractionTime = System.currentTimeMillis();
    dataRegion.writeLock(
        "Pipe: start to extract historical TsFile and Deletion(if uses pipeConsensus)");
    try {
      List<PersistentResource> resourceList = new ArrayList<>();

      if (shouldExtractInsertion) {
        flushTsFilesForExtraction(dataRegion, startHistoricalExtractionTime);
        extractTsFiles(dataRegion, startHistoricalExtractionTime, resourceList);
      }
      if (shouldExtractDeletion) {
        Optional.ofNullable(DeletionResourceManager.getInstance(String.valueOf(dataRegionId)))
            .ifPresent(manager -> extractDeletions(manager, resourceList));
      }

      // Sort tsFileResource and deletionResource
      long startTime = System.currentTimeMillis();
      LOGGER.info("Pipe {}@{}: start to sort all extracted resources", pipeName, dataRegionId);
      resourceList.sort(
          (o1, o2) ->
              startIndex instanceof TimeWindowStateProgressIndex
                  ? Long.compare(o1.getFileStartTime(), o2.getFileStartTime())
                  : o1.getProgressIndex().topologicalCompareTo(o2.getProgressIndex()));
      pendingQueue = new ArrayDeque<>(resourceList);

      LOGGER.info(
          "Pipe {}@{}: finish to sort all extracted resources, took {} ms",
          pipeName,
          dataRegionId,
          System.currentTimeMillis() - startTime);
    } finally {
      dataRegion.writeUnlock();
    }
  }

  private void flushTsFilesForExtraction(
      DataRegion dataRegion, final long startHistoricalExtractionTime) {
    LOGGER.info("Pipe {}@{}: start to flush data region", pipeName, dataRegionId);

    // Consider the scenario: a consensus pipe comes to the same region, followed by another pipe
    // **immediately**, the latter pipe will skip the flush operation.
    // Since a large number of consensus pipes are not created at the same time, resulting in no
    // serious waiting for locks. Therefore, the flush operation is always performed for the
    // consensus pipe, and the lastFlushed timestamp is not updated here.
    if (pipeName.startsWith(PipeStaticMeta.CONSENSUS_PIPE_PREFIX)) {
      dataRegion.syncCloseAllWorkingTsFileProcessors();
      LOGGER.info(
          "Pipe {}@{}: finish to flush data region, took {} ms",
          pipeName,
          dataRegionId,
          System.currentTimeMillis() - startHistoricalExtractionTime);
      return;
    }

    synchronized (DATA_REGION_ID_TO_PIPE_FLUSHED_TIME_MAP) {
      final long lastFlushedByPipeTime = DATA_REGION_ID_TO_PIPE_FLUSHED_TIME_MAP.get(dataRegionId);
      if (System.currentTimeMillis() - lastFlushedByPipeTime >= PIPE_MIN_FLUSH_INTERVAL_IN_MS) {
        dataRegion.syncCloseAllWorkingTsFileProcessors();
        DATA_REGION_ID_TO_PIPE_FLUSHED_TIME_MAP.replace(dataRegionId, System.currentTimeMillis());
        LOGGER.info(
            "Pipe {}@{}: finish to flush data region, took {} ms",
            pipeName,
            dataRegionId,
            System.currentTimeMillis() - startHistoricalExtractionTime);
      } else {
        LOGGER.info(
            "Pipe {}@{}: skip to flush data region, last flushed time {} ms ago",
            pipeName,
            dataRegionId,
            System.currentTimeMillis() - lastFlushedByPipeTime);
      }
    }
  }

  private void extractTsFiles(
      final DataRegion dataRegion,
      final long startHistoricalExtractionTime,
      final List<PersistentResource> resourceList) {
    final TsFileManager tsFileManager = dataRegion.getTsFileManager();
    tsFileManager.readLock();
    try {
      final int originalSequenceTsFileCount = tsFileManager.size(true);
      final int originalUnsequenceTsFileCount = tsFileManager.size(false);
      LOGGER.info(
          "Pipe {}@{}: start to extract historical TsFile, original sequence file count {}, "
              + "original unsequence file count {}, start progress index {}",
          pipeName,
          dataRegionId,
          originalSequenceTsFileCount,
          originalUnsequenceTsFileCount,
          startIndex);

      final Collection<TsFileResource> sequenceTsFileResources =
          tsFileManager.getTsFileList(true).stream()
              .filter(
                  resource ->
                      // Some resource is marked as deleted but not removed from the list.
                      !resource.isDeleted()
                          && (
                          // Some resource may not be closed due to the control of
                          // PIPE_MIN_FLUSH_INTERVAL_IN_MS. We simply ignore them.
                          !resource.isClosed()
                              || mayTsFileContainUnprocessedData(resource)
                                  && isTsFileResourceOverlappedWithTimeRange(resource)
                                  && isTsFileGeneratedAfterExtractionTimeLowerBound(resource)
                                  && mayTsFileResourceOverlappedWithPattern(resource)))
              .collect(Collectors.toList());
      resourceList.addAll(sequenceTsFileResources);

      final Collection<TsFileResource> unsequenceTsFileResources =
          tsFileManager.getTsFileList(false).stream()
              .filter(
                  resource ->
                      // Some resource is marked as deleted but not removed from the list.
                      !resource.isDeleted()
                          && (
                          // Some resource may not be closed due to the control of
                          // PIPE_MIN_FLUSH_INTERVAL_IN_MS. We simply ignore them.
                          !resource.isClosed()
                              || mayTsFileContainUnprocessedData(resource)
                                  && isTsFileResourceOverlappedWithTimeRange(resource)
                                  && isTsFileGeneratedAfterExtractionTimeLowerBound(resource)
                                  && mayTsFileResourceOverlappedWithPattern(resource)))
              .collect(Collectors.toList());
      resourceList.addAll(unsequenceTsFileResources);

      resourceList.removeIf(
          resource -> {
            // Pin the resource, in case the file is removed by compaction or anything.
            // Will unpin it after the PipeTsFileInsertionEvent is created and pinned.
            try {
              PipeDataNodeResourceManager.tsfile()
                  .pinTsFileResource((TsFileResource) resource, shouldTransferModFile);
              return false;
            } catch (final IOException e) {
              LOGGER.warn(
                  "Pipe: failed to pin TsFileResource {}",
                  ((TsFileResource) resource).getTsFilePath(),
                  e);
              return true;
            }
          });

      LOGGER.info(
          "Pipe {}@{}: finish to extract historical TsFile, extracted sequence file count {}/{}, "
              + "extracted unsequence file count {}/{}, extracted file count {}/{}, took {} ms",
          pipeName,
          dataRegionId,
          sequenceTsFileResources.size(),
          originalSequenceTsFileCount,
          unsequenceTsFileResources.size(),
          originalUnsequenceTsFileCount,
          resourceList.size(),
          originalSequenceTsFileCount + originalUnsequenceTsFileCount,
          System.currentTimeMillis() - startHistoricalExtractionTime);
    } finally {
      tsFileManager.readUnlock();
    }
  }

  private boolean mayTsFileContainUnprocessedData(final TsFileResource resource) {
    if (startIndex instanceof TimeWindowStateProgressIndex) {
      // The resource is closed thus the TsFileResource#getFileEndTime() is safe to use
      return ((TimeWindowStateProgressIndex) startIndex).getMinTime() <= resource.getFileEndTime();
    }

    if (startIndex instanceof StateProgressIndex) {
      startIndex = ((StateProgressIndex) startIndex).getInnerProgressIndex();
    }

    if (pipeName.startsWith(PipeStaticMeta.CONSENSUS_PIPE_PREFIX)) {
      // For consensus pipe, we only focus on the progressIndex that is generated from local write
      // instead of replication or something else.
      ProgressIndex dedicatedProgressIndex =
          tryToExtractLocalProgressIndexForIoTV2(resource.getMaxProgressIndexAfterClose());
      return greaterThanStartIndex(dedicatedProgressIndex);
    }
    return greaterThanStartIndex(resource.getMaxProgressIndexAfterClose());
  }

  private boolean greaterThanStartIndex(ProgressIndex progressIndex) {
    return !startIndex.isAfter(progressIndex) && !startIndex.equals(progressIndex);
  }

  private boolean mayTsFileResourceOverlappedWithPattern(final TsFileResource resource) {
    final Set<IDeviceID> deviceSet;
    try {
      final Map<IDeviceID, Boolean> deviceIsAlignedMap =
          PipeDataNodeResourceManager.tsfile()
              .getDeviceIsAlignedMapFromCache(
                  PipeTsFileResourceManager.getHardlinkOrCopiedFileInPipeDir(resource.getTsFile()),
                  false);
      deviceSet =
          Objects.nonNull(deviceIsAlignedMap) ? deviceIsAlignedMap.keySet() : resource.getDevices();
    } catch (final IOException e) {
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
            deviceID -> {
              if (!isModelDetected) {
                detectModel(resource, deviceID);
                isModelDetected = true;
              }

              return isTableModel
                  ? (tablePattern.isTableModelDataAllowedToBeCaptured()
                      && tablePattern.matchesDatabase(resource.getDatabaseName())
                      && tablePattern.matchesTable(deviceID.getTableName()))
                  : (treePattern.isTreeModelDataAllowedToBeCaptured()
                      && treePattern.mayOverlapWithDevice(deviceID));
            });
  }

  private void detectModel(final TsFileResource resource, final IDeviceID deviceID) {
    this.isTableModel =
        !(deviceID instanceof PlainDeviceID
            || deviceID.getTableName().startsWith(TREE_MODEL_EVENT_TABLE_NAME_PREFIX)
            || deviceID.getTableName().equals(PATH_ROOT));

    final String databaseName = resource.getDatabaseName();
    isDbNameCoveredByPattern =
        isTableModel
            ? tablePattern.isTableModelDataAllowedToBeCaptured()
                && tablePattern.coversDb(databaseName)
            : treePattern.isTreeModelDataAllowedToBeCaptured()
                && treePattern.coversDb(databaseName);
  }

  private boolean isTsFileResourceOverlappedWithTimeRange(final TsFileResource resource) {
    return !(resource.getFileEndTime() < historicalDataExtractionStartTime
        || historicalDataExtractionEndTime < resource.getFileStartTime());
  }

  private boolean isTsFileResourceCoveredByTimeRange(final TsFileResource resource) {
    return historicalDataExtractionStartTime <= resource.getFileStartTime()
        && historicalDataExtractionEndTime >= resource.getFileEndTime();
  }

  private boolean isTsFileGeneratedAfterExtractionTimeLowerBound(final TsFileResource resource) {
    try {
      return historicalDataExtractionTimeLowerBound
          <= TsFileNameGenerator.getTsFileName(resource.getTsFile().getName()).getTime();
    } catch (final IOException e) {
      LOGGER.warn(
          "Pipe {}@{}: failed to get the generation time of TsFile {}, extract it anyway"
              + " (historical data extraction time lower bound: {})",
          pipeName,
          dataRegionId,
          resource.getTsFilePath(),
          historicalDataExtractionTimeLowerBound,
          e);
      // If failed to get the generation time of the TsFile, we will extract the data in the TsFile
      // anyway.
      return true;
    }
  }

  private void extractDeletions(
      final DeletionResourceManager deletionResourceManager,
      final List<PersistentResource> resourceList) {
    LOGGER.info("Pipe {}@{}: start to extract deletions", pipeName, dataRegionId);
    long startTime = System.currentTimeMillis();
    List<DeletionResource> allDeletionResources = deletionResourceManager.getAllDeletionResources();
    final int originalDeletionCount = allDeletionResources.size();
    // For deletions that are filtered and will not be sent, we should manually decrease its
    // reference count. Because the initial value of referenceCount is `ReplicaNum - 1`
    allDeletionResources.stream()
        .filter(
            resource -> {
              ProgressIndex toBeCompared = resource.getProgressIndex();
              if (pipeName.startsWith(PipeStaticMeta.CONSENSUS_PIPE_PREFIX)) {
                toBeCompared = tryToExtractLocalProgressIndexForIoTV2(toBeCompared);
              }
              return !greaterThanStartIndex(toBeCompared);
            })
        .forEach(DeletionResource::decreaseReference);
    // Get deletions that should be sent.
    allDeletionResources =
        allDeletionResources.stream()
            .filter(
                resource -> {
                  ProgressIndex toBeCompared = resource.getProgressIndex();
                  if (pipeName.startsWith(PipeStaticMeta.CONSENSUS_PIPE_PREFIX)) {
                    toBeCompared = tryToExtractLocalProgressIndexForIoTV2(toBeCompared);
                  }
                  return greaterThanStartIndex(toBeCompared);
                })
            .collect(Collectors.toList());
    resourceList.addAll(allDeletionResources);
    LOGGER.info(
        "Pipe {}@{}: finish to extract deletions, extract deletions count {}/{}, took {} ms",
        pipeName,
        dataRegionId,
        allDeletionResources.size(),
        originalDeletionCount,
        System.currentTimeMillis() - startTime);
  }

  @Override
  public synchronized Event supply() {
    if (!hasBeenStarted && StorageEngine.getInstance().isReadyForNonReadWriteFunctions()) {
      start();
    }

    if (Objects.isNull(pendingQueue)) {
      return null;
    }

    final PersistentResource resource = pendingQueue.poll();
    if (resource == null) {
      return supplyTerminateEvent();
    } else if (resource instanceof TsFileResource) {
      return supplyTsFileEvent((TsFileResource) resource);
    } else {
      return supplyDeletionEvent((DeletionResource) resource);
    }
  }

  private Event supplyTerminateEvent() {
    final PipeTerminateEvent terminateEvent =
        new PipeTerminateEvent(pipeName, creationTime, pipeTaskMeta, dataRegionId);
    if (!terminateEvent.increaseReferenceCount(
        PipeHistoricalDataRegionTsFileAndDeletionExtractor.class.getName())) {
      LOGGER.warn(
          "Pipe {}@{}: failed to increase reference count for terminate event, will resend it",
          pipeName,
          dataRegionId);
      return null;
    }
    isTerminateSignalSent = true;
    return terminateEvent;
  }

  private Event supplyTsFileEvent(final TsFileResource resource) {
    final PipeTsFileInsertionEvent event =
        new PipeTsFileInsertionEvent(
            isModelDetected ? isTableModel : null,
            resource.getDatabaseName(),
            resource,
            shouldTransferModFile,
            false,
            true,
            pipeName,
            creationTime,
            pipeTaskMeta,
            treePattern,
            tablePattern,
            userName,
            skipIfNoPrivileges,
            historicalDataExtractionStartTime,
            historicalDataExtractionEndTime);

    // if using IoTV2, assign a replicateIndex for this event
    if (DataRegionConsensusImpl.getInstance() instanceof PipeConsensus
        && PipeConsensusProcessor.isShouldReplicate(event)) {
      event.setReplicateIndexForIoTV2(
          ReplicateProgressDataNodeManager.assignReplicateIndexForIoTV2(pipeName));
      LOGGER.info(
          "[{}]Set {} for historical event {}", pipeName, event.getReplicateIndexForIoTV2(), event);
    }

    if (sloppyPattern || isDbNameCoveredByPattern) {
      event.skipParsingPattern();
    }
    if (sloppyTimeRange || isTsFileResourceCoveredByTimeRange(resource)) {
      event.skipParsingTime();
    }

    try {
      final boolean isReferenceCountIncreased =
          event.increaseReferenceCount(
              PipeHistoricalDataRegionTsFileAndDeletionExtractor.class.getName());
      if (!isReferenceCountIncreased) {
        LOGGER.warn(
            "Pipe {}@{}: failed to increase reference count for historical tsfile event {}, will discard it",
            pipeName,
            dataRegionId,
            event);
      }
      return isReferenceCountIncreased ? event : null;
    } finally {
      try {
        PipeDataNodeResourceManager.tsfile().unpinTsFileResource(resource);
      } catch (final IOException e) {
        LOGGER.warn(
            "Pipe {}@{}: failed to unpin TsFileResource after creating event, original path: {}",
            pipeName,
            dataRegionId,
            resource.getTsFilePath());
      }
    }
  }

  private Event supplyDeletionEvent(final DeletionResource deletionResource) {
    final PipeDeleteDataNodeEvent event =
        new PipeDeleteDataNodeEvent(
            deletionResource.getDeleteDataNode(),
            pipeName,
            creationTime,
            pipeTaskMeta,
            treePattern,
            tablePattern,
            userName,
            skipIfNoPrivileges,
            false);

    if (sloppyPattern || isDbNameCoveredByPattern) {
      event.skipParsingPattern();
    }
    if (sloppyTimeRange) {
      event.skipParsingTime();
    }

    final boolean isReferenceCountIncreased =
        event.increaseReferenceCount(
            PipeHistoricalDataRegionTsFileAndDeletionExtractor.class.getName());
    if (!isReferenceCountIncreased) {
      LOGGER.warn(
          "Pipe {}@{}: failed to increase reference count for historical deletion event {}, will discard it",
          pipeName,
          dataRegionId,
          event);
    } else {
      Optional.ofNullable(DeletionResourceManager.getInstance(String.valueOf(dataRegionId)))
          .ifPresent(
              manager ->
                  event.setDeletionResource(
                      manager.getDeletionResource(event.getDeleteDataNode())));
    }
    return isReferenceCountIncreased ? event : null;
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
    if (Objects.nonNull(pendingQueue)) {
      pendingQueue.forEach(
          resource -> {
            if (resource instanceof TsFileResource) {
              try {
                PipeDataNodeResourceManager.tsfile().unpinTsFileResource((TsFileResource) resource);
              } catch (final IOException e) {
                LOGGER.warn(
                    "Pipe {}@{}: failed to unpin TsFileResource after dropping pipe, original path: {}",
                    pipeName,
                    dataRegionId,
                    ((TsFileResource) resource).getTsFilePath());
              }
            }
          });
      pendingQueue.clear();
      pendingQueue = null;
    }
  }
}
