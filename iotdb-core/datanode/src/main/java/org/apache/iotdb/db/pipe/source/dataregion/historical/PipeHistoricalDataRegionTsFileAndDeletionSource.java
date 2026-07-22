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
import org.apache.iotdb.commons.consensus.index.impl.RecoverProgressIndex;
import org.apache.iotdb.commons.consensus.index.impl.StateProgressIndex;
import org.apache.iotdb.commons.consensus.index.impl.TimePartitionProgressIndex;
import org.apache.iotdb.commons.consensus.index.impl.TimeWindowStateProgressIndex;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.pipe.agent.task.PipeTaskAgent;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeStaticMeta;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeTaskMeta;
import org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant;
import org.apache.iotdb.commons.pipe.config.constant.SystemConstant;
import org.apache.iotdb.commons.pipe.config.plugin.env.PipeTaskSourceRuntimeEnvironment;
import org.apache.iotdb.commons.pipe.datastructure.pattern.TablePattern;
import org.apache.iotdb.commons.pipe.datastructure.pattern.TreePattern;
import org.apache.iotdb.commons.pipe.datastructure.resource.PersistentResource;
import org.apache.iotdb.commons.pipe.event.EnrichedEvent;
import org.apache.iotdb.commons.pipe.event.ProgressReportEvent;
import org.apache.iotdb.commons.queryengine.utils.DateTimeUtils;
import org.apache.iotdb.commons.utils.PathUtils;
import org.apache.iotdb.consensus.pipe.IoTConsensusV2;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.consensus.DataRegionConsensusImpl;
import org.apache.iotdb.db.i18n.DataNodePipeMessages;
import org.apache.iotdb.db.pipe.consensus.ReplicateProgressDataNodeManager;
import org.apache.iotdb.db.pipe.consensus.deletion.DeletionResource;
import org.apache.iotdb.db.pipe.consensus.deletion.DeletionResourceManager;
import org.apache.iotdb.db.pipe.event.common.deletion.PipeDeleteDataNodeEvent;
import org.apache.iotdb.db.pipe.event.common.terminate.PipeTerminateEvent;
import org.apache.iotdb.db.pipe.event.common.tsfile.PipeTsFileInsertionEvent;
import org.apache.iotdb.db.pipe.processor.iotconsensusv2.IoTConsensusV2Processor;
import org.apache.iotdb.db.pipe.resource.PipeDataNodeResourceManager;
import org.apache.iotdb.db.pipe.resource.tsfile.PipeTsFileResourceManager;
import org.apache.iotdb.db.pipe.source.dataregion.DataRegionListeningFilter;
import org.apache.iotdb.db.pipe.source.dataregion.realtime.assigner.PipeTsFileEpochProgressIndexKeeper;
import org.apache.iotdb.db.storageengine.StorageEngine;
import org.apache.iotdb.db.storageengine.dataregion.DataRegion;
import org.apache.iotdb.db.storageengine.dataregion.memtable.TsFileProcessor;
import org.apache.iotdb.db.storageengine.dataregion.read.reader.common.MergeReaderPriority;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileManager;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.pipe.api.customizer.configuration.PipeExtractorRuntimeConfiguration;
import org.apache.iotdb.pipe.api.customizer.configuration.PipeRuntimeEnvironment;
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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.EXTRACTOR_END_TIME_KEY;
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
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.EXTRACTOR_MODE_STRICT_DEFAULT_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.EXTRACTOR_MODE_STRICT_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.EXTRACTOR_MODS_DEFAULT_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.EXTRACTOR_MODS_ENABLE_DEFAULT_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.EXTRACTOR_MODS_ENABLE_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.EXTRACTOR_MODS_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.EXTRACTOR_START_TIME_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.SOURCE_END_TIME_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.SOURCE_HISTORY_ENABLE_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.SOURCE_HISTORY_END_TIME_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.SOURCE_HISTORY_LOOSE_RANGE_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.SOURCE_HISTORY_START_TIME_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.SOURCE_HISTORY_TSFILE_ORDER_BY_QUERY_PRIORITY_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.SOURCE_MODE_STRICT_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.SOURCE_MODS_ENABLE_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.SOURCE_MODS_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.SOURCE_START_TIME_KEY;
import static org.apache.iotdb.commons.pipe.source.IoTDBSource.getSkipIfNoPrivileges;
import static org.apache.tsfile.common.constant.TsFileConstant.PATH_ROOT;
import static org.apache.tsfile.common.constant.TsFileConstant.PATH_SEPARATOR;

public class PipeHistoricalDataRegionTsFileAndDeletionSource
    implements PipeHistoricalDataRegionSource {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(PipeHistoricalDataRegionTsFileAndDeletionSource.class);

  private static final String TREE_MODEL_EVENT_TABLE_NAME_PREFIX = PATH_ROOT + PATH_SEPARATOR;

  private String pipeName;
  private long creationTime;
  private String pipeNameWithCreationTime;
  private String tsFileDedupScopeID;

  private PipeTaskMeta pipeTaskMeta;
  private ProgressIndex startIndex;

  private int dataRegionId;

  private TreePattern treePattern;
  private TablePattern tablePattern;

  private boolean isModelDetected = false;
  private boolean isTableModel;
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
  private boolean shouldExtractDeletion;
  private boolean shouldTransferModFile; // Whether to transfer mods
  protected String userId;
  protected String userName;
  protected String cliHostname;
  protected boolean skipIfNoPrivileges = true;
  private boolean shouldTerminatePipeOnAllHistoricalEventsConsumed;
  private boolean isTerminateSignalSent = false;

  private boolean isForwardingPipeRequests;

  private volatile boolean hasBeenStarted = false;

  private Queue<PersistentResource> pendingQueue;
  private final Map<TsFileResource, Set<String>> filteredTsFileResources2TableNames =
      new HashMap<>();
  private final Map<PersistentResource, Long> pendingResource2ReplicateIndexForIoTV2 =
      new HashMap<>();
  private final Set<PersistentResource> historicalProgressReportResources = new HashSet<>();
  private ProgressIndex maxHistoricalProgressIndex = MinimumProgressIndex.INSTANCE;
  private ProgressIndex maxSuppliedHistoricalProgressReportIndex = MinimumProgressIndex.INSTANCE;
  private ProgressIndex pendingHistoricalProgressIndexToReport;
  private boolean shouldReportMaxHistoricalProgressIndex = false;
  private int extractedHistoricalTsFileCount = 0;
  private int extractedHistoricalDeletionCount = 0;

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
                  DataNodePipeMessages
                      .PIPE_EXCEPTION_PARAMETERS_IN_SET_S_ARE_NOT_ALLOWED_IN_HISTORY_LOOSE_RANGE_0F685D5C,
                  sloppyOptionSet));
        }
      }
    }

    if (parameters.hasAnyAttributes(
        SOURCE_START_TIME_KEY,
        EXTRACTOR_START_TIME_KEY,
        SOURCE_END_TIME_KEY,
        EXTRACTOR_END_TIME_KEY)) {
      isHistoricalSourceEnabled = true;

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
                  DataNodePipeMessages
                      .PIPE_EXCEPTION_S_S_S_SHOULD_BE_LESS_THAN_OR_EQUAL_TO_S_S_S_0B9726E1,
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
    isHistoricalSourceEnabled =
        parameters.getBooleanOrDefault(
                SystemConstant.RESTART_OR_NEWLY_ADDED_KEY,
                SystemConstant.RESTART_OR_NEWLY_ADDED_DEFAULT_VALUE)
            || parameters.getBooleanOrDefault(
                Arrays.asList(EXTRACTOR_HISTORY_ENABLE_KEY, SOURCE_HISTORY_ENABLE_KEY),
                EXTRACTOR_HISTORY_ENABLE_DEFAULT_VALUE);

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
                DataNodePipeMessages
                    .PIPE_EXCEPTION_S_S_S_SHOULD_BE_LESS_THAN_OR_EQUAL_TO_S_S_S_0B9726E1,
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
    shouldOrderHistoricalTsFileByQueryPriority =
        parameters.getBooleanOrDefault(
            Arrays.asList(
                EXTRACTOR_HISTORY_TSFILE_ORDER_BY_QUERY_PRIORITY_KEY,
                SOURCE_HISTORY_TSFILE_ORDER_BY_QUERY_PRIORITY_KEY),
            EXTRACTOR_HISTORY_TSFILE_ORDER_BY_QUERY_PRIORITY_DEFAULT_VALUE);

    final PipeRuntimeEnvironment environment = configuration.getRuntimeEnvironment();

    pipeName = environment.getPipeName();
    creationTime = environment.getCreationTime();
    pipeNameWithCreationTime =
        PipeTsFileResourceManager.getPipeTsFileResourcePipeName(pipeName, creationTime);
    if (environment instanceof PipeTaskSourceRuntimeEnvironment) {
      pipeTaskMeta = ((PipeTaskSourceRuntimeEnvironment) environment).getPipeTaskMeta();
      if (pipeName.startsWith(PipeStaticMeta.CONSENSUS_PIPE_PREFIX)) {
        startIndex = tryToExtractLocalProgressIndexForIoTV2(pipeTaskMeta.getProgressIndex());
      } else {
        startIndex = pipeTaskMeta.getProgressIndex();
      }
    }

    dataRegionId = environment.getRegionId();
    tsFileDedupScopeID =
        pipeName
            + "_"
            + dataRegionId
            + "_"
            + creationTime
            + "_"
            + Integer.toHexString(System.identityHashCode(environment));

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

    shouldTerminatePipeOnAllHistoricalEventsConsumed = PipeTaskAgent.isSnapshotMode(parameters);

    userId =
        parameters.getStringOrDefault(
            Arrays.asList(
                PipeSourceConstant.EXTRACTOR_IOTDB_USER_ID,
                PipeSourceConstant.SOURCE_IOTDB_USER_ID),
            "-1");
    userName =
        parameters.getStringByKeys(
            PipeSourceConstant.EXTRACTOR_IOTDB_USER_KEY,
            PipeSourceConstant.SOURCE_IOTDB_USER_KEY,
            PipeSourceConstant.EXTRACTOR_IOTDB_USERNAME_KEY,
            PipeSourceConstant.SOURCE_IOTDB_USERNAME_KEY);
    cliHostname =
        parameters.getStringByKeys(
            PipeSourceConstant.EXTRACTOR_IOTDB_CLI_HOSTNAME,
            PipeSourceConstant.SOURCE_IOTDB_CLI_HOSTNAME);

    skipIfNoPrivileges = getSkipIfNoPrivileges(parameters);

    final boolean isDoubleLiving = PipeSourceConstant.isDoubleLiving(parameters);
    if (isDoubleLiving) {
      isForwardingPipeRequests = false;
    } else {
      isForwardingPipeRequests =
          parameters.getBooleanOrDefault(
              PipeSourceConstant.FORWARDING_PIPE_REQUESTS_KEYS,
              PipeSourceConstant.EXTRACTOR_FORWARDING_PIPE_REQUESTS_DEFAULT_VALUE);
    }

    if (LOGGER.isInfoEnabled()) {
      LOGGER.info(
          DataNodePipeMessages.PIPE_HISTORICAL_DATA_EXTRACTION_TIME_RANGE_START,
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
          skipIfNoPrivileges,
          isForwardingPipeRequests);
    }
  }

  /**
   * IoTV2 will only resend event that contains un-replicated local write data. So we only extract
   * ProgressIndex containing local writes for comparison to prevent misjudgment on whether
   * high-level tsFiles with mixed progressIndexes need to be retransmitted
   *
   * @return recoverProgressIndex dedicated in local DataNodeId or origin for fallback.
   */
  private ProgressIndex tryToExtractLocalProgressIndexForIoTV2(final ProgressIndex origin) {
    return tryToExtractLocalProgressIndexForIoTV2(origin, true);
  }

  private ProgressIndex tryToExtractLocalProgressIndexForIoTV2(
      final ProgressIndex origin, final boolean shouldWarnUnexpectedType) {
    if (Objects.isNull(origin)) {
      return MinimumProgressIndex.INSTANCE;
    }

    if (origin instanceof StateProgressIndex) {
      final StateProgressIndex stateProgressIndex = (StateProgressIndex) origin;
      return new StateProgressIndex(
          stateProgressIndex.getVersion(),
          stateProgressIndex.getState(),
          tryToExtractLocalProgressIndexForIoTV2(
              stateProgressIndex.getInnerProgressIndex(), shouldWarnUnexpectedType));
    }

    if (origin instanceof RecoverProgressIndex) {
      return extractRecoverProgressIndex((RecoverProgressIndex) origin);
    }

    if (origin instanceof TimePartitionProgressIndex) {
      return new TimePartitionProgressIndex(
          ((TimePartitionProgressIndex) origin)
              .getTimePartitionId2ProgressIndex().entrySet().stream()
                  .collect(
                      Collectors.toMap(
                          Map.Entry::getKey,
                          entry ->
                              tryToExtractLocalProgressIndexForIoTV2(entry.getValue(), false))));
    }

    if (origin instanceof HybridProgressIndex) {
      final Map<Short, ProgressIndex> type2Index = ((HybridProgressIndex) origin).getType2Index();
      ProgressIndex result = null;
      if (type2Index.containsKey(ProgressIndexType.RECOVER_PROGRESS_INDEX.getType())) {
        result =
            extractRecoverProgressIndex(
                (RecoverProgressIndex)
                    type2Index.get(ProgressIndexType.RECOVER_PROGRESS_INDEX.getType()));
      }
      if (type2Index.containsKey(ProgressIndexType.TIME_PARTITION_PROGRESS_INDEX.getType())) {
        final ProgressIndex timePartitionProgressIndex =
            tryToExtractLocalProgressIndexForIoTV2(
                type2Index.get(ProgressIndexType.TIME_PARTITION_PROGRESS_INDEX.getType()), false);
        result =
            Objects.isNull(result)
                ? timePartitionProgressIndex
                : result.updateToMinimumEqualOrIsAfterProgressIndex(timePartitionProgressIndex);
      }
      if (Objects.nonNull(result)) {
        return result;
      }
    }

    if (shouldWarnUnexpectedType) {
      LOGGER.warn(
          DataNodePipeMessages.PIPE_UNEXPECTED_PROGRESSINDEX_TYPE_FALLBACK_TO_ORIGIN,
          pipeName,
          dataRegionId,
          origin.getType(),
          origin);
    }
    return origin;
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
    if (!shouldExtractInsertion && !shouldExtractDeletion) {
      hasBeenStarted = true;
      return;
    }
    if (!StorageEngine.getInstance().isReadyForNonReadWriteFunctions()) {
      LOGGER.info(
          DataNodePipeMessages.PIPE_FAILED_TO_START_TO_EXTRACT_HISTORICAL, pipeName, dataRegionId);
      return;
    }
    hasBeenStarted = true;
    extractedHistoricalTsFileCount = 0;
    extractedHistoricalDeletionCount = 0;
    maxHistoricalProgressIndex = MinimumProgressIndex.INSTANCE;
    maxSuppliedHistoricalProgressReportIndex = MinimumProgressIndex.INSTANCE;
    pendingHistoricalProgressIndexToReport = null;
    shouldReportMaxHistoricalProgressIndex = false;
    historicalProgressReportResources.clear();

    final DataRegion dataRegion =
        StorageEngine.getInstance().getDataRegion(new DataRegionId(dataRegionId));
    if (Objects.isNull(dataRegion)) {
      pendingQueue = new ArrayDeque<>();
      return;
    }

    final long startHistoricalExtractionTime = System.currentTimeMillis();
    dataRegion.writeLock(
        "Pipe: start to extract historical TsFile and Deletion(if uses iotConsensusV2)");
    try {
      List<PersistentResource> originalResourceList = new ArrayList<>();

      if (shouldExtractInsertion) {
        flushTsFilesForExtraction(dataRegion);
        extractTsFiles(dataRegion, startHistoricalExtractionTime, originalResourceList);
      }
      if (shouldExtractDeletion) {
        Optional.ofNullable(DeletionResourceManager.getInstance(dataRegionId))
            .ifPresent(manager -> extractDeletions(manager, originalResourceList));
      }

      // Sort tsFileResource and deletionResource
      long startTime = System.currentTimeMillis();
      LOGGER.info(
          DataNodePipeMessages.PIPE_START_TO_SORT_ALL_EXTRACTED_RESOURCES, pipeName, dataRegionId);
      if (shouldUseHistoricalTsFileQueryPriorityOrder()) {
        prepareResourcesForHistoricalTsFileQueryPriorityOrder(originalResourceList);
      }
      sortExtractedResources(originalResourceList);
      if (shouldUseHistoricalTsFileQueryPriorityOrder()) {
        prepareProgressReportResourcesForHistoricalTsFileQueryPriorityOrder(originalResourceList);
      }
      pendingQueue = new ArrayDeque<>(originalResourceList);
      PipeTerminateEvent.initializeHistoricalTransferSummary(
          pipeName,
          creationTime,
          dataRegionId,
          extractedHistoricalTsFileCount,
          extractedHistoricalDeletionCount);

      LOGGER.info(
          DataNodePipeMessages.PIPE_FINISH_TO_SORT_ALL_EXTRACTED_RESOURCES,
          pipeName,
          dataRegionId,
          System.currentTimeMillis() - startTime);
    } finally {
      dataRegion.writeUnlock();
    }
  }

  private boolean shouldUseHistoricalTsFileQueryPriorityOrder() {
    // Deletion resources only carry progressIndex. Keep the old progressIndex order when deletions
    // are extracted together with TsFiles so insertion/deletion ordering semantics are unchanged.
    return shouldOrderHistoricalTsFileByQueryPriority
        && shouldExtractInsertion
        && !shouldExtractDeletion;
  }

  private void prepareResourcesForHistoricalTsFileQueryPriorityOrder(
      final List<PersistentResource> resourceList) {
    // Query-priority order is intentionally not compatible with progressIndex order, so only
    // selected historical TsFiles should participate in query-order progress reports.
    resourceList.removeIf(
        resource ->
            resource instanceof TsFileResource
                && !filteredTsFileResources2TableNames.containsKey(resource));
    updateMaxHistoricalProgressIndex(resourceList);
    shouldReportMaxHistoricalProgressIndex = !resourceList.isEmpty();
  }

  private void prepareProgressReportResourcesForHistoricalTsFileQueryPriorityOrder(
      final List<PersistentResource> resourceList) {
    historicalProgressReportResources.clear();
    final Map<Long, List<ProgressIndex>> timePartitionId2RemainingMinimalProgressIndexes =
        new HashMap<>();
    for (int i = resourceList.size() - 1; i >= 0; --i) {
      final PersistentResource resource = resourceList.get(i);
      if (!(resource instanceof TsFileResource)) {
        continue;
      }

      final ProgressIndex progressIndex = resource.getProgressIndex();
      if (Objects.isNull(progressIndex)) {
        continue;
      }

      final List<ProgressIndex> remainingMinimalProgressIndexes =
          timePartitionId2RemainingMinimalProgressIndexes.computeIfAbsent(
              ((TsFileResource) resource).getTimePartition(), ignored -> new ArrayList<>());
      // A query-priority report is persisted as a time-partition-scoped progress index. Recovery
      // only uses it to cover TsFiles from the same partition, so it does not rely on any global
      // ordering guarantee between partitions. A numerically larger progress in partition A cannot
      // skip an untransferred resource in partition B.
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

  private void updateMaxHistoricalProgressIndex(final List<PersistentResource> resourceList) {
    for (final PersistentResource resource : resourceList) {
      final ProgressIndex progressIndex = resource.getProgressIndex();
      if (Objects.nonNull(progressIndex)) {
        maxHistoricalProgressIndex =
            maxHistoricalProgressIndex.updateToMinimumEqualOrIsAfterProgressIndex(progressIndex);
      }
    }
  }

  private void sortExtractedResources(final List<PersistentResource> resourceList) {
    if (shouldUseHistoricalTsFileQueryPriorityOrder()) {
      // Send TsFiles from lower query/compaction priority to higher priority. For duplicated
      // points, covered files are loaded first on the receiver and covering files are loaded later
      // to preserve overwrite semantics.
      resourceList.sort(
          (o1, o2) ->
              o1 instanceof TsFileResource && o2 instanceof TsFileResource
                  ? compareTsFileResourcesByQueryPriority((TsFileResource) o1, (TsFileResource) o2)
                  : comparePersistentResourcesByProgressIndex(o1, o2));
      return;
    }

    resourceList.sort(
        (o1, o2) ->
            startIndex instanceof TimeWindowStateProgressIndex
                ? Long.compare(o1.getFileStartTime(), o2.getFileStartTime())
                : comparePersistentResourcesByProgressIndex(o1, o2));
  }

  private int comparePersistentResourcesByProgressIndex(
      final PersistentResource resource1, final PersistentResource resource2) {
    return resource1.getProgressIndex().topologicalCompareTo(resource2.getProgressIndex());
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

  private void flushTsFilesForExtraction(DataRegion dataRegion) {
    LOGGER.info(DataNodePipeMessages.PIPE_START_TO_FLUSH_DATA_REGION, pipeName, dataRegionId);

    // Consider the scenario: a consensus pipe comes to the same region, followed by another pipe
    // **immediately**, the latter pipe will skip the flush operation.
    // Since a large number of consensus pipes are not created at the same time, resulting in no
    // serious waiting for locks. Therefore, the flush operation is always performed for the
    // consensus pipe, and the lastFlushed timestamp is not updated here.
    if (pipeName.startsWith(PipeStaticMeta.CONSENSUS_PIPE_PREFIX)
        || shouldUseHistoricalTsFileQueryPriorityOrder()) {
      dataRegion.syncCloseAllWorkingTsFileProcessors();
    } else {
      dataRegion.asyncCloseAllWorkingTsFileProcessors();
    }
  }

  private void extractTsFiles(
      final DataRegion dataRegion,
      final long startHistoricalExtractionTime,
      final List<PersistentResource> originalResourceList) {
    final TsFileManager tsFileManager = dataRegion.getTsFileManager();
    tsFileManager.readLock();
    try {
      final int originalSequenceTsFileCount = tsFileManager.size(true);
      final int originalUnSequenceTsFileCount = tsFileManager.size(false);
      LOGGER.info(
          DataNodePipeMessages.PIPE_START_TO_EXTRACT_HISTORICAL_TSFILE_ORIGINAL,
          pipeName,
          dataRegionId,
          originalSequenceTsFileCount,
          originalUnSequenceTsFileCount,
          startIndex);

      final HistoricalTsFileExtractionStatistics statistics =
          new HistoricalTsFileExtractionStatistics();
      final Map<TsFileResource, Set<String>> sequenceTsFileResources2TableNames =
          tsFileManager.getTsFileList(true).stream()
              .peek(originalResourceList::add)
              .filter(resource -> shouldExtractTsFileResource(resource, statistics))
              .collect(
                  Collectors.toMap(
                      Function.identity(),
                      resource ->
                          isModelDetected && isTableModel
                              ? resource.getDevices().stream()
                                  .map(IDeviceID::getTableName)
                                  .collect(Collectors.toSet())
                              : Collections.emptySet()));
      filteredTsFileResources2TableNames.putAll(sequenceTsFileResources2TableNames);

      final Map<TsFileResource, Set<String>> unSequenceTsFileResources2TableNames =
          tsFileManager.getTsFileList(false).stream()
              .peek(originalResourceList::add)
              .filter(resource -> shouldExtractTsFileResource(resource, statistics))
              .collect(
                  Collectors.toMap(
                      Function.identity(),
                      resource ->
                          isModelDetected && isTableModel
                              ? resource.getDevices().stream()
                                  .map(IDeviceID::getTableName)
                                  .collect(Collectors.toSet())
                              : Collections.emptySet()));
      filteredTsFileResources2TableNames.putAll(unSequenceTsFileResources2TableNames);

      filteredTsFileResources2TableNames
          .keySet()
          .removeIf(
              resource -> {
                // Pin the resource, in case the file is removed by compaction or anything.
                // Will unpin it after the PipeTsFileInsertionEvent is created and pinned.
                try {
                  PipeDataNodeResourceManager.tsfile()
                      .pinTsFileResource(resource, shouldTransferModFile, pipeNameWithCreationTime);
                  return false;
                } catch (final IOException e) {
                  ++statistics.pinFailedCount;
                  LOGGER.warn(
                      DataNodePipeMessages.PIPE_FAILED_TO_PIN_TSFILERESOURCE,
                      resource.getTsFilePath(),
                      e);
                  return true;
                }
              });
      extractedHistoricalTsFileCount = filteredTsFileResources2TableNames.size();

      LOGGER.info(
          DataNodePipeMessages.PIPE_FINISH_TO_EXTRACT_HISTORICAL_TSFILE_EXTRACTED,
          pipeName,
          dataRegionId,
          sequenceTsFileResources2TableNames.size(),
          originalSequenceTsFileCount,
          unSequenceTsFileResources2TableNames.size(),
          originalUnSequenceTsFileCount,
          filteredTsFileResources2TableNames.size(),
          originalSequenceTsFileCount + originalUnSequenceTsFileCount,
          System.currentTimeMillis() - startHistoricalExtractionTime);
      LOGGER.info(
          DataNodePipeMessages
              .MESSAGE_PIPE_ARG_ARG_HISTORICAL_TSFILE_SELECTION_SUMMARY_SELECTED_BY_PROGRESS_UNCOVERED_ARG_7B74E18D,
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
    } finally {
      tsFileManager.readUnlock();
    }
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

    // Some resource may not be closed due to the control of PIPE_MIN_FLUSH_INTERVAL_IN_MS. We
    // simply ignore them.
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
    if (innerStartIndex instanceof TimeWindowStateProgressIndex) {
      // The resource is closed thus the TsFileResource#getFileEndTime() is safe to use
      return ((TimeWindowStateProgressIndex) innerStartIndex).getMinTime()
          <= resource.getFileEndTime();
    }

    if (pipeName.startsWith(PipeStaticMeta.CONSENSUS_PIPE_PREFIX)) {
      // For consensus pipe, we only focus on the progressIndex that is generated from local write
      // instead of replication or something else.
      ProgressIndex dedicatedProgressIndex =
          tryToExtractLocalProgressIndexForIoTV2(resource.getMaxProgressIndexAfterClose());
      return isProgressIndexNotCoveredByStartIndex(resource, dedicatedProgressIndex);
    }
    return isProgressIndexNotCoveredByStartIndex(
        resource, resource.getMaxProgressIndexAfterClose());
  }

  private boolean isProgressIndexNotCoveredByStartIndex(
      PersistentResource resource, ProgressIndex progressIndex) {
    final ProgressIndex innerStartIndex = getInnerProgressIndex(startIndex);
    if (innerStartIndex.isEqualOrAfter(progressIndex)
        || isProgressIndexCoveredByTimePartitionProgressIndex(
            resource, progressIndex, innerStartIndex)) {
      return false;
    }

    LOGGER.info(
        DataNodePipeMessages.PIPE_RESOURCE_MEETS_MAYTSFILECONTAINUNPROCESSEDDATA_CONDITION_EXTRACT,
        pipeName,
        dataRegionId,
        resource,
        innerStartIndex,
        progressIndex);
    return true;
  }

  private ProgressIndex getInnerProgressIndex(final ProgressIndex progressIndex) {
    return progressIndex instanceof StateProgressIndex
        ? ((StateProgressIndex) progressIndex).getInnerProgressIndex()
        : Objects.isNull(progressIndex) ? MinimumProgressIndex.INSTANCE : progressIndex;
  }

  private boolean isProgressIndexCoveredByTimePartitionProgressIndex(
      final PersistentResource resource,
      final ProgressIndex progressIndex,
      final ProgressIndex startIndex) {
    if (!(resource instanceof TsFileResource)) {
      return false;
    }

    final TimePartitionProgressIndex timePartitionProgressIndex =
        getTimePartitionProgressIndex(startIndex);
    // Keep this check strictly partition-local, matching the reporting side. This is what makes
    // query-priority historical transfer restart-safe even when different partitions are sent in an
    // order that conflicts with the global ProgressIndex order.
    return Objects.nonNull(timePartitionProgressIndex)
        && timePartitionProgressIndex.isProgressIndexEqualOrAfter(
            ((TsFileResource) resource).getTimePartition(), progressIndex);
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

  private static class HistoricalTsFileExtractionStatistics {

    private int selectedByProgressUncoveredCount;
    private int selectedByUnclosedOrClosingCount;
    private int filteredByTimeOrPathCount;
    private int filteredByTimeCount;
    private int filteredByPathCount;
    private int skippedCoveredCount;
    private int skippedDeletedCount;
    private int skippedGeneratedByPipeCount;
    private int pinFailedCount;
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
    } catch (final IOException e) {
      LOGGER.warn(
          DataNodePipeMessages.PIPE_FAILED_TO_GET_DEVICES_FROM_TSFILE_1,
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

  private void extractDeletions(
      final DeletionResourceManager deletionResourceManager,
      final List<PersistentResource> resourceList) {
    LOGGER.info(DataNodePipeMessages.PIPE_START_TO_EXTRACT_DELETIONS, pipeName, dataRegionId);
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
              return !isProgressIndexNotCoveredByStartIndex(resource, toBeCompared);
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
                  return isProgressIndexNotCoveredByStartIndex(resource, toBeCompared);
                })
            .collect(Collectors.toList());
    resourceList.addAll(allDeletionResources);
    extractedHistoricalDeletionCount = allDeletionResources.size();
    LOGGER.info(
        DataNodePipeMessages.PIPE_FINISH_TO_EXTRACT_DELETIONS_EXTRACT_DELETIONS,
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

    if (Objects.nonNull(pendingHistoricalProgressIndexToReport)) {
      final ProgressIndex progressIndex = pendingHistoricalProgressIndexToReport;
      pendingHistoricalProgressIndexToReport = null;
      return supplyHistoricalProgressReportEvent(progressIndex);
    }

    if (Objects.isNull(pendingQueue)) {
      return null;
    }

    while (true) {
      final PersistentResource resource = pendingQueue.peek();
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

      if (resource instanceof TsFileResource) {
        final TsFileResource tsFileResource = (TsFileResource) resource;
        if (consumeSkippedHistoricalTsFileEventIfNecessary(tsFileResource)) {
          clearReplicateIndexForResource(tsFileResource);
          pendingQueue.poll();
          if (shouldUseHistoricalTsFileQueryPriorityOrder()) {
            if (shouldReportHistoricalProgressAfterResource(tsFileResource)) {
              return supplyHistoricalProgressReportEvent(
                  getHistoricalProgressIndexAfterResource(tsFileResource));
            }
            continue;
          }
          return supplyProgressReportEvent(tsFileResource.getMaxProgressIndex());
        }

        final Event event = supplyTsFileEvent(tsFileResource);
        pendingQueue.poll();
        if (Objects.nonNull(event) && shouldReportHistoricalProgressAfterResource(tsFileResource)) {
          pendingHistoricalProgressIndexToReport =
              getHistoricalProgressIndexAfterResource(tsFileResource);
        }
        return event;
      }

      final Event event = supplyDeletionEvent((DeletionResource) resource);
      pendingQueue.poll();
      return event;
    }
  }

  private boolean shouldReportHistoricalProgressAfterResource(final PersistentResource resource) {
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
          DataNodePipeMessages
              .PIPE_LOG_PIPE_HISTORICAL_SOURCE_HAS_SUPPLIED_ALL_EVENTS_EMITTING_8B58DE19,
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
        PipeHistoricalDataRegionTsFileAndDeletionSource.class.getName())) {
      LOGGER.warn(
          DataNodePipeMessages.PIPE_FAILED_TO_INCREASE_REFERENCE_COUNT_FOR_2,
          pipeName,
          dataRegionId);
      return null;
    }
    isTerminateSignalSent = true;
    return terminateEvent;
  }

  protected boolean consumeSkippedHistoricalTsFileEventIfNecessary(final TsFileResource resource) {
    if (!filteredTsFileResources2TableNames.containsKey(resource)
        || !shouldSkipHistoricalTsFileEvent(resource)) {
      return false;
    }

    filteredTsFileResources2TableNames.remove(resource);
    PipeTerminateEvent.markHistoricalTsFileSkipped(pipeName, creationTime, dataRegionId);
    LOGGER.info(
        DataNodePipeMessages.PIPE_SKIP_HISTORICAL_TSFILE_BECAUSE_REALTIME_SOURCE,
        pipeName,
        dataRegionId,
        resource.getTsFilePath(),
        tsFileDedupScopeID);
    try {
      return true;
    } finally {
      try {
        PipeDataNodeResourceManager.tsfile()
            .unpinTsFileResource(resource, shouldTransferModFile, pipeNameWithCreationTime);
      } catch (final IOException e) {
        LOGGER.warn(
            DataNodePipeMessages.PIPE_FAILED_TO_UNPIN_SKIPPED_HISTORICAL_TSFILERESOURCE,
            pipeName,
            dataRegionId,
            resource.getTsFilePath(),
            e);
      }
    }
  }

  protected Event supplyProgressReportEvent(final ProgressIndex progressIndex) {
    final ProgressReportEvent progressReportEvent =
        new ProgressReportEvent(pipeName, creationTime, pipeTaskMeta);
    progressReportEvent.bindProgressIndex(progressIndex);
    final boolean isReferenceCountIncreased =
        progressReportEvent.increaseReferenceCount(
            PipeHistoricalDataRegionTsFileAndDeletionSource.class.getName());
    if (!isReferenceCountIncreased) {
      LOGGER.warn(
          DataNodePipeMessages.THE_REFERENCE_COUNT_OF_THE_EVENT_CANNOT, progressReportEvent);
    }
    return isReferenceCountIncreased ? progressReportEvent : null;
  }

  private Event supplyHistoricalProgressReportEvent(final ProgressIndex progressIndex) {
    maxSuppliedHistoricalProgressReportIndex =
        maxSuppliedHistoricalProgressReportIndex.updateToMinimumEqualOrIsAfterProgressIndex(
            progressIndex);
    return supplyProgressReportEvent(progressIndex);
  }

  protected Event supplyTsFileEvent(final TsFileResource resource) {
    if (!filteredTsFileResources2TableNames.containsKey(resource)) {
      clearReplicateIndexForResource(resource);
      return shouldUseHistoricalTsFileQueryPriorityOrder()
          ? null
          : supplyProgressReportEvent(resource.getMaxProgressIndex());
    }

    boolean shouldUnpinResource = false;
    boolean shouldClearReplicateIndex = false;
    try {
      final PipeTsFileInsertionEvent event =
          new PipeTsFileInsertionEvent(
              isModelDetected ? isTableModel : null,
              resource.getDatabaseName(),
              resource,
              null,
              shouldTransferModFile,
              false,
              true,
              filteredTsFileResources2TableNames.get(resource),
              pipeName,
              creationTime,
              pipeTaskMeta,
              treePattern,
              tablePattern,
              userId,
              userName,
              cliHostname,
              skipIfNoPrivileges,
              historicalDataExtractionStartTime,
              historicalDataExtractionEndTime);

      if (shouldUseHistoricalTsFileQueryPriorityOrder()) {
        event.skipReportOnCommitAndGeneratedEvents();
      }

      // if using IoTV2, assign a replicateIndex for this event
      if (shouldAssignReplicateIndexForIoTV2(event)) {
        event.setReplicateIndexForIoTV2(assignReplicateIndexForResource(resource));
        LOGGER.debug(
            DataNodePipeMessages.SET_FOR_HISTORICAL_EVENT,
            pipeName,
            event.getReplicateIndexForIoTV2(),
            event);
      }

      if (sloppyPattern
          || isDbNameCoveredByPattern
          || isTsFileResourceCoveredByTablePattern(
              resource, filteredTsFileResources2TableNames.get(resource))) {
        event.skipParsingPattern();
      }
      if (sloppyTimeRange || isTsFileResourceCoveredByTimeRange(resource)) {
        event.skipParsingTime();
      }

      final boolean isReferenceCountIncreased =
          event.increaseReferenceCount(
              PipeHistoricalDataRegionTsFileAndDeletionSource.class.getName());
      if (!isReferenceCountIncreased) {
        LOGGER.warn(
            DataNodePipeMessages.PIPE_FAILED_TO_INCREASE_REFERENCE_COUNT_FOR_1,
            pipeName,
            dataRegionId,
            event);
      }
      filteredTsFileResources2TableNames.remove(resource);
      shouldUnpinResource = true;
      shouldClearReplicateIndex = true;
      return isReferenceCountIncreased ? event : null;
    } finally {
      if (shouldClearReplicateIndex) {
        clearReplicateIndexForResource(resource);
      }
      if (shouldUnpinResource) {
        try {
          PipeDataNodeResourceManager.tsfile()
              .unpinTsFileResource(resource, shouldTransferModFile, pipeNameWithCreationTime);
        } catch (final IOException e) {
          LOGGER.warn(
              DataNodePipeMessages.PIPE_FAILED_TO_UNPIN_TSFILERESOURCE_AFTER_CREATING,
              pipeName,
              dataRegionId,
              resource.getTsFilePath());
        }
      }
    }
  }

  private boolean shouldSkipHistoricalTsFileEvent(final TsFileResource resource) {
    return pipeName.startsWith(PipeStaticMeta.CONSENSUS_PIPE_PREFIX)
        && DataRegionConsensusImpl.getInstance() instanceof IoTConsensusV2
        && PipeTsFileEpochProgressIndexKeeper.getInstance()
            .containsTsFile(dataRegionId, tsFileDedupScopeID, resource.getTsFilePath());
  }

  private boolean isTsFileResourceCoveredByTablePattern(
      final TsFileResource resource, final Set<String> tableNames) {
    return isModelDetected
        && isTableModel
        && tablePattern.isTableModelDataAllowedToBeCaptured()
        && Objects.nonNull(resource)
        && Objects.nonNull(tableNames)
        && !tableNames.isEmpty()
        && tableNames.stream()
            .allMatch(
                tableName ->
                    tablePattern.matchesDatabase(resource.getDatabaseName())
                        && tablePattern.matchesTable(tableName));
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
            userId,
            userName,
            cliHostname,
            skipIfNoPrivileges,
            false);
    // if using IoTV2, assign a replicateIndex for this historical deletion event
    if (shouldAssignReplicateIndexForIoTV2(event)) {
      event.setReplicateIndexForIoTV2(assignReplicateIndexForResource(deletionResource));
      LOGGER.debug(
          DataNodePipeMessages.SET_FOR_HISTORICAL_DELETION_EVENT,
          pipeName,
          event.getReplicateIndexForIoTV2(),
          event);
    }

    if (sloppyPattern || isDbNameCoveredByPattern) {
      event.skipParsingPattern();
    }
    if (sloppyTimeRange) {
      event.skipParsingTime();
    }

    final boolean isReferenceCountIncreased =
        event.increaseReferenceCount(
            PipeHistoricalDataRegionTsFileAndDeletionSource.class.getName());
    if (!isReferenceCountIncreased) {
      LOGGER.warn(
          DataNodePipeMessages.PIPE_FAILED_TO_INCREASE_REFERENCE_COUNT_FOR,
          pipeName,
          dataRegionId,
          event);
    } else {
      Optional.ofNullable(DeletionResourceManager.getInstance(dataRegionId))
          .ifPresent(
              manager ->
                  event.setDeletionResource(
                      manager.getDeletionResource(event.getDeleteDataNode())));
    }
    clearReplicateIndexForResource(deletionResource);
    return isReferenceCountIncreased ? event : null;
  }

  protected boolean shouldAssignReplicateIndexForIoTV2(final EnrichedEvent event) {
    return DataRegionConsensusImpl.getInstance() instanceof IoTConsensusV2
        && IoTConsensusV2Processor.isShouldReplicate(event);
  }

  protected long assignReplicateIndexForResource(final PersistentResource resource) {
    return pendingResource2ReplicateIndexForIoTV2.computeIfAbsent(
        resource,
        ignored -> ReplicateProgressDataNodeManager.assignReplicateIndexForIoTV2(pipeName));
  }

  protected void clearReplicateIndexForResource(final PersistentResource resource) {
    pendingResource2ReplicateIndexForIoTV2.remove(resource);
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
    if (Objects.nonNull(pendingQueue)) {
      pendingQueue.forEach(
          resource -> {
            if (resource instanceof TsFileResource) {
              try {
                PipeDataNodeResourceManager.tsfile()
                    .unpinTsFileResource(
                        (TsFileResource) resource, shouldTransferModFile, pipeNameWithCreationTime);
              } catch (final IOException e) {
                LOGGER.warn(
                    DataNodePipeMessages.PIPE_FAILED_TO_UNPIN_TSFILERESOURCE_AFTER_DROPPING,
                    pipeName,
                    dataRegionId,
                    ((TsFileResource) resource).getTsFilePath());
              }
            }
          });
      pendingQueue.clear();
      pendingQueue = null;
    }
    pendingResource2ReplicateIndexForIoTV2.clear();
    historicalProgressReportResources.clear();
    pendingHistoricalProgressIndexToReport = null;
  }
}
