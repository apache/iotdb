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
import org.apache.iotdb.commons.consensus.index.impl.StateProgressIndex;
import org.apache.iotdb.commons.consensus.index.impl.TimeWindowStateProgressIndex;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.pipe.agent.task.PipeTaskAgent;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeStaticMeta;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeTaskMeta;
import org.apache.iotdb.commons.pipe.config.constant.SystemConstant;
import org.apache.iotdb.commons.pipe.config.plugin.env.PipeTaskSourceRuntimeEnvironment;
import org.apache.iotdb.commons.pipe.datastructure.pattern.PipePattern;
import org.apache.iotdb.commons.pipe.event.ProgressReportEvent;
import org.apache.iotdb.db.pipe.event.common.terminate.PipeTerminateEvent;
import org.apache.iotdb.db.pipe.event.common.tsfile.PipeTsFileInsertionEvent;
import org.apache.iotdb.db.pipe.resource.PipeDataNodeResourceManager;
import org.apache.iotdb.db.pipe.source.dataregion.DataRegionListeningFilter;
import org.apache.iotdb.db.storageengine.StorageEngine;
import org.apache.iotdb.db.storageengine.dataregion.DataRegion;
import org.apache.iotdb.db.storageengine.dataregion.memtable.TsFileProcessor;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileManager;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
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
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.EXTRACTOR_MODS_ENABLE_DEFAULT_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.EXTRACTOR_MODS_ENABLE_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.EXTRACTOR_START_TIME_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.SOURCE_END_TIME_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.SOURCE_FORWARDING_PIPE_REQUESTS_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.SOURCE_HISTORY_ENABLE_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.SOURCE_HISTORY_END_TIME_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.SOURCE_HISTORY_LOOSE_RANGE_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.SOURCE_HISTORY_START_TIME_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.SOURCE_MODS_ENABLE_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.SOURCE_START_TIME_KEY;

public class PipeHistoricalDataRegionTsFileSource implements PipeHistoricalDataRegionSource {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(PipeHistoricalDataRegionTsFileSource.class);

  private String pipeName;
  private long creationTime;

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

  private Pair<Boolean, Boolean> listeningOptionPair;
  private boolean shouldExtractInsertion;
  private boolean shouldTransferModFile; // Whether to transfer mods

  private boolean shouldTerminatePipeOnAllHistoricalEventsConsumed;
  private boolean isTerminateSignalSent = false;

  private boolean isForwardingPipeRequests;

  private volatile boolean hasBeenStarted = false;

  private Queue<TsFileResource> pendingQueue;
  private final Set<TsFileResource> filteredTsFileResources = new HashSet<>();

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
    isHistoricalSourceEnabled =
        parameters.getBooleanOrDefault(
                SystemConstant.RESTART_OR_NEWLY_ADDED_KEY, SystemConstant.RESTART_DEFAULT_VALUE)
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
    // Do nothing if only extract deletion
    if (!shouldExtractInsertion) {
      return;
    }

    final PipeTaskSourceRuntimeEnvironment environment =
        (PipeTaskSourceRuntimeEnvironment) configuration.getRuntimeEnvironment();

    pipeName = environment.getPipeName();
    creationTime = environment.getCreationTime();
    pipeTaskMeta = environment.getPipeTaskMeta();
    startIndex = environment.getPipeTaskMeta().getProgressIndex();

    dataRegionId = environment.getRegionId();
    pipePattern = PipePattern.parsePipePatternFromSourceParameters(parameters);

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

    final DataRegion dataRegion =
        StorageEngine.getInstance().getDataRegion(new DataRegionId(dataRegionId));
    if (Objects.isNull(dataRegion)) {
      pendingQueue = new ArrayDeque<>();
      return;
    }

    dataRegion.writeLock("Pipe: start to extract historical TsFile");
    final long startHistoricalExtractionTime = System.currentTimeMillis();
    try {
      LOGGER.info("Pipe {}@{}: start to flush data region", pipeName, dataRegionId);

      // Consider the scenario: a consensus pipe comes to the same region, followed by another pipe
      // **immediately**, the latter pipe will skip the flush operation.
      // Since a large number of consensus pipes are not created at the same time, resulting in no
      // serious waiting for locks. Therefore, the flush operation is always performed for the
      // consensus pipe, and the lastFlushed timestamp is not updated here.
      if (pipeName.startsWith(PipeStaticMeta.CONSENSUS_PIPE_PREFIX)) {
        dataRegion.syncCloseAllWorkingTsFileProcessors();
      } else {
        dataRegion.asyncCloseAllWorkingTsFileProcessors();
      }
      LOGGER.info(
          "Pipe {}@{}: finish to flush data region, took {} ms",
          pipeName,
          dataRegionId,
          System.currentTimeMillis() - startHistoricalExtractionTime);

      final TsFileManager tsFileManager = dataRegion.getTsFileManager();
      tsFileManager.readLock();
      try {
        final int originalSequenceTsFileCount = tsFileManager.size(true);
        final int originalUnSequenceTsFileCount = tsFileManager.size(false);
        final List<TsFileResource> originalResourceList =
            new ArrayList<>(originalSequenceTsFileCount + originalUnSequenceTsFileCount);
        LOGGER.info(
            "Pipe {}@{}: start to extract historical TsFile, original sequence file count {}, "
                + "original unSequence file count {}, start progress index {}",
            pipeName,
            dataRegionId,
            originalSequenceTsFileCount,
            originalUnSequenceTsFileCount,
            startIndex);

        final Collection<TsFileResource> sequenceTsFileResources =
            tsFileManager.getTsFileList(true).stream()
                .peek(originalResourceList::add)
                .filter(
                    resource ->
                        isHistoricalSourceEnabled
                            &&
                            // Some resource is marked as deleted but not removed from the list.
                            !resource.isDeleted()
                            // Some resource is generated by pipe. We ignore them if the pipe should
                            // not transfer pipe requests.
                            && (!resource.isGeneratedByPipe() || isForwardingPipeRequests)
                            && (
                            // If the tsFile is not already marked closing, it is not captured by
                            // the pipe realtime module. Thus, we can wait for the realtime sync
                            // module to handle this, to avoid blocking the pipe sync process.
                            !resource.isClosed()
                                    && Optional.ofNullable(resource.getProcessor())
                                        .map(TsFileProcessor::alreadyMarkedClosing)
                                        .orElse(true)
                                || mayTsFileContainUnprocessedData(resource)
                                    && isTsFileResourceOverlappedWithTimeRange(resource)
                                    && mayTsFileResourceOverlappedWithPattern(resource)))
                .collect(Collectors.toList());
        filteredTsFileResources.addAll(sequenceTsFileResources);

        final Collection<TsFileResource> unSequenceTsFileResources =
            tsFileManager.getTsFileList(false).stream()
                .peek(originalResourceList::add)
                .filter(
                    resource ->
                        isHistoricalSourceEnabled
                            &&
                            // Some resource is marked as deleted but not removed from the list.
                            !resource.isDeleted()
                            // Some resource is generated by pipe. We ignore them if the pipe should
                            // not transfer pipe requests.
                            && (!resource.isGeneratedByPipe() || isForwardingPipeRequests)
                            && (
                            // If the tsFile is not already marked closing, it is not captured by
                            // the pipe realtime module. Thus, we can wait for the realtime sync
                            // module to handle this, to avoid blocking the pipe sync process.
                            !resource.isClosed()
                                    && Optional.ofNullable(resource.getProcessor())
                                        .map(TsFileProcessor::alreadyMarkedClosing)
                                        .orElse(true)
                                || mayTsFileContainUnprocessedData(resource)
                                    && isTsFileResourceOverlappedWithTimeRange(resource)
                                    && mayTsFileResourceOverlappedWithPattern(resource)))
                .collect(Collectors.toList());
        filteredTsFileResources.addAll(unSequenceTsFileResources);

        filteredTsFileResources.removeIf(
            resource -> {
              // Pin the resource, in case the file is removed by compaction or anything.
              // Will unpin it after the PipeTsFileInsertionEvent is created and pinned.
              try {
                PipeDataNodeResourceManager.tsfile()
                    .pinTsFileResource(resource, shouldTransferModFile, pipeName);
                return false;
              } catch (final IOException e) {
                LOGGER.warn("Pipe: failed to pin TsFileResource {}", resource.getTsFilePath(), e);
                return true;
              }
            });

        originalResourceList.sort(
            (o1, o2) ->
                startIndex instanceof TimeWindowStateProgressIndex
                    ? Long.compare(o1.getFileStartTime(), o2.getFileStartTime())
                    : o1.getMaxProgressIndex().topologicalCompareTo(o2.getMaxProgressIndex()));
        pendingQueue = new ArrayDeque<>(originalResourceList);

        LOGGER.info(
            "Pipe {}@{}: finish to extract historical TsFile, extracted sequence file count {}/{}, "
                + "extracted unsequence file count {}/{}, extracted file count {}/{}, took {} ms",
            pipeName,
            dataRegionId,
            sequenceTsFileResources.size(),
            originalSequenceTsFileCount,
            unSequenceTsFileResources.size(),
            originalUnSequenceTsFileCount,
            filteredTsFileResources.size(),
            originalSequenceTsFileCount + originalUnSequenceTsFileCount,
            System.currentTimeMillis() - startHistoricalExtractionTime);
      } finally {
        tsFileManager.readUnlock();
      }
    } finally {
      dataRegion.writeUnlock();
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

    if (!startIndex.isAfter(resource.getMaxProgressIndex())
        && !startIndex.equals(resource.getMaxProgressIndex())) {
      LOGGER.info(
          "Pipe {}@{}: file {} meets mayTsFileContainUnprocessedData condition, extractor progressIndex: {}, resource ProgressIndex: {}",
          pipeName,
          dataRegionId,
          resource.getTsFilePath(),
          startIndex,
          resource.getMaxProgressIndex());
      return true;
    }
    return false;
  }

  private boolean mayTsFileResourceOverlappedWithPattern(final TsFileResource resource) {
    final Set<IDeviceID> deviceSet;
    try {
      final Map<IDeviceID, Boolean> deviceIsAlignedMap =
          PipeDataNodeResourceManager.tsfile()
              .getDeviceIsAlignedMapFromCache(resource.getTsFile(), false);
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
            deviceID -> pipePattern.mayOverlapWithDevice(((PlainDeviceID) deviceID).toStringID()));
  }

  private boolean isTsFileResourceOverlappedWithTimeRange(final TsFileResource resource) {
    return !(resource.getFileEndTime() < historicalDataExtractionStartTime
        || historicalDataExtractionEndTime < resource.getFileStartTime());
  }

  private boolean isTsFileResourceCoveredByTimeRange(final TsFileResource resource) {
    return historicalDataExtractionStartTime <= resource.getFileStartTime()
        && historicalDataExtractionEndTime >= resource.getFileEndTime();
  }

  @Override
  public synchronized Event supply() {
    if (!hasBeenStarted && StorageEngine.getInstance().isReadyForNonReadWriteFunctions()) {
      start();
    }

    if (Objects.isNull(pendingQueue)) {
      return null;
    }

    final TsFileResource resource = pendingQueue.poll();

    if (resource == null) {
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

    if (!filteredTsFileResources.contains(resource)) {
      final ProgressReportEvent progressReportEvent =
          new ProgressReportEvent(pipeName, creationTime, pipeTaskMeta);
      progressReportEvent.bindProgressIndex(resource.getMaxProgressIndex());
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

    filteredTsFileResources.remove(resource);
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
    if (sloppyPattern || isDbNameCoveredByPattern) {
      event.skipParsingPattern();
    }

    if (sloppyTimeRange || isTsFileResourceCoveredByTimeRange(resource)) {
      event.skipParsingTime();
    }

    try {
      final boolean isReferenceCountIncreased =
          event.increaseReferenceCount(PipeHistoricalDataRegionTsFileSource.class.getName());
      if (!isReferenceCountIncreased) {
        LOGGER.warn(
            "Pipe {}@{}: failed to increase reference count for historical event {}, will discard it",
            pipeName,
            dataRegionId,
            event);
      }
      return isReferenceCountIncreased ? event : null;
    } finally {
      try {
        PipeDataNodeResourceManager.tsfile().unpinTsFileResource(resource, pipeName);
      } catch (final IOException e) {
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
    if (Objects.nonNull(pendingQueue)) {
      pendingQueue.forEach(
          resource -> {
            try {
              PipeDataNodeResourceManager.tsfile().unpinTsFileResource(resource, pipeName);
            } catch (final IOException e) {
              LOGGER.warn(
                  "Pipe {}@{}: failed to unpin TsFileResource after dropping pipe, original path: {}",
                  pipeName,
                  dataRegionId,
                  resource.getTsFilePath());
            }
          });
      pendingQueue.clear();
      pendingQueue = null;
    }
  }
}
