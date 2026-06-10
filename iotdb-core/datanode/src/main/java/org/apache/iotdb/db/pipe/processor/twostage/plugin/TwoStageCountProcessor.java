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

package org.apache.iotdb.db.pipe.processor.twostage.plugin;

import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.commons.consensus.index.ProgressIndex;
import org.apache.iotdb.commons.consensus.index.impl.MinimumProgressIndex;
import org.apache.iotdb.commons.consensus.index.impl.StateProgressIndex;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeTaskMeta;
import org.apache.iotdb.commons.pipe.config.plugin.env.PipeTaskProcessorRuntimeEnvironment;
import org.apache.iotdb.commons.pipe.event.EnrichedEvent;
import org.apache.iotdb.commons.utils.PathUtils;
import org.apache.iotdb.db.i18n.DataNodePipeMessages;
import org.apache.iotdb.db.pipe.event.common.heartbeat.PipeHeartbeatEvent;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeInsertNodeTabletInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeRawTabletInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.tsfile.PipeTsFileInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.watermark.PipeWatermarkEvent;
import org.apache.iotdb.db.pipe.processor.twostage.combiner.PipeCombineHandlerManager;
import org.apache.iotdb.db.pipe.processor.twostage.exchange.payload.CombineRequest;
import org.apache.iotdb.db.pipe.processor.twostage.exchange.payload.FetchCombineResultRequest;
import org.apache.iotdb.db.pipe.processor.twostage.exchange.payload.FetchCombineResultResponse;
import org.apache.iotdb.db.pipe.processor.twostage.exchange.sender.TwoStageAggregateSender;
import org.apache.iotdb.db.pipe.processor.twostage.operator.CountOperator;
import org.apache.iotdb.db.pipe.processor.twostage.state.CountState;
import org.apache.iotdb.db.storageengine.StorageEngine;
import org.apache.iotdb.pipe.api.PipeProcessor;
import org.apache.iotdb.pipe.api.annotation.TreeModel;
import org.apache.iotdb.pipe.api.collector.EventCollector;
import org.apache.iotdb.pipe.api.customizer.configuration.PipeProcessorRuntimeConfiguration;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameterValidator;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;
import org.apache.iotdb.pipe.api.event.Event;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;
import org.apache.iotdb.pipe.api.event.dml.insertion.TsFileInsertionEvent;
import org.apache.iotdb.pipe.api.exception.PipeException;
import org.apache.iotdb.pipe.api.exception.PipeParameterNotValidException;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferResp;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.iotdb.commons.pipe.config.constant.PipeProcessorConstant.PROCESSOR_OUTPUT_SERIES_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeProcessorConstant._PROCESSOR_OUTPUT_SERIES_KEY;

@TreeModel
public class TwoStageCountProcessor implements PipeProcessor {

  private static final Logger LOGGER = LoggerFactory.getLogger(TwoStageCountProcessor.class);

  private String pipeName;
  private long creationTime;
  private int regionId;
  private PipeTaskMeta pipeTaskMeta;
  private String dataBaseName;
  private Boolean isTableModel;

  private PartialPath outputSeries;

  private static final String LOCAL_COUNT_STATE_KEY = "count";
  private final AtomicLong localCount = new AtomicLong(0);
  private final AtomicReference<ProgressIndex> localCommitProgressIndex =
      new AtomicReference<>(MinimumProgressIndex.INSTANCE);

  private final Queue<Pair<long[], ProgressIndex> /* ([timestamp, local count], progress index) */>
      localRequestQueue = new ConcurrentLinkedQueue<>();
  private final Queue<Pair<long[], ProgressIndex> /* ([timestamp, local count], progress index) */>
      localCommitQueue = new ConcurrentLinkedQueue<>();

  private TwoStageAggregateSender twoStageAggregateSender;
  private final Queue<Pair<Long, Long> /* (timestamp, global count) */> globalCountQueue =
      new ConcurrentLinkedQueue<>();

  @Override
  public void validate(PipeParameterValidator validator) throws Exception {
    checkInvalidParameters(validator);

    final String rawOutputSeries =
        validator
            .getParameters()
            .getStringByKeys(PROCESSOR_OUTPUT_SERIES_KEY, _PROCESSOR_OUTPUT_SERIES_KEY);
    try {
      PathUtils.isLegalPath(Objects.requireNonNull(rawOutputSeries));
    } catch (Exception e) {
      throw new PipeParameterNotValidException(
          DataNodePipeMessages.ILLEGAL_OUTPUT_SERIES_PATH + rawOutputSeries);
    }
  }

  private void checkInvalidParameters(final PipeParameterValidator validator) {
    // Check coexistence of output.series and output-series
    validator.validateSynonymAttributes(
        Collections.singletonList(PROCESSOR_OUTPUT_SERIES_KEY),
        Collections.singletonList(_PROCESSOR_OUTPUT_SERIES_KEY),
        true);
  }

  @Override
  public void customize(PipeParameters parameters, PipeProcessorRuntimeConfiguration configuration)
      throws Exception {
    final PipeTaskProcessorRuntimeEnvironment runtimeEnvironment =
        (PipeTaskProcessorRuntimeEnvironment) configuration.getRuntimeEnvironment();
    pipeName = runtimeEnvironment.getPipeName();
    creationTime = runtimeEnvironment.getCreationTime();
    regionId = runtimeEnvironment.getRegionId();
    pipeTaskMeta = runtimeEnvironment.getPipeTaskMeta();
    dataBaseName =
        StorageEngine.getInstance()
            .getDataRegion(new DataRegionId(runtimeEnvironment.getRegionId()))
            .getDatabaseName();
    if (dataBaseName != null) {
      isTableModel = PathUtils.isTableModelDatabase(dataBaseName);
    }

    outputSeries = parseOutputSeries(parameters);

    if (Objects.nonNull(pipeTaskMeta) && Objects.nonNull(pipeTaskMeta.getProgressIndex())) {
      if (pipeTaskMeta.getProgressIndex() instanceof MinimumProgressIndex) {
        pipeTaskMeta.updateProgressIndex(
            new StateProgressIndex(Long.MIN_VALUE, new HashMap<>(), MinimumProgressIndex.INSTANCE));
      }

      final StateProgressIndex stateProgressIndex =
          (StateProgressIndex) pipeTaskMeta.getProgressIndex();
      localCommitProgressIndex.set(stateProgressIndex.getInnerProgressIndex());
      final Binary localCountState = stateProgressIndex.getState().get(LOCAL_COUNT_STATE_KEY);
      localCount.set(
          Objects.isNull(localCountState) ? 0 : Long.parseLong(localCountState.toString()));
    }
    LOGGER.info(
        DataNodePipeMessages.TWOSTAGECOUNTPROCESSOR_CUSTOMIZED_BY_THREAD_PIPENAME_CREATIONTIME_RE,
        Thread.currentThread().getName(),
        pipeName,
        creationTime,
        regionId,
        outputSeries,
        localCommitProgressIndex.get(),
        localCount.get());

    PipeCombineHandlerManager.getInstance()
        .register(
            pipeName, creationTime, (combineId) -> new CountOperator(combineId, globalCountQueue));
    twoStageAggregateSender = new TwoStageAggregateSender(pipeName, creationTime);
  }

  static PartialPath parseOutputSeries(final PipeParameters parameters)
      throws IllegalPathException {
    return new PartialPath(
        parameters.getStringByKeys(PROCESSOR_OUTPUT_SERIES_KEY, _PROCESSOR_OUTPUT_SERIES_KEY));
  }

  @Override
  public void process(TabletInsertionEvent tabletInsertionEvent, EventCollector eventCollector)
      throws Exception {
    if (!(tabletInsertionEvent instanceof PipeInsertNodeTabletInsertionEvent)
        && !(tabletInsertionEvent instanceof PipeRawTabletInsertionEvent)) {
      LOGGER.warn(
          DataNodePipeMessages.IGNORED_TABLETINSERTIONEVENT_IS_NOT_AN_INSTANCE_OF,
          tabletInsertionEvent);
      return;
    }

    final EnrichedEvent event = (EnrichedEvent) tabletInsertionEvent;
    event.skipReportOnCommit();

    final long count =
        (event instanceof PipeInsertNodeTabletInsertionEvent)
            ? ((PipeInsertNodeTabletInsertionEvent) event).count()
            : ((PipeRawTabletInsertionEvent) event).count();
    localCount.accumulateAndGet(count, Long::sum);

    localCommitProgressIndex.updateAndGet(
        index -> index.updateToMinimumEqualOrIsAfterProgressIndex(event.getProgressIndex()));
  }

  @Override
  public void process(TsFileInsertionEvent tsFileInsertionEvent, EventCollector eventCollector)
      throws Exception {
    if (!(tsFileInsertionEvent instanceof PipeTsFileInsertionEvent)) {
      LOGGER.warn(
          DataNodePipeMessages.IGNORED_TSFILEINSERTIONEVENT_IS_NOT_AN_INSTANCE_OF,
          tsFileInsertionEvent);
      return;
    }

    final PipeTsFileInsertionEvent event = (PipeTsFileInsertionEvent) tsFileInsertionEvent;
    event.skipReportOnCommit();

    if (!event.waitForTsFileClose()) {
      LOGGER.warn(DataNodePipeMessages.IGNORED_TSFILEINSERTIONEVENT_IS_EMPTY, event);
      return;
    }

    final long count = event.count(true);
    localCount.accumulateAndGet(count, Long::sum);

    localCommitProgressIndex.updateAndGet(
        index -> index.updateToMinimumEqualOrIsAfterProgressIndex(event.getProgressIndex()));
  }

  @Override
  public void process(Event event, EventCollector eventCollector) throws Exception {
    if (event instanceof PipeHeartbeatEvent) {
      collectGlobalCountIfNecessary(eventCollector);
      commitLocalProgressIndexIfNecessary();
      triggerCombineIfNecessary();
      eventCollector.collect(event);
      return;
    }

    if (event instanceof PipeWatermarkEvent) {
      triggerCombine(
          new Pair<>(
              new long[] {((PipeWatermarkEvent) event).getWatermark(), localCount.get()},
              localCommitProgressIndex.get()));
      // TODO: Collect watermark events. We ignore it because they may cause OOM in collector.
    }
  }

  private void collectGlobalCountIfNecessary(EventCollector eventCollector) throws IOException {
    while (!globalCountQueue.isEmpty()) {
      final Object lastCombinedValue =
          PipeCombineHandlerManager.getInstance().getLastCombinedValue(pipeName, creationTime);
      final Pair<Long, Long> lastCollectedTimestampCountPair =
          Objects.isNull(lastCombinedValue)
              ? new Pair<>(Long.MIN_VALUE, 0L)
              : (Pair<Long, Long>) lastCombinedValue;

      final Pair<Long, Long> timestampCountPair = globalCountQueue.poll();
      if (timestampCountPair.right < lastCollectedTimestampCountPair.right) {
        timestampCountPair.right = lastCollectedTimestampCountPair.right;
        LOGGER.warn(
            DataNodePipeMessages.GLOBAL_COUNT_IS_LESS_THAN_THE_LAST,
            timestampCountPair.left,
            timestampCountPair.right);
      }

      final Tablet tablet =
          new Tablet(
              outputSeries.getIDeviceID().toString(),
              Collections.singletonList(
                  new MeasurementSchema(outputSeries.getMeasurement(), TSDataType.INT64)),
              1);
      tablet.addTimestamp(0, timestampCountPair.left);
      tablet.addValue(outputSeries.getMeasurement(), 0, timestampCountPair.right);

      // TODO: table model database name is not supported
      eventCollector.collect(
          new PipeRawTabletInsertionEvent(
              isTableModel, dataBaseName, null, null, tablet, false, null, 0, null, null, false));

      PipeCombineHandlerManager.getInstance()
          .updateLastCombinedValue(pipeName, creationTime, timestampCountPair);
    }
  }

  private void commitLocalProgressIndexIfNecessary() {
    final int currentQueueSize = localCommitQueue.size();
    for (int i = 0; i < currentQueueSize; i++) {
      final Pair<long[], ProgressIndex> pair = localCommitQueue.poll();
      if (Objects.isNull(pair)) {
        break;
      }

      try {
        // TODO: optimize the combine result fetching with batch fetching
        final FetchCombineResultResponse fetchCombineResultResponse =
            FetchCombineResultResponse.fromTPipeTransferResp(
                twoStageAggregateSender.request(
                    pair.left[0],
                    FetchCombineResultRequest.toTPipeTransferReq(
                        pipeName,
                        creationTime,
                        Collections.singletonList(Long.toString(pair.left[0])))));

        if (fetchCombineResultResponse.getStatus().getCode()
            != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
          throw new PipeException(
              DataNodePipeMessages.FAILED_TO_FETCH_COMBINE_RESULT
                  + fetchCombineResultResponse.getStatus().getMessage());
        }

        for (final Map.Entry<String, FetchCombineResultResponse.CombineResultType> entry :
            fetchCombineResultResponse.getCombineId2ResultType().entrySet()) {
          final String combineId = entry.getKey();
          final FetchCombineResultResponse.CombineResultType resultType = entry.getValue();

          switch (resultType) {
            case OUTDATED:
              LOGGER.warn(
                  DataNodePipeMessages.TWO_STAGE_COMBINE_REGION_ID_COMBINE_ID_1,
                  regionId,
                  combineId,
                  pair.left[0],
                  pair.left[1],
                  pair.right);
              continue;
            case INCOMPLETE:
              LOGGER.info(
                  DataNodePipeMessages.TWO_STAGE_COMBINE_REGION_ID_COMBINE_ID,
                  regionId,
                  combineId,
                  pair.left[0],
                  pair.left[1],
                  pair.right);
              localCommitQueue.add(pair);
              continue;
            case SUCCESS:
              final Map<String, Binary> state = new HashMap<>();
              state.put(LOCAL_COUNT_STATE_KEY, new Binary(Long.toString(pair.left[1]).getBytes()));
              pipeTaskMeta.updateProgressIndex(
                  new StateProgressIndex(pair.left[0], state, pair.right));
              LOGGER.info(
                  DataNodePipeMessages.TWO_STAGE_COMBINE_REGION_ID_COMBINE_ID_2,
                  regionId,
                  combineId,
                  pair.left[0],
                  pair.left[1],
                  pair.right,
                  pipeTaskMeta.getProgressIndex());
              continue;
            default:
              throw new PipeException(
                  DataNodePipeMessages.UNKNOWN_COMBINE_RESULT_TYPE + resultType);
          }
        }
      } catch (Exception e) {
        localCommitQueue.add(pair);
        LOGGER.warn(
            DataNodePipeMessages.FAILURE_OCCURRED_WHEN_TRYING_TO_COMMIT_PROGRESS,
            pair.left[0],
            pair.left[1],
            pair.right,
            e);
        return;
      }
    }
  }

  private void triggerCombineIfNecessary() {
    while (!localRequestQueue.isEmpty()) {
      if (!triggerCombine(localRequestQueue.poll())) {
        return;
      }
    }
  }

  private boolean triggerCombine(Pair<long[], ProgressIndex> pair) {
    final long watermark = pair.getLeft()[0];
    final long count = pair.getLeft()[1];
    final ProgressIndex progressIndex = pair.getRight();
    try {
      final TPipeTransferResp resp =
          twoStageAggregateSender.request(
              watermark,
              CombineRequest.toTPipeTransferReq(
                  pipeName,
                  creationTime,
                  regionId,
                  Long.toString(watermark),
                  new CountState(count)));
      if (resp.getStatus().getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        throw new PipeException(
            DataNodePipeMessages.FAILED_TO_COMBINE_COUNT + resp.getStatus().getMessage());
      }
      localCommitQueue.add(pair);
      return true;
    } catch (Exception e) {
      localRequestQueue.add(pair);
      LOGGER.warn(
          DataNodePipeMessages.FAILED_TO_TRIGGER_COMBINE_WATERMARK_COUNT_PROGRESSINDEX,
          watermark,
          count,
          progressIndex,
          e);
      return false;
    }
  }

  @Override
  public void close() throws Exception {
    if (Objects.nonNull(twoStageAggregateSender)) {
      twoStageAggregateSender.close();
    }
    PipeCombineHandlerManager.getInstance().deregister(pipeName, creationTime);
  }
}
