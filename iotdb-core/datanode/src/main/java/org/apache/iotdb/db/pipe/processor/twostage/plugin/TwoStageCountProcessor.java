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

import org.apache.iotdb.commons.consensus.index.ProgressIndex;
import org.apache.iotdb.commons.consensus.index.impl.MinimumProgressIndex;
import org.apache.iotdb.commons.consensus.index.impl.StateProgressIndex;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.pipe.config.constant.PipeProcessorConstant;
import org.apache.iotdb.commons.pipe.config.plugin.env.PipeTaskProcessorRuntimeEnvironment;
import org.apache.iotdb.commons.pipe.task.meta.PipeTaskMeta;
import org.apache.iotdb.commons.utils.PathUtils;
import org.apache.iotdb.db.pipe.event.common.heartbeat.PipeHeartbeatEvent;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeRawTabletInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.watermark.PipeWatermarkEvent;
import org.apache.iotdb.db.pipe.processor.twostage.combiner.PipeCombineHandlerManager;
import org.apache.iotdb.db.pipe.processor.twostage.exchange.payload.CombineRequest;
import org.apache.iotdb.db.pipe.processor.twostage.exchange.payload.FetchCombineResultRequest;
import org.apache.iotdb.db.pipe.processor.twostage.exchange.payload.FetchCombineResultResponse;
import org.apache.iotdb.db.pipe.processor.twostage.exchange.sender.TwoStageAggregateSender;
import org.apache.iotdb.db.pipe.processor.twostage.operator.CountOperator;
import org.apache.iotdb.db.pipe.processor.twostage.state.CountState;
import org.apache.iotdb.pipe.api.PipeProcessor;
import org.apache.iotdb.pipe.api.collector.EventCollector;
import org.apache.iotdb.pipe.api.customizer.configuration.PipeProcessorRuntimeConfiguration;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameterValidator;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;
import org.apache.iotdb.pipe.api.event.Event;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;
import org.apache.iotdb.pipe.api.event.dml.insertion.TsFileInsertionEvent;
import org.apache.iotdb.pipe.api.exception.PipeException;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferResp;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class TwoStageCountProcessor implements PipeProcessor {

  private static final Logger LOGGER = LoggerFactory.getLogger(TwoStageCountProcessor.class);

  private String pipeName;
  private long creationTime;
  private int regionId;
  private PipeTaskMeta pipeTaskMeta;

  private PartialPath outputSeries;

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
    validator.validateRequiredAttribute(PipeProcessorConstant.PROCESSOR_OUTPUT_SERIES_KEY);

    final String rawOutputSeries =
        validator.getParameters().getString(PipeProcessorConstant.PROCESSOR_OUTPUT_SERIES_KEY);
    try {
      PathUtils.isLegalPath(rawOutputSeries);
    } catch (IllegalPathException e) {
      throw new IllegalArgumentException("Illegal output series path: " + rawOutputSeries);
    }
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

    outputSeries =
        new PartialPath(parameters.getString(PipeProcessorConstant.PROCESSOR_OUTPUT_SERIES_KEY));

    if (Objects.nonNull(pipeTaskMeta) && Objects.nonNull(pipeTaskMeta.getProgressIndex())) {
      if (pipeTaskMeta.getProgressIndex() instanceof MinimumProgressIndex) {
        pipeTaskMeta.updateProgressIndex(
            new StateProgressIndex(new HashMap<>(), MinimumProgressIndex.INSTANCE));
      }
      localCommitProgressIndex.set(pipeTaskMeta.getProgressIndex());
    }
    LOGGER.info(
        "TwoStageCountProcessor customized by thread {}: pipeName={}, creationTime={}, regionId={}, outputSeries={}, progressIndex={}",
        Thread.currentThread().getName(),
        pipeName,
        creationTime,
        regionId,
        outputSeries,
        localCommitProgressIndex.get());

    twoStageAggregateSender = new TwoStageAggregateSender();
    PipeCombineHandlerManager.getInstance()
        .register(
            pipeName, creationTime, (combineId) -> new CountOperator(combineId, globalCountQueue));
  }

  @Override
  public void process(TabletInsertionEvent tabletInsertionEvent, EventCollector eventCollector)
      throws Exception {
    // TODO: count the number of given points in the tablet
    // TODO: ignore progress index report
    localCount.incrementAndGet();
  }

  @Override
  public void process(TsFileInsertionEvent tsFileInsertionEvent, EventCollector eventCollector)
      throws Exception {
    // TODO: count the number of points in the TsFile
    localCount.incrementAndGet();
  }

  @Override
  public void process(Event event, EventCollector eventCollector) throws Exception {
    if (event instanceof PipeHeartbeatEvent) {
      collectGlobalCountIfNecessary(eventCollector);
      commitLocalProgressIndexIfNecessary();
      triggerCombineIfNecessary();
    }

    if (event instanceof PipeWatermarkEvent) {
      triggerCombine(
          ((PipeWatermarkEvent) event).getWatermark(),
          localCount.get(),
          localCommitProgressIndex.get());
    }
  }

  private void collectGlobalCountIfNecessary(EventCollector eventCollector) throws IOException {
    while (!globalCountQueue.isEmpty()) {
      final Pair<Long, Long> timestampCountPair = globalCountQueue.poll();
      final Tablet tablet =
          new Tablet(
              outputSeries.getDevice(),
              Collections.singletonList(
                  new MeasurementSchema(outputSeries.getMeasurement(), TSDataType.INT64)),
              1);
      tablet.rowSize = 1;
      tablet.addTimestamp(0, timestampCountPair.left);
      tablet.addValue(outputSeries.getMeasurement(), 0, timestampCountPair.right);
      eventCollector.collect(
          new PipeRawTabletInsertionEvent(tablet, false, null, null, null, false));
    }
  }

  private void commitLocalProgressIndexIfNecessary() throws TException, IOException {
    while (!localCommitQueue.isEmpty()) {
      final Pair<long[], ProgressIndex> pair = localCommitQueue.poll();
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
              "Failed to fetch combine result: "
                  + fetchCombineResultResponse.getStatus().getMessage());
        }
        fetchCombineResultResponse
            .getCombineId2ResultType()
            .forEach(
                (combineId, resultType) -> {
                  switch (resultType) {
                    case OUTDATED:
                      return;
                    case INCOMPLETE:
                      localCommitQueue.add(pair);
                      return;
                    case SUCCESS:
                      pipeTaskMeta.updateProgressIndex(pair.right);
                      return;
                    default:
                      throw new PipeException("Unknown combine result type: " + resultType);
                  }
                });
      } catch (Exception e) {
        localCommitQueue.add(pair);
        throw e;
      }
    }
  }

  private void triggerCombineIfNecessary() throws TException, IOException {
    while (!localRequestQueue.isEmpty()) {
      final Pair<long[], ProgressIndex> timestampCountPair = localRequestQueue.poll();
      triggerCombine(
          timestampCountPair.left[0], timestampCountPair.left[1], timestampCountPair.right);
    }
  }

  private void triggerCombine(long watermark, long count, ProgressIndex progressIndex)
      throws TException, IOException {
    final Pair<long[], ProgressIndex> pair =
        new Pair<>(new long[] {watermark, count}, progressIndex);
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
        throw new PipeException("Failed to combine count: " + resp.getStatus().getMessage());
      }
      localCommitQueue.add(pair);
    } catch (Exception e) {
      localRequestQueue.add(pair);
      throw e;
    }
  }

  @Override
  public void close() throws Exception {
    PipeCombineHandlerManager.getInstance().deregister(pipeName, creationTime);
    if (Objects.nonNull(twoStageAggregateSender)) {
      twoStageAggregateSender.close();
    }
  }
}
