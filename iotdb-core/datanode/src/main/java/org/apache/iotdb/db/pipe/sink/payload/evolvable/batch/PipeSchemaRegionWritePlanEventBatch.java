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

package org.apache.iotdb.db.pipe.sink.payload.evolvable.batch;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.pipe.event.EnrichedEvent;
import org.apache.iotdb.db.pipe.event.common.schema.PipeSchemaRegionWritePlanEvent;
import org.apache.iotdb.db.pipe.resource.PipeDataNodeResourceManager;
import org.apache.iotdb.db.pipe.resource.memory.PipeMemoryBlock;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metedata.write.ActivateTemplateNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metedata.write.BatchActivateTemplateNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metedata.write.CreateAlignedTimeSeriesNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metedata.write.CreateMultiTimeSeriesNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metedata.write.CreateTimeSeriesNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metedata.write.InternalBatchActivateTemplateNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metedata.write.InternalCreateMultiTimeSeriesNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metedata.write.InternalCreateTimeSeriesNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metedata.write.MeasurementGroup;
import org.apache.iotdb.metrics.impl.DoNothingHistogram;
import org.apache.iotdb.metrics.type.Histogram;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_IOTDB_BATCH_DELAY_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_IOTDB_BATCH_DELAY_MS_DEFAULT_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_IOTDB_BATCH_DELAY_MS_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_IOTDB_BATCH_SIZE_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_IOTDB_PLAIN_BATCH_SIZE_DEFAULT_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.SINK_IOTDB_BATCH_DELAY_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.SINK_IOTDB_BATCH_DELAY_MS_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.SINK_IOTDB_BATCH_SIZE_KEY;

public class PipeSchemaRegionWritePlanEventBatch implements AutoCloseable {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(PipeSchemaRegionWritePlanEventBatch.class);

  private static final PlanNodeId EMPTY_PLAN_NODE_ID = new PlanNodeId("");

  private final int maxDelayInMs;
  private final long maxBatchSizeInBytes;
  private final PipeMemoryBlock allocatedMemoryBlock;

  private final List<EnrichedEvent> events = new ArrayList<>();

  private final Map<PartialPath, Pair<Boolean, MeasurementGroup>> deviceMap = new HashMap<>();
  private final Map<PartialPath, Pair<Integer, Integer>> templateActivationMap = new HashMap<>();

  private BatchType batchType = BatchType.NONE;
  private String pipeName;
  private long creationTime;

  private long firstEventProcessingTime = Long.MIN_VALUE;
  private PlanNode cachedPlanNode;
  private ByteBuffer cachedSerializedPlanNode;

  private volatile boolean isClosed = false;

  private Histogram batchSizeHistogram = new DoNothingHistogram();
  private Histogram batchTimeIntervalHistogram = new DoNothingHistogram();
  private Histogram eventSizeHistogram = new DoNothingHistogram();

  public PipeSchemaRegionWritePlanEventBatch(final PipeParameters parameters) {
    final Integer requestMaxDelayInMillis =
        parameters.getIntByKeys(CONNECTOR_IOTDB_BATCH_DELAY_MS_KEY, SINK_IOTDB_BATCH_DELAY_MS_KEY);
    if (Objects.isNull(requestMaxDelayInMillis)) {
      final int requestMaxDelayConfig =
          parameters.getIntOrDefault(
              Arrays.asList(CONNECTOR_IOTDB_BATCH_DELAY_KEY, SINK_IOTDB_BATCH_DELAY_KEY),
              CONNECTOR_IOTDB_BATCH_DELAY_MS_DEFAULT_VALUE);
      maxDelayInMs = requestMaxDelayConfig < 0 ? Integer.MAX_VALUE : requestMaxDelayConfig;
    } else {
      maxDelayInMs = requestMaxDelayInMillis < 0 ? Integer.MAX_VALUE : requestMaxDelayInMillis;
    }

    maxBatchSizeInBytes =
        parameters.getLongOrDefault(
            Arrays.asList(CONNECTOR_IOTDB_BATCH_SIZE_KEY, SINK_IOTDB_BATCH_SIZE_KEY),
            CONNECTOR_IOTDB_PLAIN_BATCH_SIZE_DEFAULT_VALUE);
    allocatedMemoryBlock = PipeDataNodeResourceManager.memory().forceAllocate(maxBatchSizeInBytes);
  }

  public synchronized boolean onEvent(final PipeSchemaRegionWritePlanEvent event) {
    if (isClosed || !canBatch(event)) {
      return false;
    }

    if (events.isEmpty() || !Objects.equals(events.get(events.size() - 1), event)) {
      if (!event.increaseReferenceCount(PipeSchemaRegionWritePlanEventBatch.class.getName())) {
        LOGGER.warn("Cannot increase reference count for event {}, ignore it.", event);
        return true;
      }

      try {
        if (Objects.isNull(pipeName)) {
          pipeName = event.getPipeName();
          creationTime = event.getCreationTime();
        }
        appendPlanNode(event.getPlanNode());
        invalidateCachedPlanNode();
        events.add(event);
      } catch (final Exception e) {
        event.decreaseReferenceCount(PipeSchemaRegionWritePlanEventBatch.class.getName(), false);
        throw e;
      }

      if (firstEventProcessingTime == Long.MIN_VALUE) {
        firstEventProcessingTime = System.currentTimeMillis();
      }
    }

    return true;
  }

  private boolean canBatch(final PipeSchemaRegionWritePlanEvent event) {
    final BatchType eventBatchType = resolveBatchType(event.getPlanNode());
    if (eventBatchType == BatchType.NONE || containsNonEmptyProps(event.getPlanNode())) {
      return false;
    }

    if (events.isEmpty()) {
      return true;
    }

    return Objects.equals(pipeName, event.getPipeName())
        && creationTime == event.getCreationTime()
        && batchType == eventBatchType
        && !hasAlignmentConflict(event.getPlanNode());
  }

  private BatchType resolveBatchType(final PlanNode planNode) {
    switch (planNode.getType()) {
      case CREATE_TIME_SERIES:
      case CREATE_ALIGNED_TIME_SERIES:
      case CREATE_MULTI_TIME_SERIES:
      case INTERNAL_CREATE_TIME_SERIES:
      case INTERNAL_CREATE_MULTI_TIMESERIES:
        return BatchType.TIMESERIES;
      case ACTIVATE_TEMPLATE:
      case BATCH_ACTIVATE_TEMPLATE:
      case INTERNAL_BATCH_ACTIVATE_TEMPLATE:
        return BatchType.TEMPLATE_ACTIVATE;
      default:
        return BatchType.NONE;
    }
  }

  private boolean containsNonEmptyProps(final PlanNode planNode) {
    switch (planNode.getType()) {
      case CREATE_TIME_SERIES:
        return hasNonEmptyProps(((CreateTimeSeriesNode) planNode).getProps());
      case CREATE_MULTI_TIME_SERIES:
        return ((CreateMultiTimeSeriesNode) planNode)
            .getMeasurementGroupMap().values().stream()
                .anyMatch(PipeSchemaRegionWritePlanEventBatch::hasNonEmptyProps);
      case INTERNAL_CREATE_TIME_SERIES:
        return hasNonEmptyProps(((InternalCreateTimeSeriesNode) planNode).getMeasurementGroup());
      case INTERNAL_CREATE_MULTI_TIMESERIES:
        return ((InternalCreateMultiTimeSeriesNode) planNode)
            .getDeviceMap().values().stream()
                .map(Pair::getRight)
                .anyMatch(PipeSchemaRegionWritePlanEventBatch::hasNonEmptyProps);
      default:
        return false;
    }
  }

  private static boolean hasNonEmptyProps(final MeasurementGroup measurementGroup) {
    return Objects.nonNull(measurementGroup.getPropsList())
        && measurementGroup.getPropsList().stream()
            .anyMatch(PipeSchemaRegionWritePlanEventBatch::hasNonEmptyProps);
  }

  private static boolean hasNonEmptyProps(final Map<String, String> props) {
    return Objects.nonNull(props) && !props.isEmpty();
  }

  private boolean hasAlignmentConflict(final PlanNode planNode) {
    switch (planNode.getType()) {
      case CREATE_TIME_SERIES:
        return hasAlignmentConflict(
            ((CreateTimeSeriesNode) planNode).getPath().getDevicePath(), false);
      case CREATE_ALIGNED_TIME_SERIES:
        return hasAlignmentConflict(((CreateAlignedTimeSeriesNode) planNode).getDevicePath(), true);
      case CREATE_MULTI_TIME_SERIES:
        return ((CreateMultiTimeSeriesNode) planNode)
            .getMeasurementGroupMap().keySet().stream()
                .anyMatch(devicePath -> hasAlignmentConflict(devicePath, false));
      case INTERNAL_CREATE_TIME_SERIES:
        return hasAlignmentConflict(
            ((InternalCreateTimeSeriesNode) planNode).getDevicePath(),
            ((InternalCreateTimeSeriesNode) planNode).isAligned());
      case INTERNAL_CREATE_MULTI_TIMESERIES:
        return ((InternalCreateMultiTimeSeriesNode) planNode)
            .getDeviceMap().entrySet().stream()
                .anyMatch(
                    entry -> hasAlignmentConflict(entry.getKey(), entry.getValue().getLeft()));
      default:
        return false;
    }
  }

  private boolean hasAlignmentConflict(final PartialPath devicePath, final boolean isAligned) {
    final Pair<Boolean, MeasurementGroup> existing = deviceMap.get(devicePath);
    return Objects.nonNull(existing) && !Objects.equals(existing.getLeft(), isAligned);
  }

  private void appendPlanNode(final PlanNode planNode) {
    if (batchType == BatchType.NONE) {
      batchType = resolveBatchType(planNode);
    }

    switch (planNode.getType()) {
      case CREATE_TIME_SERIES:
        appendCreateTimeSeriesNode((CreateTimeSeriesNode) planNode);
        break;
      case CREATE_ALIGNED_TIME_SERIES:
        appendCreateAlignedTimeSeriesNode((CreateAlignedTimeSeriesNode) planNode);
        break;
      case CREATE_MULTI_TIME_SERIES:
        appendCreateMultiTimeSeriesNode((CreateMultiTimeSeriesNode) planNode);
        break;
      case INTERNAL_CREATE_TIME_SERIES:
        appendInternalCreateTimeSeriesNode((InternalCreateTimeSeriesNode) planNode);
        break;
      case INTERNAL_CREATE_MULTI_TIMESERIES:
        appendInternalCreateMultiTimeSeriesNode((InternalCreateMultiTimeSeriesNode) planNode);
        break;
      case ACTIVATE_TEMPLATE:
        appendActivateTemplateNode((ActivateTemplateNode) planNode);
        break;
      case BATCH_ACTIVATE_TEMPLATE:
        appendBatchActivateTemplateNode((BatchActivateTemplateNode) planNode);
        break;
      case INTERNAL_BATCH_ACTIVATE_TEMPLATE:
        appendInternalBatchActivateTemplateNode((InternalBatchActivateTemplateNode) planNode);
        break;
      default:
        throw new IllegalArgumentException("Unsupported schema plan node " + planNode.getType());
    }
  }

  private void appendCreateTimeSeriesNode(final CreateTimeSeriesNode node) {
    appendMeasurement(
        node.getPath().getDevicePath(),
        false,
        node.getPath().getMeasurement(),
        node.getDataType(),
        node.getEncoding(),
        node.getCompressor(),
        node.getAlias(),
        node.getTags(),
        node.getAttributes());
  }

  private void appendCreateAlignedTimeSeriesNode(final CreateAlignedTimeSeriesNode node) {
    for (int i = 0; i < node.getMeasurements().size(); ++i) {
      appendMeasurement(
          node.getDevicePath(),
          true,
          node.getMeasurements().get(i),
          node.getDataTypes().get(i),
          node.getEncodings().get(i),
          node.getCompressors().get(i),
          Objects.nonNull(node.getAliasList()) ? node.getAliasList().get(i) : null,
          Objects.nonNull(node.getTagsList()) ? node.getTagsList().get(i) : null,
          Objects.nonNull(node.getAttributesList()) ? node.getAttributesList().get(i) : null);
    }
  }

  private void appendCreateMultiTimeSeriesNode(final CreateMultiTimeSeriesNode node) {
    node.getMeasurementGroupMap()
        .forEach(
            (devicePath, measurementGroup) ->
                appendMeasurementGroup(devicePath, false, measurementGroup));
  }

  private void appendInternalCreateTimeSeriesNode(final InternalCreateTimeSeriesNode node) {
    appendMeasurementGroup(node.getDevicePath(), node.isAligned(), node.getMeasurementGroup());
  }

  private void appendInternalCreateMultiTimeSeriesNode(
      final InternalCreateMultiTimeSeriesNode node) {
    node.getDeviceMap()
        .forEach(
            (devicePath, isAlignedAndMeasurementGroup) ->
                appendMeasurementGroup(
                    devicePath,
                    isAlignedAndMeasurementGroup.getLeft(),
                    isAlignedAndMeasurementGroup.getRight()));
  }

  private void appendMeasurementGroup(
      final PartialPath devicePath,
      final boolean isAligned,
      final MeasurementGroup measurementGroup) {
    for (int i = 0; i < measurementGroup.size(); ++i) {
      appendMeasurement(
          devicePath,
          isAligned,
          measurementGroup.getMeasurements().get(i),
          measurementGroup.getDataTypes().get(i),
          measurementGroup.getEncodings().get(i),
          measurementGroup.getCompressors().get(i),
          Objects.nonNull(measurementGroup.getAliasList())
              ? measurementGroup.getAliasList().get(i)
              : null,
          Objects.nonNull(measurementGroup.getTagsList())
              ? measurementGroup.getTagsList().get(i)
              : null,
          Objects.nonNull(measurementGroup.getAttributesList())
              ? measurementGroup.getAttributesList().get(i)
              : null);
    }
  }

  private void appendMeasurement(
      final PartialPath devicePath,
      final boolean isAligned,
      final String measurement,
      final TSDataType dataType,
      final TSEncoding encoding,
      final CompressionType compressor,
      final String alias,
      final Map<String, String> tags,
      final Map<String, String> attributes) {
    final MeasurementGroup group =
        deviceMap
            .computeIfAbsent(devicePath, key -> new Pair<>(isAligned, new MeasurementGroup()))
            .getRight();
    if (group.addMeasurement(measurement, dataType, encoding, compressor)) {
      group.addAlias(alias);
      group.addTags(tags);
      group.addAttributes(attributes);
    }
  }

  private void appendActivateTemplateNode(final ActivateTemplateNode node) {
    templateActivationMap.putIfAbsent(
        node.getActivatePath(), new Pair<>(node.getTemplateId(), node.getTemplateSetLevel()));
  }

  private void appendBatchActivateTemplateNode(final BatchActivateTemplateNode node) {
    node.getTemplateActivationMap().forEach(templateActivationMap::putIfAbsent);
  }

  private void appendInternalBatchActivateTemplateNode(
      final InternalBatchActivateTemplateNode node) {
    node.getTemplateActivationMap().forEach(templateActivationMap::putIfAbsent);
  }

  public synchronized boolean shouldEmit() {
    if (events.isEmpty() || firstEventProcessingTime == Long.MIN_VALUE) {
      return false;
    }
    return getSerializedPlanNodeSize() >= maxBatchSizeInBytes
        || System.currentTimeMillis() - firstEventProcessingTime >= maxDelayInMs;
  }

  public synchronized void recordBatchMetrics() {
    if (events.isEmpty() || firstEventProcessingTime == Long.MIN_VALUE) {
      return;
    }
    batchTimeIntervalHistogram.update(System.currentTimeMillis() - firstEventProcessingTime);
    batchSizeHistogram.update(getSerializedPlanNodeSize());
    eventSizeHistogram.update(events.size());
  }

  public synchronized PlanNode toPlanNode() {
    if (Objects.nonNull(cachedPlanNode)) {
      return cachedPlanNode;
    }

    switch (batchType) {
      case TIMESERIES:
        cachedPlanNode =
            new InternalCreateMultiTimeSeriesNode(EMPTY_PLAN_NODE_ID, new HashMap<>(deviceMap));
        return cachedPlanNode;
      case TEMPLATE_ACTIVATE:
        cachedPlanNode =
            new BatchActivateTemplateNode(EMPTY_PLAN_NODE_ID, new HashMap<>(templateActivationMap));
        return cachedPlanNode;
      default:
        throw new IllegalStateException("Cannot build schema batch plan node from empty batch.");
    }
  }

  public synchronized ByteBuffer toPlanNodeByteBuffer() {
    return getSerializedPlanNode().duplicate();
  }

  private long getSerializedPlanNodeSize() {
    return getSerializedPlanNode().remaining();
  }

  private ByteBuffer getSerializedPlanNode() {
    if (Objects.isNull(cachedSerializedPlanNode)) {
      cachedSerializedPlanNode = toPlanNode().serializeToByteBuffer();
    }
    return cachedSerializedPlanNode;
  }

  private void invalidateCachedPlanNode() {
    cachedPlanNode = null;
    cachedSerializedPlanNode = null;
  }

  public synchronized void onSuccess() {
    events.clear();
    deviceMap.clear();
    templateActivationMap.clear();
    batchType = BatchType.NONE;
    pipeName = null;
    creationTime = 0;
    invalidateCachedPlanNode();
    firstEventProcessingTime = Long.MIN_VALUE;
  }

  public synchronized void decreaseEventsReferenceCount(
      final String holderMessage, final boolean shouldReport) {
    events.forEach(event -> event.decreaseReferenceCount(holderMessage, shouldReport));
  }

  public synchronized void discardEventsOfPipe(
      final String pipeNameToDrop, final long creationTimeToDrop, final int regionId) {
    final boolean removed =
        events.removeIf(
            event -> {
              if (pipeNameToDrop.equals(event.getPipeName())
                  && creationTimeToDrop == event.getCreationTime()
                  && regionId == event.getRegionId()) {
                event.clearReferenceCount(PipeSchemaRegionWritePlanEventBatch.class.getName());
                return true;
              }
              return false;
            });
    if (removed) {
      rebuildFromEvents();
    }
  }

  private void rebuildFromEvents() {
    deviceMap.clear();
    templateActivationMap.clear();
    batchType = BatchType.NONE;
    pipeName = null;
    creationTime = 0;
    invalidateCachedPlanNode();

    if (events.isEmpty()) {
      firstEventProcessingTime = Long.MIN_VALUE;
      return;
    }

    // After a partial discard, the enqueue timestamp of the oldest remaining event is unknown.
    // Reset the emit window conservatively to avoid flushing immediately because of removed events.
    firstEventProcessingTime = System.currentTimeMillis();
    batchType = resolveBatchType(((PipeSchemaRegionWritePlanEvent) events.get(0)).getPlanNode());

    for (final EnrichedEvent event : events) {
      final PipeSchemaRegionWritePlanEvent schemaEvent = (PipeSchemaRegionWritePlanEvent) event;
      if (Objects.isNull(pipeName)) {
        pipeName = schemaEvent.getPipeName();
        creationTime = schemaEvent.getCreationTime();
      }
      appendPlanNode(schemaEvent.getPlanNode());
    }
    invalidateCachedPlanNode();
  }

  public synchronized boolean isEmpty() {
    return events.isEmpty();
  }

  public synchronized int size() {
    return events.size();
  }

  public synchronized String getPipeName() {
    return pipeName;
  }

  public synchronized long getCreationTime() {
    return creationTime;
  }

  public void setBatchSizeHistogram(final Histogram batchSizeHistogram) {
    if (Objects.nonNull(batchSizeHistogram)) {
      this.batchSizeHistogram = batchSizeHistogram;
    }
  }

  public void setBatchTimeIntervalHistogram(final Histogram batchTimeIntervalHistogram) {
    if (Objects.nonNull(batchTimeIntervalHistogram)) {
      this.batchTimeIntervalHistogram = batchTimeIntervalHistogram;
    }
  }

  public void setEventSizeHistogram(final Histogram eventSizeHistogram) {
    if (Objects.nonNull(eventSizeHistogram)) {
      this.eventSizeHistogram = eventSizeHistogram;
    }
  }

  @Override
  public synchronized void close() {
    isClosed = true;
    events.forEach(
        event -> event.clearReferenceCount(PipeSchemaRegionWritePlanEventBatch.class.getName()));
    events.clear();
    deviceMap.clear();
    templateActivationMap.clear();
    invalidateCachedPlanNode();
    allocatedMemoryBlock.close();
  }

  private enum BatchType {
    NONE,
    TIMESERIES,
    TEMPLATE_ACTIVATE
  }
}
