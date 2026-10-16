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

package org.apache.iotdb.db.pipe.sink;

import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.pipe.agent.task.progress.CommitterKey;
import org.apache.iotdb.db.pipe.event.common.schema.PipeSchemaRegionWritePlanEvent;
import org.apache.iotdb.db.pipe.sink.payload.evolvable.batch.PipeSchemaRegionWritePlanEventBatch;
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
import org.apache.iotdb.metrics.type.Histogram;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.utils.Pair;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_IOTDB_BATCH_DELAY_MS_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_IOTDB_BATCH_SIZE_KEY;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class PipeSchemaRegionWritePlanEventBatchTest {

  @Test
  public void testBatchTimeSeriesEvents() throws Exception {
    try (PipeSchemaRegionWritePlanEventBatch batch =
        new PipeSchemaRegionWritePlanEventBatch(createParameters(1000, 1048576))) {
      Assert.assertTrue(
          batch.onEvent(
              createEvent(
                  new CreateTimeSeriesNode(
                      new PlanNodeId("1"),
                      new MeasurementPath("root.db.d1.s1"),
                      TSDataType.INT64,
                      TSEncoding.PLAIN,
                      CompressionType.LZ4,
                      null,
                      Collections.singletonMap("tag", "v1"),
                      Collections.singletonMap("attr", "a1"),
                      "alias1"),
                  "pipeA",
                  1L)));

      Assert.assertTrue(
          batch.onEvent(
              createEvent(
                  new CreateAlignedTimeSeriesNode(
                      new PlanNodeId("2"),
                      new PartialPath("root.db.d2"),
                      Arrays.asList("s1", "s2"),
                      Arrays.asList(TSDataType.INT32, TSDataType.DOUBLE),
                      Arrays.asList(TSEncoding.RLE, TSEncoding.GORILLA),
                      Arrays.asList(CompressionType.SNAPPY, CompressionType.ZSTD),
                      Arrays.asList("alias2", null),
                      Arrays.asList(Collections.singletonMap("tag", "v2"), null),
                      Arrays.asList(Collections.singletonMap("attr", "a2"), null)),
                  "pipeA",
                  1L)));

      final PlanNode planNode = batch.toPlanNode();
      Assert.assertTrue(planNode instanceof InternalCreateMultiTimeSeriesNode);

      final Map<PartialPath, Pair<Boolean, MeasurementGroup>> deviceMap =
          ((InternalCreateMultiTimeSeriesNode) planNode).getDeviceMap();
      Assert.assertEquals(2, deviceMap.size());

      final Pair<Boolean, MeasurementGroup> d1Group = deviceMap.get(new PartialPath("root.db.d1"));
      Assert.assertNotNull(d1Group);
      Assert.assertFalse(d1Group.getLeft());
      Assert.assertEquals(Collections.singletonList("s1"), d1Group.getRight().getMeasurements());
      Assert.assertEquals(Collections.singletonList("alias1"), d1Group.getRight().getAliasList());
      Assert.assertEquals(
          Collections.singletonList(Collections.singletonMap("tag", "v1")),
          d1Group.getRight().getTagsList());
      Assert.assertEquals(
          Collections.singletonList(Collections.singletonMap("attr", "a1")),
          d1Group.getRight().getAttributesList());

      final Pair<Boolean, MeasurementGroup> d2Group = deviceMap.get(new PartialPath("root.db.d2"));
      Assert.assertNotNull(d2Group);
      Assert.assertTrue(d2Group.getLeft());
      Assert.assertEquals(Arrays.asList("s1", "s2"), d2Group.getRight().getMeasurements());
      Assert.assertEquals(Arrays.asList("alias2", null), d2Group.getRight().getAliasList());
    }
  }

  @Test
  public void testBatchAdditionalTimeSeriesNodeTypes() throws Exception {
    try (PipeSchemaRegionWritePlanEventBatch batch =
        new PipeSchemaRegionWritePlanEventBatch(createParameters(1000, 1048576))) {
      final Map<PartialPath, MeasurementGroup> createMultiMap = new HashMap<>();
      createMultiMap.put(new PartialPath("root.db.d1"), createMeasurementGroup("s1", "alias1"));
      Assert.assertTrue(
          batch.onEvent(
              createEvent(
                  new CreateMultiTimeSeriesNode(new PlanNodeId("1"), createMultiMap),
                  "pipeA",
                  1L)));

      Assert.assertTrue(
          batch.onEvent(
              createEvent(
                  new InternalCreateTimeSeriesNode(
                      new PlanNodeId("2"),
                      new PartialPath("root.db.d2"),
                      createMeasurementGroup("s2", "alias2"),
                      true),
                  "pipeA",
                  1L)));

      final Map<PartialPath, Pair<Boolean, MeasurementGroup>> internalCreateMultiMap =
          new HashMap<>();
      internalCreateMultiMap.put(
          new PartialPath("root.db.d3"), new Pair<>(false, createMeasurementGroup("s3", "alias3")));
      Assert.assertTrue(
          batch.onEvent(
              createEvent(
                  new InternalCreateMultiTimeSeriesNode(
                      new PlanNodeId("3"), internalCreateMultiMap),
                  "pipeA",
                  1L)));

      final PlanNode planNode = batch.toPlanNode();
      Assert.assertTrue(planNode instanceof InternalCreateMultiTimeSeriesNode);

      final Map<PartialPath, Pair<Boolean, MeasurementGroup>> deviceMap =
          ((InternalCreateMultiTimeSeriesNode) planNode).getDeviceMap();
      Assert.assertEquals(3, deviceMap.size());
      Assert.assertFalse(deviceMap.get(new PartialPath("root.db.d1")).getLeft());
      Assert.assertTrue(deviceMap.get(new PartialPath("root.db.d2")).getLeft());
      Assert.assertFalse(deviceMap.get(new PartialPath("root.db.d3")).getLeft());
      Assert.assertEquals(
          Collections.singletonList("s2"),
          deviceMap.get(new PartialPath("root.db.d2")).getRight().getMeasurements());
      Assert.assertEquals(
          Collections.singletonList("alias3"),
          deviceMap.get(new PartialPath("root.db.d3")).getRight().getAliasList());
    }
  }

  @Test
  public void testBatchTemplateActivationEvents() throws Exception {
    try (PipeSchemaRegionWritePlanEventBatch batch =
        new PipeSchemaRegionWritePlanEventBatch(createParameters(1000, 1048576))) {
      Assert.assertTrue(
          batch.onEvent(
              createEvent(
                  new ActivateTemplateNode(
                      new PlanNodeId("1"), new PartialPath("root.db.d1"), 1, 10),
                  "pipeA",
                  1L)));

      final Map<PartialPath, Pair<Integer, Integer>> templateActivationMap = new HashMap<>();
      templateActivationMap.put(new PartialPath("root.db.d2"), new Pair<>(2, 20));
      Assert.assertTrue(
          batch.onEvent(
              createEvent(
                  new BatchActivateTemplateNode(new PlanNodeId("2"), templateActivationMap),
                  "pipeA",
                  1L)));

      final Map<PartialPath, Pair<Integer, Integer>> internalTemplateActivationMap =
          new HashMap<>();
      internalTemplateActivationMap.put(new PartialPath("root.db.d3"), new Pair<>(3, 30));
      Assert.assertTrue(
          batch.onEvent(
              createEvent(
                  new InternalBatchActivateTemplateNode(
                      new PlanNodeId("3"), internalTemplateActivationMap),
                  "pipeA",
                  1L)));

      final PlanNode planNode = batch.toPlanNode();
      Assert.assertTrue(planNode instanceof BatchActivateTemplateNode);

      final Map<PartialPath, Pair<Integer, Integer>> batchedMap =
          ((BatchActivateTemplateNode) planNode).getTemplateActivationMap();
      Assert.assertEquals(3, batchedMap.size());
      Assert.assertEquals(new Pair<>(10, 1), batchedMap.get(new PartialPath("root.db.d1")));
      Assert.assertEquals(new Pair<>(2, 20), batchedMap.get(new PartialPath("root.db.d2")));
      Assert.assertEquals(new Pair<>(3, 30), batchedMap.get(new PartialPath("root.db.d3")));

      Assert.assertFalse(
          batch.onEvent(
              createEvent(
                  new CreateTimeSeriesNode(
                      new PlanNodeId("4"),
                      new MeasurementPath("root.db.d4.s1"),
                      TSDataType.INT64,
                      TSEncoding.PLAIN,
                      CompressionType.LZ4,
                      null,
                      null,
                      null,
                      null),
                  "pipeA",
                  1L)));
    }
  }

  @Test
  public void testRejectDifferentPipePropsAndAlignmentConflict() throws Exception {
    try (PipeSchemaRegionWritePlanEventBatch batch =
        new PipeSchemaRegionWritePlanEventBatch(createParameters(1000, 1048576))) {
      Assert.assertFalse(
          batch.onEvent(
              createEvent(
                  new CreateTimeSeriesNode(
                      new PlanNodeId("1"),
                      new MeasurementPath("root.db.d1.s1"),
                      TSDataType.INT64,
                      TSEncoding.PLAIN,
                      CompressionType.LZ4,
                      Collections.singletonMap("prop", "v1"),
                      null,
                      null,
                      null),
                  "pipeA",
                  1L)));

      final Map<PartialPath, MeasurementGroup> measurementGroupMapWithProps = new HashMap<>();
      measurementGroupMapWithProps.put(
          new PartialPath("root.db.d2"), createMeasurementGroupWithProps("s1"));
      Assert.assertFalse(
          batch.onEvent(
              createEvent(
                  new CreateMultiTimeSeriesNode(new PlanNodeId("2"), measurementGroupMapWithProps),
                  "pipeA",
                  1L)));

      Assert.assertTrue(
          batch.onEvent(
              createEvent(
                  new CreateTimeSeriesNode(
                      new PlanNodeId("3"),
                      new MeasurementPath("root.db.d3.s1"),
                      TSDataType.INT64,
                      TSEncoding.PLAIN,
                      CompressionType.LZ4,
                      null,
                      null,
                      null,
                      null),
                  "pipeA",
                  1L)));

      Assert.assertFalse(
          batch.onEvent(
              createEvent(
                  new CreateAlignedTimeSeriesNode(
                      new PlanNodeId("4"),
                      new PartialPath("root.db.d3"),
                      Collections.singletonList("s2"),
                      Collections.singletonList(TSDataType.INT32),
                      Collections.singletonList(TSEncoding.RLE),
                      Collections.singletonList(CompressionType.SNAPPY),
                      null,
                      null,
                      null),
                  "pipeA",
                  1L)));

      Assert.assertFalse(
          batch.onEvent(
              createEvent(
                  new CreateTimeSeriesNode(
                      new PlanNodeId("5"),
                      new MeasurementPath("root.db.d4.s1"),
                      TSDataType.INT64,
                      TSEncoding.PLAIN,
                      CompressionType.LZ4,
                      null,
                      null,
                      null,
                      null),
                  "pipeB",
                  1L)));
    }
  }

  @Test
  public void testDiscardEventsOfPipeRebuildsBatchAndResetsEmitWindow() throws Exception {
    try (PipeSchemaRegionWritePlanEventBatch batch =
        new PipeSchemaRegionWritePlanEventBatch(createParameters(10000, 1048576))) {
      final PipeSchemaRegionWritePlanEvent removedEvent =
          createEvent(
              new CreateTimeSeriesNode(
                  new PlanNodeId("1"),
                  new MeasurementPath("root.db.d1.s1"),
                  TSDataType.INT64,
                  TSEncoding.PLAIN,
                  CompressionType.LZ4,
                  null,
                  null,
                  null,
                  null),
              "pipeA",
              1L,
              1);
      final PipeSchemaRegionWritePlanEvent remainingEvent =
          createEvent(
              new CreateTimeSeriesNode(
                  new PlanNodeId("2"),
                  new MeasurementPath("root.db.d2.s1"),
                  TSDataType.INT64,
                  TSEncoding.PLAIN,
                  CompressionType.LZ4,
                  null,
                  null,
                  null,
                  null),
              "pipeA",
              1L,
              2);

      Assert.assertTrue(batch.onEvent(removedEvent));
      Assert.assertTrue(batch.onEvent(remainingEvent));

      setField(batch, "firstEventProcessingTime", System.currentTimeMillis() - 20000);
      Assert.assertTrue(batch.shouldEmit());

      batch.discardEventsOfPipe("pipeA", 1L, 1);

      Assert.assertTrue(removedEvent.isReleased());
      Assert.assertFalse(remainingEvent.isReleased());
      Assert.assertEquals(1, batch.size());
      Assert.assertEquals("pipeA", batch.getPipeName());
      Assert.assertEquals(1L, batch.getCreationTime());
      Assert.assertFalse(batch.shouldEmit());

      final PlanNode planNode = batch.toPlanNode();
      final Map<PartialPath, Pair<Boolean, MeasurementGroup>> deviceMap =
          ((InternalCreateMultiTimeSeriesNode) planNode).getDeviceMap();
      Assert.assertEquals(1, deviceMap.size());
      Assert.assertTrue(deviceMap.containsKey(new PartialPath("root.db.d2")));
    }
  }

  @Test
  public void testRecordMetricsAndCloseReleaseEvents() throws Exception {
    try (PipeSchemaRegionWritePlanEventBatch batch =
        new PipeSchemaRegionWritePlanEventBatch(createParameters(1000, 1048576))) {
      final Histogram batchSizeHistogram = Mockito.mock(Histogram.class);
      final Histogram batchTimeIntervalHistogram = Mockito.mock(Histogram.class);
      final Histogram eventSizeHistogram = Mockito.mock(Histogram.class);
      batch.setBatchSizeHistogram(batchSizeHistogram);
      batch.setBatchTimeIntervalHistogram(batchTimeIntervalHistogram);
      batch.setEventSizeHistogram(eventSizeHistogram);

      final PipeSchemaRegionWritePlanEvent event =
          createEvent(
              new CreateTimeSeriesNode(
                  new PlanNodeId("1"),
                  new MeasurementPath("root.db.d1.s1"),
                  TSDataType.INT64,
                  TSEncoding.PLAIN,
                  CompressionType.LZ4,
                  null,
                  null,
                  null,
                  null),
              "pipeA",
              1L);

      Assert.assertTrue(batch.onEvent(event));
      batch.recordBatchMetrics();

      verify(batchTimeIntervalHistogram, times(1)).update(anyLong());
      verify(batchSizeHistogram, times(1)).update(anyLong());
      verify(eventSizeHistogram, times(1)).update(1L);

      batch.close();
      Assert.assertTrue(event.isReleased());
    }
  }

  private PipeParameters createParameters(final int delayInMs, final long batchSizeInBytes) {
    return new PipeParameters(
        new HashMap<String, String>() {
          {
            put(CONNECTOR_IOTDB_BATCH_DELAY_MS_KEY, String.valueOf(delayInMs));
            put(CONNECTOR_IOTDB_BATCH_SIZE_KEY, String.valueOf(batchSizeInBytes));
          }
        });
  }

  private MeasurementGroup createMeasurementGroup(final String measurement, final String alias) {
    final MeasurementGroup measurementGroup = new MeasurementGroup();
    measurementGroup.addMeasurement(
        measurement, TSDataType.INT64, TSEncoding.PLAIN, CompressionType.LZ4);
    measurementGroup.addAlias(alias);
    measurementGroup.addTags(Collections.singletonMap("tag", alias));
    measurementGroup.addAttributes(Collections.singletonMap("attr", alias));
    return measurementGroup;
  }

  private MeasurementGroup createMeasurementGroupWithProps(final String measurement) {
    final MeasurementGroup measurementGroup = new MeasurementGroup();
    measurementGroup.addMeasurement(
        measurement, TSDataType.INT64, TSEncoding.PLAIN, CompressionType.LZ4);
    measurementGroup.addProps(Collections.singletonMap("prop", "v1"));
    return measurementGroup;
  }

  private PipeSchemaRegionWritePlanEvent createEvent(
      final PlanNode planNode, final String pipeName, final long creationTime) {
    return createEvent(planNode, pipeName, creationTime, -1);
  }

  private PipeSchemaRegionWritePlanEvent createEvent(
      final PlanNode planNode, final String pipeName, final long creationTime, final int regionId) {
    final PipeSchemaRegionWritePlanEvent event =
        new PipeSchemaRegionWritePlanEvent(planNode, pipeName, creationTime, null, null, true);
    event.setCommitterKeyAndCommitId(new CommitterKey(pipeName, creationTime, regionId, -1), 1L);
    return event;
  }

  private void setField(final Object target, final String fieldName, final Object value)
      throws Exception {
    final Field field = target.getClass().getDeclaredField(fieldName);
    field.setAccessible(true);
    field.set(target, value);
  }
}
