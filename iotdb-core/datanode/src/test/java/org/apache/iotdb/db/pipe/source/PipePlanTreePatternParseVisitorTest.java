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

package org.apache.iotdb.db.pipe.source;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.pipe.datastructure.pattern.IoTDBTreePattern;
import org.apache.iotdb.commons.pipe.datastructure.pattern.IoTDBTreePatternOperations;
import org.apache.iotdb.commons.pipe.datastructure.pattern.UnionIoTDBTreePattern;
import org.apache.iotdb.commons.schema.view.viewExpression.ViewExpression;
import org.apache.iotdb.commons.schema.view.viewExpression.leaf.TimeSeriesViewOperand;
import org.apache.iotdb.db.pipe.source.schemaregion.IoTDBSchemaRegionSource;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.write.ActivateTemplateNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.write.AlterTimeSeriesNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.write.BatchActivateTemplateNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.write.CreateAlignedTimeSeriesNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.write.CreateMultiTimeSeriesNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.write.CreateTimeSeriesNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.write.InternalBatchActivateTemplateNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.write.InternalCreateMultiTimeSeriesNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.write.InternalCreateTimeSeriesNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.write.MeasurementGroup;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.write.view.AlterLogicalViewNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.write.view.CreateLogicalViewNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.DeleteDataNode;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.AlterTimeSeriesStatement;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.utils.Pair;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class PipePlanTreePatternParseVisitorTest {

  private final IoTDBTreePatternOperations prefixPathPattern =
      new UnionIoTDBTreePattern(new IoTDBTreePattern("root.db.device.**"));
  private final IoTDBTreePatternOperations fullPathPattern =
      new UnionIoTDBTreePattern(new IoTDBTreePattern("root.db.device.s1"));
  private final IoTDBTreePatternOperations multiplePathPattern =
      new UnionIoTDBTreePattern(
          Arrays.asList(
              new IoTDBTreePattern("root.db.device.s1"),
              new IoTDBTreePattern("root.db.device.s2")));

  @Test
  public void testCreateTimeSeries() throws IllegalPathException {
    final CreateTimeSeriesNode createTimeSeriesNode =
        new CreateTimeSeriesNode(
            new PlanNodeId("2024-04-30-1"),
            new MeasurementPath("root.db.device.s1"),
            TSDataType.FLOAT,
            TSEncoding.RLE,
            CompressionType.SNAPPY,
            Collections.emptyMap(),
            Collections.emptyMap(),
            Collections.emptyMap(),
            "a1");
    final CreateTimeSeriesNode createTimeSeriesNodeToFilter =
        new CreateTimeSeriesNode(
            new PlanNodeId("2024-04-30-2"),
            new MeasurementPath("root.db1.device.s1"),
            TSDataType.FLOAT,
            TSEncoding.RLE,
            CompressionType.SNAPPY,
            Collections.emptyMap(),
            Collections.emptyMap(),
            Collections.emptyMap(),
            "a1");
    final CreateTimeSeriesNode createTimeSeriesNode2 =
        new CreateTimeSeriesNode(
            new PlanNodeId("2024-04-30-3"),
            new MeasurementPath("root.db.device.s2"),
            TSDataType.FLOAT,
            TSEncoding.RLE,
            CompressionType.SNAPPY,
            Collections.emptyMap(),
            Collections.emptyMap(),
            Collections.emptyMap(),
            "a2");

    Assert.assertEquals(
        createTimeSeriesNode,
        IoTDBSchemaRegionSource.TREE_PATTERN_PARSE_VISITOR
            .visitCreateTimeSeries(createTimeSeriesNode, prefixPathPattern)
            .orElseThrow(AssertionError::new));
    Assert.assertFalse(
        IoTDBSchemaRegionSource.TREE_PATTERN_PARSE_VISITOR
            .visitCreateTimeSeries(createTimeSeriesNodeToFilter, prefixPathPattern)
            .isPresent());

    Assert.assertEquals(
        createTimeSeriesNode,
        IoTDBSchemaRegionSource.TREE_PATTERN_PARSE_VISITOR
            .visitCreateTimeSeries(createTimeSeriesNode, multiplePathPattern)
            .orElseThrow(AssertionError::new));
    Assert.assertEquals(
        createTimeSeriesNode2,
        IoTDBSchemaRegionSource.TREE_PATTERN_PARSE_VISITOR
            .visitCreateTimeSeries(createTimeSeriesNode2, multiplePathPattern)
            .orElseThrow(AssertionError::new));
    Assert.assertFalse(
        IoTDBSchemaRegionSource.TREE_PATTERN_PARSE_VISITOR
            .visitCreateTimeSeries(createTimeSeriesNodeToFilter, multiplePathPattern)
            .isPresent());
  }

  @Test
  public void testCreateAlignedTimeSeries() throws IllegalPathException {
    Assert.assertEquals(
        new CreateAlignedTimeSeriesNode(
            new PlanNodeId("2024-04-30-1"),
            new PartialPath("root.db.device"),
            Collections.singletonList("s1"),
            Collections.singletonList(TSDataType.FLOAT),
            Collections.singletonList(TSEncoding.RLE),
            Collections.singletonList(CompressionType.SNAPPY),
            Collections.singletonList("a1"),
            Collections.singletonList(Collections.emptyMap()),
            Collections.singletonList(Collections.emptyMap())),
        IoTDBSchemaRegionSource.TREE_PATTERN_PARSE_VISITOR
            .visitCreateAlignedTimeSeries(
                new CreateAlignedTimeSeriesNode(
                    new PlanNodeId("2024-04-30-1"),
                    new PartialPath("root.db.device"),
                    Arrays.asList("s1", "s2"),
                    Arrays.asList(TSDataType.FLOAT, TSDataType.BOOLEAN),
                    Arrays.asList(TSEncoding.RLE, TSEncoding.PLAIN),
                    Arrays.asList(CompressionType.SNAPPY, CompressionType.SNAPPY),
                    Arrays.asList("a1", "a2"),
                    Arrays.asList(Collections.emptyMap(), Collections.emptyMap()),
                    Arrays.asList(Collections.emptyMap(), Collections.emptyMap())),
                fullPathPattern)
            .orElseThrow(AssertionError::new));

    Assert.assertEquals(
        new CreateAlignedTimeSeriesNode(
            new PlanNodeId("2024-04-30-2"),
            new PartialPath("root.db.device"),
            Arrays.asList("s1", "s2"),
            Arrays.asList(TSDataType.FLOAT, TSDataType.BOOLEAN),
            Arrays.asList(TSEncoding.RLE, TSEncoding.PLAIN),
            Arrays.asList(CompressionType.SNAPPY, CompressionType.SNAPPY),
            Arrays.asList("a1", "a2"),
            Arrays.asList(Collections.emptyMap(), Collections.emptyMap()),
            Arrays.asList(Collections.emptyMap(), Collections.emptyMap())),
        IoTDBSchemaRegionSource.TREE_PATTERN_PARSE_VISITOR
            .visitCreateAlignedTimeSeries(
                new CreateAlignedTimeSeriesNode(
                    new PlanNodeId("2024-04-30-2"),
                    new PartialPath("root.db.device"),
                    Arrays.asList("s1", "s2", "s3"),
                    Arrays.asList(TSDataType.FLOAT, TSDataType.BOOLEAN, TSDataType.INT32),
                    Arrays.asList(TSEncoding.RLE, TSEncoding.PLAIN, TSEncoding.RLE),
                    Arrays.asList(
                        CompressionType.SNAPPY, CompressionType.SNAPPY, CompressionType.SNAPPY),
                    Arrays.asList("a1", "a2", "a3"),
                    Arrays.asList(
                        Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap()),
                    Arrays.asList(
                        Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap())),
                multiplePathPattern)
            .orElseThrow(AssertionError::new));
  }

  @Test
  public void testCreateMultiTimeSeries() throws IllegalPathException {
    Assert.assertEquals(
        new CreateMultiTimeSeriesNode(
            new PlanNodeId("2024-04-30-1"),
            Collections.singletonList(new MeasurementPath("root.db.device.s1")),
            Collections.singletonList(TSDataType.FLOAT),
            Collections.singletonList(TSEncoding.RLE),
            Collections.singletonList(CompressionType.SNAPPY),
            Collections.singletonList(Collections.emptyMap()),
            Collections.singletonList("a1"),
            Collections.singletonList(Collections.emptyMap()),
            Collections.singletonList(Collections.emptyMap())),
        IoTDBSchemaRegionSource.TREE_PATTERN_PARSE_VISITOR
            .visitCreateMultiTimeSeries(
                new CreateMultiTimeSeriesNode(
                    new PlanNodeId("2024-04-30-1"),
                    Arrays.asList(
                        new MeasurementPath("root.db.device.s1"),
                        new MeasurementPath("root.db1.device.s1")),
                    Arrays.asList(TSDataType.FLOAT, TSDataType.BOOLEAN),
                    Arrays.asList(TSEncoding.RLE, TSEncoding.PLAIN),
                    Arrays.asList(CompressionType.SNAPPY, CompressionType.SNAPPY),
                    Arrays.asList(Collections.emptyMap(), Collections.emptyMap()),
                    Arrays.asList("a1", "a2"),
                    Arrays.asList(Collections.emptyMap(), Collections.emptyMap()),
                    Arrays.asList(Collections.emptyMap(), Collections.emptyMap())),
                fullPathPattern)
            .orElseThrow(AssertionError::new));

    Assert.assertEquals(
        new CreateMultiTimeSeriesNode(
            new PlanNodeId("2024-04-30-2"),
            Arrays.asList(
                new MeasurementPath("root.db.device.s1"), new MeasurementPath("root.db.device.s2")),
            Arrays.asList(TSDataType.FLOAT, TSDataType.BOOLEAN),
            Arrays.asList(TSEncoding.RLE, TSEncoding.PLAIN),
            Arrays.asList(CompressionType.SNAPPY, CompressionType.SNAPPY),
            Arrays.asList(Collections.emptyMap(), Collections.emptyMap()),
            Arrays.asList("a1", "a2"),
            Arrays.asList(Collections.emptyMap(), Collections.emptyMap()),
            Arrays.asList(Collections.emptyMap(), Collections.emptyMap())),
        IoTDBSchemaRegionSource.TREE_PATTERN_PARSE_VISITOR
            .visitCreateMultiTimeSeries(
                new CreateMultiTimeSeriesNode(
                    new PlanNodeId("2024-04-30-2"),
                    Arrays.asList(
                        new MeasurementPath("root.db.device.s1"),
                        new MeasurementPath("root.db.device.s2"),
                        new MeasurementPath("root.db1.device.s1")),
                    Arrays.asList(TSDataType.FLOAT, TSDataType.BOOLEAN, TSDataType.INT32),
                    Arrays.asList(TSEncoding.RLE, TSEncoding.PLAIN, TSEncoding.RLE),
                    Arrays.asList(
                        CompressionType.SNAPPY, CompressionType.SNAPPY, CompressionType.SNAPPY),
                    Arrays.asList(
                        Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap()),
                    Arrays.asList("a1", "a2", "a3"),
                    Arrays.asList(
                        Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap()),
                    Arrays.asList(
                        Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap())),
                multiplePathPattern)
            .orElseThrow(AssertionError::new));
  }

  @Test
  public void testAlterTimeSeries() throws IllegalPathException {
    final Map<String, String> attributesMap = Collections.singletonMap("k1", "v1");
    final AlterTimeSeriesNode alterTimeSeriesNode =
        new AlterTimeSeriesNode(
            new PlanNodeId("2024-04-30-1"),
            new MeasurementPath("root.db.device.s1"),
            AlterTimeSeriesStatement.AlterType.ADD_ATTRIBUTES,
            attributesMap,
            "",
            Collections.emptyMap(),
            attributesMap,
            false);
    final AlterTimeSeriesNode alterTimeSeriesNodeToFilter =
        new AlterTimeSeriesNode(
            new PlanNodeId("2024-04-30-2"),
            new MeasurementPath("root.db1.device.s1"),
            AlterTimeSeriesStatement.AlterType.ADD_ATTRIBUTES,
            attributesMap,
            "",
            Collections.emptyMap(),
            attributesMap,
            false);
    final AlterTimeSeriesNode alterTimeSeriesNode2 =
        new AlterTimeSeriesNode(
            new PlanNodeId("2024-04-30-3"),
            new MeasurementPath("root.db.device.s2"),
            AlterTimeSeriesStatement.AlterType.ADD_ATTRIBUTES,
            attributesMap,
            "",
            Collections.emptyMap(),
            attributesMap,
            false);

    Assert.assertEquals(
        alterTimeSeriesNode,
        IoTDBSchemaRegionSource.TREE_PATTERN_PARSE_VISITOR
            .visitAlterTimeSeries(alterTimeSeriesNode, prefixPathPattern)
            .orElseThrow(AssertionError::new));
    Assert.assertFalse(
        IoTDBSchemaRegionSource.TREE_PATTERN_PARSE_VISITOR
            .visitAlterTimeSeries(alterTimeSeriesNodeToFilter, prefixPathPattern)
            .isPresent());

    Assert.assertEquals(
        alterTimeSeriesNode,
        IoTDBSchemaRegionSource.TREE_PATTERN_PARSE_VISITOR
            .visitAlterTimeSeries(alterTimeSeriesNode, multiplePathPattern)
            .orElseThrow(AssertionError::new));
    Assert.assertEquals(
        alterTimeSeriesNode2,
        IoTDBSchemaRegionSource.TREE_PATTERN_PARSE_VISITOR
            .visitAlterTimeSeries(alterTimeSeriesNode2, multiplePathPattern)
            .orElseThrow(AssertionError::new));
    Assert.assertFalse(
        IoTDBSchemaRegionSource.TREE_PATTERN_PARSE_VISITOR
            .visitAlterTimeSeries(alterTimeSeriesNodeToFilter, multiplePathPattern)
            .isPresent());
  }

  @Test
  public void testInternalCreateTimeSeries() throws IllegalPathException {
    final MeasurementGroup expectedMeasurementGroup = new MeasurementGroup();
    expectedMeasurementGroup.addMeasurement(
        "s1", TSDataType.FLOAT, TSEncoding.RLE, CompressionType.SNAPPY);
    expectedMeasurementGroup.addProps(Collections.emptyMap());
    expectedMeasurementGroup.addAlias("a1");
    expectedMeasurementGroup.addTags(Collections.emptyMap());
    expectedMeasurementGroup.addAttributes(Collections.emptyMap());

    final MeasurementGroup originalMeasurementGroup = new MeasurementGroup();
    originalMeasurementGroup.addMeasurement(
        "s1", TSDataType.FLOAT, TSEncoding.RLE, CompressionType.SNAPPY);
    originalMeasurementGroup.addProps(Collections.emptyMap());
    originalMeasurementGroup.addAlias("a1");
    originalMeasurementGroup.addTags(Collections.emptyMap());
    originalMeasurementGroup.addAttributes(Collections.emptyMap());
    originalMeasurementGroup.addMeasurement(
        "s2", TSDataType.BOOLEAN, TSEncoding.PLAIN, CompressionType.SNAPPY);
    originalMeasurementGroup.addProps(Collections.emptyMap());
    originalMeasurementGroup.addAlias("a2");
    originalMeasurementGroup.addTags(Collections.emptyMap());
    originalMeasurementGroup.addAttributes(Collections.emptyMap());

    Assert.assertEquals(
        new InternalCreateTimeSeriesNode(
            new PlanNodeId("2024-04-30-1"),
            new PartialPath("root.db.device"),
            expectedMeasurementGroup,
            true),
        IoTDBSchemaRegionSource.TREE_PATTERN_PARSE_VISITOR
            .visitInternalCreateTimeSeries(
                new InternalCreateTimeSeriesNode(
                    new PlanNodeId("2024-04-30-1"),
                    new PartialPath("root.db.device"),
                    originalMeasurementGroup,
                    true),
                fullPathPattern)
            .orElseThrow(AssertionError::new));

    Assert.assertEquals(
        new InternalCreateTimeSeriesNode(
            new PlanNodeId("2024-04-30-2"),
            new PartialPath("root.db.device"),
            originalMeasurementGroup,
            true),
        IoTDBSchemaRegionSource.TREE_PATTERN_PARSE_VISITOR
            .visitInternalCreateTimeSeries(
                new InternalCreateTimeSeriesNode(
                    new PlanNodeId("2024-04-30-2"),
                    new PartialPath("root.db.device"),
                    originalMeasurementGroup,
                    true),
                multiplePathPattern)
            .orElseThrow(AssertionError::new));
  }

  @Test
  public void testActivateTemplate() throws IllegalPathException {
    final ActivateTemplateNode activateTemplateNode =
        new ActivateTemplateNode(
            new PlanNodeId("2024-04-30-1"), new PartialPath("root.db.device"), 3, 1);
    final ActivateTemplateNode activateTemplateNodeToFilter =
        new ActivateTemplateNode(new PlanNodeId("2024-04-30-2"), new PartialPath("root.db"), 2, 1);

    Assert.assertEquals(
        activateTemplateNode,
        IoTDBSchemaRegionSource.TREE_PATTERN_PARSE_VISITOR
            .visitActivateTemplate(activateTemplateNode, prefixPathPattern)
            .orElseThrow(AssertionError::new));
    Assert.assertFalse(
        IoTDBSchemaRegionSource.TREE_PATTERN_PARSE_VISITOR
            .visitActivateTemplate(activateTemplateNodeToFilter, prefixPathPattern)
            .isPresent());

    Assert.assertEquals(
        activateTemplateNode,
        IoTDBSchemaRegionSource.TREE_PATTERN_PARSE_VISITOR
            .visitActivateTemplate(activateTemplateNode, multiplePathPattern)
            .orElseThrow(AssertionError::new));
    Assert.assertFalse(
        IoTDBSchemaRegionSource.TREE_PATTERN_PARSE_VISITOR
            .visitActivateTemplate(activateTemplateNodeToFilter, multiplePathPattern)
            .isPresent());
  }

  @Test
  public void testInternalBatchActivateTemplate() throws IllegalPathException {
    Assert.assertEquals(
        new InternalBatchActivateTemplateNode(
            new PlanNodeId("2024-04-30-1"),
            Collections.singletonMap(new PartialPath("root.db.device"), new Pair<>(1, 1))),
        IoTDBSchemaRegionSource.TREE_PATTERN_PARSE_VISITOR
            .visitInternalBatchActivateTemplate(
                new InternalBatchActivateTemplateNode(
                    new PlanNodeId("2024-04-30-1"),
                    new HashMap<PartialPath, Pair<Integer, Integer>>() {
                      {
                        put(new PartialPath("root.db.device"), new Pair<>(1, 1));
                        put(new PartialPath("root.db"), new Pair<>(2, 2));
                      }
                    }),
                fullPathPattern)
            .orElseThrow(AssertionError::new));

    Assert.assertEquals(
        new InternalBatchActivateTemplateNode(
            new PlanNodeId("2024-04-30-2"),
            Collections.singletonMap(new PartialPath("root.db.device"), new Pair<>(1, 1))),
        IoTDBSchemaRegionSource.TREE_PATTERN_PARSE_VISITOR
            .visitInternalBatchActivateTemplate(
                new InternalBatchActivateTemplateNode(
                    new PlanNodeId("2024-04-30-2"),
                    new HashMap<PartialPath, Pair<Integer, Integer>>() {
                      {
                        put(new PartialPath("root.db.device"), new Pair<>(1, 1));
                        put(new PartialPath("root.db1"), new Pair<>(2, 2));
                      }
                    }),
                multiplePathPattern)
            .orElseThrow(AssertionError::new));
  }

  @Test
  public void testInternalCreateMultiTimeSeries() throws IllegalPathException {
    final MeasurementGroup expectedMeasurementGroup = new MeasurementGroup();
    expectedMeasurementGroup.addMeasurement(
        "s1", TSDataType.FLOAT, TSEncoding.RLE, CompressionType.SNAPPY);
    expectedMeasurementGroup.addProps(Collections.emptyMap());
    expectedMeasurementGroup.addAlias("a1");
    expectedMeasurementGroup.addTags(Collections.emptyMap());
    expectedMeasurementGroup.addAttributes(Collections.emptyMap());

    final MeasurementGroup originalMeasurementGroup = new MeasurementGroup();
    originalMeasurementGroup.addMeasurement(
        "s1", TSDataType.FLOAT, TSEncoding.RLE, CompressionType.SNAPPY);
    originalMeasurementGroup.addProps(Collections.emptyMap());
    originalMeasurementGroup.addAlias("a1");
    originalMeasurementGroup.addTags(Collections.emptyMap());
    originalMeasurementGroup.addAttributes(Collections.emptyMap());
    originalMeasurementGroup.addMeasurement(
        "s2", TSDataType.BOOLEAN, TSEncoding.PLAIN, CompressionType.SNAPPY);
    originalMeasurementGroup.addProps(Collections.emptyMap());
    originalMeasurementGroup.addAlias("a2");
    originalMeasurementGroup.addTags(Collections.emptyMap());
    originalMeasurementGroup.addAttributes(Collections.emptyMap());

    final MeasurementGroup anotherMeasurementGroup = new MeasurementGroup();
    anotherMeasurementGroup.addMeasurement(
        "s3", TSDataType.INT32, TSEncoding.RLE, CompressionType.SNAPPY);

    Assert.assertEquals(
        new InternalCreateMultiTimeSeriesNode(
            new PlanNodeId("2024-04-30-1"),
            Collections.singletonMap(
                new PartialPath("root.db.device"), new Pair<>(false, expectedMeasurementGroup))),
        IoTDBSchemaRegionSource.TREE_PATTERN_PARSE_VISITOR
            .visitInternalCreateMultiTimeSeries(
                new InternalCreateMultiTimeSeriesNode(
                    new PlanNodeId("2024-04-30-1"),
                    new HashMap<PartialPath, Pair<Boolean, MeasurementGroup>>() {
                      {
                        put(
                            new PartialPath("root.db.device"),
                            new Pair<>(false, originalMeasurementGroup));
                        put(
                            new PartialPath("root.db1.device"),
                            new Pair<>(false, originalMeasurementGroup));
                      }
                    }),
                fullPathPattern)
            .orElseThrow(AssertionError::new));

    Assert.assertEquals(
        new InternalCreateMultiTimeSeriesNode(
            new PlanNodeId("2024-04-30-2"),
            Collections.singletonMap(
                new PartialPath("root.db.device"), new Pair<>(false, originalMeasurementGroup))),
        IoTDBSchemaRegionSource.TREE_PATTERN_PARSE_VISITOR
            .visitInternalCreateMultiTimeSeries(
                new InternalCreateMultiTimeSeriesNode(
                    new PlanNodeId("2024-04-30-2"),
                    new HashMap<PartialPath, Pair<Boolean, MeasurementGroup>>() {
                      {
                        put(
                            new PartialPath("root.db.device"),
                            new Pair<>(false, originalMeasurementGroup));
                        put(
                            new PartialPath("root.db1.device"),
                            new Pair<>(false, anotherMeasurementGroup));
                      }
                    }),
                multiplePathPattern)
            .orElseThrow(AssertionError::new));
  }

  @Test
  public void testBatchActivateTemplate() throws IllegalPathException {
    Assert.assertEquals(
        new BatchActivateTemplateNode(
            new PlanNodeId("2024-04-30-1"),
            Collections.singletonMap(new PartialPath("root.db.device"), new Pair<>(1, 1))),
        IoTDBSchemaRegionSource.TREE_PATTERN_PARSE_VISITOR
            .visitBatchActivateTemplate(
                new BatchActivateTemplateNode(
                    new PlanNodeId("2024-04-30-1"),
                    new HashMap<PartialPath, Pair<Integer, Integer>>() {
                      {
                        put(new PartialPath("root.db.device"), new Pair<>(1, 1));
                        put(new PartialPath("root.db"), new Pair<>(2, 2));
                      }
                    }),
                fullPathPattern)
            .orElseThrow(AssertionError::new));

    Assert.assertEquals(
        new BatchActivateTemplateNode(
            new PlanNodeId("2024-04-30-2"),
            Collections.singletonMap(new PartialPath("root.db.device"), new Pair<>(1, 1))),
        IoTDBSchemaRegionSource.TREE_PATTERN_PARSE_VISITOR
            .visitBatchActivateTemplate(
                new BatchActivateTemplateNode(
                    new PlanNodeId("2024-04-30-2"),
                    new HashMap<PartialPath, Pair<Integer, Integer>>() {
                      {
                        put(new PartialPath("root.db.device"), new Pair<>(1, 1));
                        put(new PartialPath("root.db"), new Pair<>(2, 2));
                      }
                    }),
                multiplePathPattern)
            .orElseThrow(AssertionError::new));
  }

  @Test
  public void testCreateLogicalView() throws IllegalPathException {
    Assert.assertEquals(
        new CreateLogicalViewNode(
            new PlanNodeId("2024-04-30-1"),
            Collections.singletonMap(
                new PartialPath("root.db.device.a1"), new TimeSeriesViewOperand("root.sg1.d1"))),
        IoTDBSchemaRegionSource.TREE_PATTERN_PARSE_VISITOR
            .visitCreateLogicalView(
                new CreateLogicalViewNode(
                    new PlanNodeId("2024-04-30-1"),
                    new HashMap<PartialPath, ViewExpression>() {
                      {
                        put(
                            new PartialPath("root.db.device.a1"),
                            new TimeSeriesViewOperand("root.sg1.d1"));
                        put(
                            new PartialPath("root.db1.device.a1"),
                            new TimeSeriesViewOperand("root.sg1.d2"));
                      }
                    }),
                prefixPathPattern)
            .orElseThrow(AssertionError::new));
  }

  @Test
  public void testAlterLogicalView() throws IllegalPathException {
    Assert.assertEquals(
        new AlterLogicalViewNode(
            new PlanNodeId("2024-04-30-1"),
            Collections.singletonMap(
                new PartialPath("root.db.device.a1"), new TimeSeriesViewOperand("root.sg1.d1"))),
        IoTDBSchemaRegionSource.TREE_PATTERN_PARSE_VISITOR
            .visitAlterLogicalView(
                new AlterLogicalViewNode(
                    new PlanNodeId("2024-04-30-1"),
                    new HashMap<PartialPath, ViewExpression>() {
                      {
                        put(
                            new PartialPath("root.db.device.a1"),
                            new TimeSeriesViewOperand("root.sg1.d1"));
                        put(
                            new PartialPath("root.db1.device.a1"),
                            new TimeSeriesViewOperand("root.sg1.d2"));
                      }
                    }),
                prefixPathPattern)
            .orElseThrow(AssertionError::new));
  }

  @Test
  public void testDeleteData() throws IllegalPathException {
    Assert.assertEquals(
        new DeleteDataNode(
            new PlanNodeId("2024-04-30-1"),
            Collections.singletonList(new MeasurementPath("root.db.device.s1")),
            Long.MIN_VALUE,
            Long.MAX_VALUE),
        IoTDBSchemaRegionSource.TREE_PATTERN_PARSE_VISITOR
            .visitDeleteData(
                new DeleteDataNode(
                    new PlanNodeId("2024-04-30-1"),
                    Arrays.asList(
                        new MeasurementPath("root.*.device.s1"),
                        new MeasurementPath("root.db.*.s1")),
                    Long.MIN_VALUE,
                    Long.MAX_VALUE),
                prefixPathPattern)
            .orElseThrow(AssertionError::new));

    Assert.assertEquals(
        new DeleteDataNode(
            new PlanNodeId("2024-04-30-2"),
            Arrays.asList(
                new MeasurementPath("root.db.device.s1"), new MeasurementPath("root.db.device.s2")),
            Long.MIN_VALUE,
            Long.MAX_VALUE),
        IoTDBSchemaRegionSource.TREE_PATTERN_PARSE_VISITOR
            .visitDeleteData(
                new DeleteDataNode(
                    new PlanNodeId("2024-04-30-2"),
                    Arrays.asList(
                        new MeasurementPath("root.db.device.s1"),
                        new MeasurementPath("root.db.device.s2"),
                        new MeasurementPath("root.db.device.s3")),
                    Long.MIN_VALUE,
                    Long.MAX_VALUE),
                multiplePathPattern)
            .orElseThrow(AssertionError::new));
  }
}
