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

package org.timecho.iotdb.db.pipe;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.pipe.datastructure.pattern.IoTDBTreePattern;
import org.apache.iotdb.commons.pipe.datastructure.pattern.IoTDBTreePatternOperations;
import org.apache.iotdb.commons.pipe.datastructure.pattern.UnionIoTDBTreePattern;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.commons.schema.utils.MeasurementPropsUtils;
import org.apache.iotdb.db.pipe.event.common.schema.PipeSchemaRegionPlanUtil;
import org.apache.iotdb.db.pipe.receiver.visitor.PipeStatementTreePatternParseVisitor;
import org.apache.iotdb.db.pipe.source.schemaregion.IoTDBSchemaRegionSource;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.write.CreateTimeSeriesNode;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.AlterTimeSeriesStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.CreateAlignedTimeSeriesStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.CreateTimeSeriesStatement;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class TimechoPipeRenamedSeriesSchemaRegionFilterTest {

  private final IoTDBTreePatternOperations prefixPathPattern =
      new UnionIoTDBTreePattern(new IoTDBTreePattern("root.db.device.**"));

  @Test
  public void testRealtimeSourceCreateInvalidPhysicalTimeSeriesIsFiltered()
      throws IllegalPathException {
    Assert.assertFalse(
        IoTDBSchemaRegionSource.TREE_PATTERN_PARSE_VISITOR
            .visitCreateTimeSeries(
                new CreateTimeSeriesNode(
                    new PlanNodeId("2026-07-07-1"),
                    new MeasurementPath("root.db.device.s_physical"),
                    TSDataType.FLOAT,
                    TSEncoding.RLE,
                    CompressionType.SNAPPY,
                    invalidPhysicalProps(),
                    Collections.emptyMap(),
                    Collections.emptyMap(),
                    null),
                prefixPathPattern)
            .isPresent());
  }

  @Test
  public void testHistoricalSnapshotCreateInvalidPhysicalTimeSeriesKeepsSchemaAndSanitizesProps()
      throws IllegalPathException {
    final CreateTimeSeriesStatement invalidPhysicalStatement = new CreateTimeSeriesStatement();
    invalidPhysicalStatement.setPath(new MeasurementPath("root.db.device.s_physical"));
    invalidPhysicalStatement.setDataType(TSDataType.FLOAT);
    invalidPhysicalStatement.setEncoding(TSEncoding.RLE);
    invalidPhysicalStatement.setCompressor(CompressionType.SNAPPY);
    invalidPhysicalStatement.setProps(invalidPhysicalProps());

    Assert.assertTrue(
        PipeSchemaRegionPlanUtil.isInvalidPhysicalSnapshotStatement(invalidPhysicalStatement));

    final CreateTimeSeriesStatement sanitizedStatement =
        (CreateTimeSeriesStatement)
            PipeSchemaRegionPlanUtil.sanitizeRenameInternalStatement(invalidPhysicalStatement)
                .orElseThrow(AssertionError::new);

    Assert.assertEquals(
        new MeasurementPath("root.db.device.s_physical"), sanitizedStatement.getPath());
    Assert.assertNull(sanitizedStatement.getProps());
  }

  @Test
  public void testSourceCreateRenamedAliasTimeSeriesIsFiltered() throws IllegalPathException {
    Assert.assertFalse(
        IoTDBSchemaRegionSource.TREE_PATTERN_PARSE_VISITOR
            .visitCreateTimeSeries(
                new CreateTimeSeriesNode(
                    new PlanNodeId("2026-07-07-2"),
                    new MeasurementPath("root.db.device.s_alias"),
                    TSDataType.FLOAT,
                    TSEncoding.RLE,
                    CompressionType.SNAPPY,
                    renamedAliasProps(),
                    Collections.emptyMap(),
                    Collections.emptyMap(),
                    null),
                prefixPathPattern)
            .isPresent());
  }

  @Test
  public void testSourceCreateRenamingTimeSeriesIsFiltered() throws IllegalPathException {
    Assert.assertFalse(
        IoTDBSchemaRegionSource.TREE_PATTERN_PARSE_VISITOR
            .visitCreateTimeSeries(
                new CreateTimeSeriesNode(
                    new PlanNodeId("2026-07-07-3"),
                    new MeasurementPath("root.db.device.s_renaming"),
                    TSDataType.FLOAT,
                    TSEncoding.RLE,
                    CompressionType.SNAPPY,
                    renamingProps(),
                    Collections.emptyMap(),
                    Collections.emptyMap(),
                    null),
                prefixPathPattern)
            .isPresent());
  }

  @Test
  public void testOrdinaryAliasAlterIsNotFilteredAsRenameInternal() {
    final AlterTimeSeriesStatement statement = new AlterTimeSeriesStatement();
    statement.setAlias("alias");
    statement.setTagsMap(
        Collections.singletonMap(MeasurementPropsUtils.IS_RENAMED_KEY, Boolean.TRUE.toString()));
    statement.setAttributesMap(
        Collections.singletonMap(MeasurementPropsUtils.INVALID_KEY, Boolean.TRUE.toString()));

    Assert.assertFalse(PipeSchemaRegionPlanUtil.isRenameInternalStatement(statement));
  }

  @Test
  public void testReceiverCreateInvalidPhysicalTimeSeriesKeepsSchemaAndSanitizesProps()
      throws IllegalPathException {
    final CreateTimeSeriesStatement invalidPhysicalStatement = new CreateTimeSeriesStatement();
    invalidPhysicalStatement.setPath(new MeasurementPath("root.db.device.s1"));
    invalidPhysicalStatement.setDataType(TSDataType.FLOAT);
    invalidPhysicalStatement.setEncoding(TSEncoding.RLE);
    invalidPhysicalStatement.setCompressor(CompressionType.SNAPPY);
    invalidPhysicalStatement.setProps(invalidPhysicalProps());
    invalidPhysicalStatement.setTags(Collections.emptyMap());
    invalidPhysicalStatement.setAttributes(Collections.emptyMap());
    invalidPhysicalStatement.setAlias("a1");

    final CreateTimeSeriesStatement parsedStatement =
        (CreateTimeSeriesStatement)
            new PipeStatementTreePatternParseVisitor()
                .visitCreateTimeseries(invalidPhysicalStatement, prefixPathPattern)
                .orElseThrow(AssertionError::new);

    Assert.assertEquals(new MeasurementPath("root.db.device.s1"), parsedStatement.getPath());
    Assert.assertNull(parsedStatement.getProps());
  }

  @Test
  public void testReceiverCreateRenamedAliasTimeSeriesIsFiltered() throws IllegalPathException {
    final CreateTimeSeriesStatement renamedAliasStatement = new CreateTimeSeriesStatement();
    renamedAliasStatement.setPath(new MeasurementPath("root.db.device.s1_alias"));
    renamedAliasStatement.setDataType(TSDataType.FLOAT);
    renamedAliasStatement.setEncoding(TSEncoding.RLE);
    renamedAliasStatement.setCompressor(CompressionType.SNAPPY);
    renamedAliasStatement.setProps(renamedAliasProps());

    Assert.assertFalse(
        new PipeStatementTreePatternParseVisitor()
            .visitCreateTimeseries(renamedAliasStatement, prefixPathPattern)
            .isPresent());
  }

  @Test
  public void testReceiverCreateRenamingTimeSeriesIsFiltered() throws IllegalPathException {
    final CreateTimeSeriesStatement renamingStatement = new CreateTimeSeriesStatement();
    renamingStatement.setPath(new MeasurementPath("root.db.device.s_renaming"));
    renamingStatement.setDataType(TSDataType.FLOAT);
    renamingStatement.setEncoding(TSEncoding.RLE);
    renamingStatement.setCompressor(CompressionType.SNAPPY);
    renamingStatement.setProps(renamingProps());

    Assert.assertFalse(
        new PipeStatementTreePatternParseVisitor()
            .visitCreateTimeseries(renamingStatement, prefixPathPattern)
            .isPresent());
  }

  @Test
  public void testReceiverCreateAlignedInvalidPhysicalTimeSeriesKeepsSchemaAndSanitizesProps()
      throws IllegalPathException {
    final CreateAlignedTimeSeriesStatement mixedStatement = new CreateAlignedTimeSeriesStatement();
    mixedStatement.setDevicePath(new PartialPath("root.db.device"));
    mixedStatement.setMeasurements(Arrays.asList("s_physical", "s_alias", "s_renaming"));
    mixedStatement.setDataTypes(
        Arrays.asList(TSDataType.FLOAT, TSDataType.FLOAT, TSDataType.FLOAT));
    mixedStatement.setEncodings(Arrays.asList(TSEncoding.RLE, TSEncoding.RLE, TSEncoding.RLE));
    mixedStatement.setCompressors(
        Arrays.asList(CompressionType.SNAPPY, CompressionType.SNAPPY, CompressionType.SNAPPY));
    mixedStatement.setPropsList(
        Arrays.asList(invalidPhysicalProps(), renamedAliasProps(), renamingProps()));
    mixedStatement.setTagsList(
        Arrays.asList(Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap()));
    mixedStatement.setAttributesList(
        Arrays.asList(Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap()));
    mixedStatement.setAliasList(Arrays.asList(null, null, null));

    final CreateAlignedTimeSeriesStatement parsedStatement =
        (CreateAlignedTimeSeriesStatement)
            new PipeStatementTreePatternParseVisitor()
                .visitCreateAlignedTimeseries(mixedStatement, prefixPathPattern)
                .orElseThrow(AssertionError::new);

    Assert.assertEquals(Collections.singletonList("s_physical"), parsedStatement.getMeasurements());
    Assert.assertEquals(
        Collections.singletonList((Map<String, String>) null), parsedStatement.getPropsList());
  }

  private static Map<String, String> invalidPhysicalProps() {
    final Map<String, String> props = new HashMap<>();
    MeasurementPropsUtils.setInvalid(props, true);
    MeasurementPropsUtils.setAliasPathString(props, "root.db.device.s_alias");
    return props;
  }

  private static Map<String, String> renamedAliasProps() {
    final Map<String, String> props = new HashMap<>();
    MeasurementPropsUtils.setIsRenamed(props, true);
    MeasurementPropsUtils.setOriginalPathString(props, "root.db.device.s_physical");
    return props;
  }

  private static Map<String, String> renamingProps() {
    final Map<String, String> props = new HashMap<>();
    MeasurementPropsUtils.setIsRenaming(props, true);
    return props;
  }
}
