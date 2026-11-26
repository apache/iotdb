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

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.pipe.datastructure.pattern.IoTDBTreePattern;
import org.apache.iotdb.commons.pipe.datastructure.pattern.IoTDBTreePatternOperations;
import org.apache.iotdb.commons.pipe.datastructure.pattern.UnionIoTDBTreePattern;
import org.apache.iotdb.commons.pipe.datastructure.pattern.WithExclusionIoTDBTreePattern;
import org.apache.iotdb.commons.schema.view.viewExpression.leaf.TimeSeriesViewOperand;
import org.apache.iotdb.db.pipe.receiver.visitor.PipeStatementTreePatternParseVisitor;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.AlterTimeSeriesStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.CreateAlignedTimeSeriesStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.CreateTimeSeriesStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.template.ActivateTemplateStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.view.CreateLogicalViewStatement;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

public class PipeStatementTreePatternParseVisitorTest {

  private final IoTDBTreePatternOperations prefixPathPattern =
      new UnionIoTDBTreePattern(new IoTDBTreePattern("root.db.device.**"));
  private final IoTDBTreePatternOperations fullPathPattern =
      new UnionIoTDBTreePattern(new IoTDBTreePattern("root.db.device.s1"));
  private final IoTDBTreePatternOperations multiplePathPattern =
      new UnionIoTDBTreePattern(
          Arrays.asList(
              new IoTDBTreePattern("root.db.device.s1"),
              new IoTDBTreePattern("root.db.device.s2")));
  private final IoTDBTreePatternOperations exclusionPattern =
      new WithExclusionIoTDBTreePattern(
          new UnionIoTDBTreePattern(
              new IoTDBTreePattern("root.db.device.**")), // Inclusion: root.db.device.**
          new UnionIoTDBTreePattern(
              new IoTDBTreePattern("root.db.device.s2")) // Exclusion: root.db.device.s2
          );

  @Test
  public void testCreateTimeSeries() throws IllegalPathException {
    final CreateTimeSeriesStatement createTimeSeriesStatementS1 = new CreateTimeSeriesStatement();
    createTimeSeriesStatementS1.setPath(new MeasurementPath("root.db.device.s1"));
    createTimeSeriesStatementS1.setDataType(TSDataType.FLOAT);
    createTimeSeriesStatementS1.setEncoding(TSEncoding.RLE);
    createTimeSeriesStatementS1.setCompressor(CompressionType.SNAPPY);
    createTimeSeriesStatementS1.setProps(Collections.emptyMap());
    createTimeSeriesStatementS1.setTags(Collections.emptyMap());
    createTimeSeriesStatementS1.setAttributes(Collections.emptyMap());
    createTimeSeriesStatementS1.setAlias("a1");

    final CreateTimeSeriesStatement createTimeSeriesStatementS2 = new CreateTimeSeriesStatement();
    createTimeSeriesStatementS2.setPath(new MeasurementPath("root.db.device.s2"));
    createTimeSeriesStatementS2.setDataType(TSDataType.INT32);
    createTimeSeriesStatementS2.setEncoding(TSEncoding.PLAIN);
    createTimeSeriesStatementS2.setCompressor(CompressionType.SNAPPY);
    createTimeSeriesStatementS2.setProps(Collections.emptyMap());
    createTimeSeriesStatementS2.setTags(Collections.emptyMap());
    createTimeSeriesStatementS2.setAttributes(Collections.emptyMap());
    createTimeSeriesStatementS2.setAlias("a2");

    final CreateTimeSeriesStatement createTimeSeriesStatementToFilter =
        new CreateTimeSeriesStatement();
    createTimeSeriesStatementToFilter.setPath(new MeasurementPath("root.db1.device.s1"));
    createTimeSeriesStatementToFilter.setDataType(TSDataType.FLOAT);
    createTimeSeriesStatementToFilter.setEncoding(TSEncoding.RLE);
    createTimeSeriesStatementToFilter.setCompressor(CompressionType.SNAPPY);
    createTimeSeriesStatementToFilter.setProps(Collections.emptyMap());
    createTimeSeriesStatementToFilter.setTags(Collections.emptyMap());
    createTimeSeriesStatementToFilter.setAttributes(Collections.emptyMap());
    createTimeSeriesStatementToFilter.setAlias("a3");

    Assert.assertEquals(
        createTimeSeriesStatementS1,
        new PipeStatementTreePatternParseVisitor()
            .visitCreateTimeseries(createTimeSeriesStatementS1, prefixPathPattern)
            .orElseThrow(AssertionError::new));
    Assert.assertFalse(
        new PipeStatementTreePatternParseVisitor()
            .visitCreateTimeseries(createTimeSeriesStatementToFilter, prefixPathPattern)
            .isPresent());

    Assert.assertEquals(
        createTimeSeriesStatementS1,
        new PipeStatementTreePatternParseVisitor()
            .visitCreateTimeseries(createTimeSeriesStatementS1, multiplePathPattern)
            .orElseThrow(AssertionError::new));
    Assert.assertFalse(
        new PipeStatementTreePatternParseVisitor()
            .visitCreateTimeseries(createTimeSeriesStatementToFilter, multiplePathPattern)
            .isPresent());

    Assert.assertEquals(
        createTimeSeriesStatementS1,
        new PipeStatementTreePatternParseVisitor()
            .visitCreateTimeseries(createTimeSeriesStatementS1, exclusionPattern)
            .orElseThrow(AssertionError::new));
    Assert.assertFalse(
        new PipeStatementTreePatternParseVisitor()
            .visitCreateTimeseries(createTimeSeriesStatementS2, exclusionPattern)
            .isPresent());
    Assert.assertFalse(
        new PipeStatementTreePatternParseVisitor()
            .visitCreateTimeseries(createTimeSeriesStatementToFilter, exclusionPattern)
            .isPresent());
  }

  @Test
  public void testCreateAlignedTimeSeries() throws IllegalPathException {
    final CreateAlignedTimeSeriesStatement expectedS1Only = new CreateAlignedTimeSeriesStatement();
    expectedS1Only.setDevicePath(new PartialPath("root.db.device"));
    expectedS1Only.setMeasurements(Collections.singletonList("s1"));
    expectedS1Only.setDataTypes(Collections.singletonList(TSDataType.FLOAT));
    expectedS1Only.setEncodings(Collections.singletonList(TSEncoding.RLE));
    expectedS1Only.setCompressors(Collections.singletonList(CompressionType.SNAPPY));
    expectedS1Only.setTagsList(Collections.singletonList(Collections.emptyMap()));
    expectedS1Only.setAttributesList(Collections.singletonList(Collections.emptyMap()));
    expectedS1Only.setAliasList(Collections.singletonList("a1"));

    final CreateAlignedTimeSeriesStatement originalS1S2 = new CreateAlignedTimeSeriesStatement();
    originalS1S2.setDevicePath(new PartialPath("root.db.device"));
    originalS1S2.setMeasurements(Arrays.asList("s1", "s2"));
    originalS1S2.setDataTypes(Arrays.asList(TSDataType.FLOAT, TSDataType.BOOLEAN));
    originalS1S2.setEncodings(Arrays.asList(TSEncoding.RLE, TSEncoding.PLAIN));
    originalS1S2.setCompressors(Arrays.asList(CompressionType.SNAPPY, CompressionType.SNAPPY));
    originalS1S2.setTagsList(Arrays.asList(Collections.emptyMap(), Collections.emptyMap()));
    originalS1S2.setAttributesList(Arrays.asList(Collections.emptyMap(), Collections.emptyMap()));
    originalS1S2.setAliasList(Arrays.asList("a1", "a2"));

    final CreateAlignedTimeSeriesStatement originalS1S2S3 = new CreateAlignedTimeSeriesStatement();
    originalS1S2S3.setDevicePath(new PartialPath("root.db.device"));
    originalS1S2S3.setMeasurements(Arrays.asList("s1", "s2", "s3"));
    originalS1S2S3.setDataTypes(
        Arrays.asList(TSDataType.FLOAT, TSDataType.BOOLEAN, TSDataType.INT32));
    originalS1S2S3.setEncodings(Arrays.asList(TSEncoding.RLE, TSEncoding.PLAIN, TSEncoding.RLE));
    originalS1S2S3.setCompressors(
        Arrays.asList(CompressionType.SNAPPY, CompressionType.SNAPPY, CompressionType.SNAPPY));
    originalS1S2S3.setTagsList(
        Arrays.asList(Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap()));
    originalS1S2S3.setAttributesList(
        Arrays.asList(Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap()));
    originalS1S2S3.setAliasList(Arrays.asList("a1", "a2", "a3"));

    Assert.assertEquals(
        expectedS1Only,
        new PipeStatementTreePatternParseVisitor()
            .visitCreateAlignedTimeseries(originalS1S2, fullPathPattern)
            .orElseThrow(AssertionError::new));

    Assert.assertEquals(
        originalS1S2,
        new PipeStatementTreePatternParseVisitor()
            .visitCreateAlignedTimeseries(originalS1S2S3, multiplePathPattern)
            .orElseThrow(AssertionError::new));

    final CreateAlignedTimeSeriesStatement expectedS1S3 = new CreateAlignedTimeSeriesStatement();
    expectedS1S3.setDevicePath(new PartialPath("root.db.device"));
    expectedS1S3.setMeasurements(Arrays.asList("s1", "s3"));
    expectedS1S3.setDataTypes(Arrays.asList(TSDataType.FLOAT, TSDataType.INT32));
    expectedS1S3.setEncodings(Arrays.asList(TSEncoding.RLE, TSEncoding.RLE));
    expectedS1S3.setCompressors(Arrays.asList(CompressionType.SNAPPY, CompressionType.SNAPPY));
    expectedS1S3.setTagsList(Arrays.asList(Collections.emptyMap(), Collections.emptyMap()));
    expectedS1S3.setAttributesList(Arrays.asList(Collections.emptyMap(), Collections.emptyMap()));
    expectedS1S3.setAliasList(Arrays.asList("a1", "a3"));

    Assert.assertEquals(
        expectedS1S3,
        new PipeStatementTreePatternParseVisitor()
            .visitCreateAlignedTimeseries(originalS1S2S3, exclusionPattern)
            .orElseThrow(AssertionError::new));
  }

  @Test
  public void testAlterTimeSeries() throws IllegalPathException {
    final Map<String, String> attributeMap = Collections.singletonMap("k1", "v1");

    final AlterTimeSeriesStatement alterTimeSeriesStatementS1 = new AlterTimeSeriesStatement(true);
    alterTimeSeriesStatementS1.setPath(new MeasurementPath("root.db.device.s1"));
    alterTimeSeriesStatementS1.setAlterMap(attributeMap);
    alterTimeSeriesStatementS1.setTagsMap(Collections.emptyMap());
    alterTimeSeriesStatementS1.setAttributesMap(attributeMap);
    alterTimeSeriesStatementS1.setAlias("");

    final AlterTimeSeriesStatement alterTimeSeriesStatementS2 = new AlterTimeSeriesStatement(true);
    alterTimeSeriesStatementS2.setPath(new MeasurementPath("root.db.device.s2")); // Different path
    alterTimeSeriesStatementS2.setAlterMap(attributeMap);
    alterTimeSeriesStatementS2.setTagsMap(Collections.emptyMap());
    alterTimeSeriesStatementS2.setAttributesMap(attributeMap);
    alterTimeSeriesStatementS2.setAlias("");

    final AlterTimeSeriesStatement alterTimeSeriesStatementToFilter =
        new AlterTimeSeriesStatement(true);
    alterTimeSeriesStatementToFilter.setPath(new MeasurementPath("root.db1.device.s1"));
    alterTimeSeriesStatementToFilter.setAlterMap(attributeMap);
    alterTimeSeriesStatementToFilter.setTagsMap(Collections.emptyMap());
    alterTimeSeriesStatementToFilter.setAttributesMap(attributeMap);
    alterTimeSeriesStatementToFilter.setAlias("");

    Assert.assertEquals(
        alterTimeSeriesStatementS1,
        new PipeStatementTreePatternParseVisitor()
            .visitAlterTimeSeries(alterTimeSeriesStatementS1, fullPathPattern)
            .orElseThrow(AssertionError::new));
    Assert.assertFalse(
        new PipeStatementTreePatternParseVisitor()
            .visitAlterTimeSeries(alterTimeSeriesStatementToFilter, prefixPathPattern)
            .isPresent());

    Assert.assertEquals(
        alterTimeSeriesStatementS1,
        new PipeStatementTreePatternParseVisitor()
            .visitAlterTimeSeries(alterTimeSeriesStatementS1, multiplePathPattern)
            .orElseThrow(AssertionError::new));
    Assert.assertFalse(
        new PipeStatementTreePatternParseVisitor()
            .visitAlterTimeSeries(alterTimeSeriesStatementToFilter, multiplePathPattern)
            .isPresent());

    Assert.assertEquals(
        alterTimeSeriesStatementS1,
        new PipeStatementTreePatternParseVisitor()
            .visitAlterTimeSeries(alterTimeSeriesStatementS1, exclusionPattern)
            .orElseThrow(AssertionError::new));
    Assert.assertFalse(
        new PipeStatementTreePatternParseVisitor()
            .visitAlterTimeSeries(alterTimeSeriesStatementS2, exclusionPattern)
            .isPresent());
    Assert.assertFalse(
        new PipeStatementTreePatternParseVisitor()
            .visitAlterTimeSeries(alterTimeSeriesStatementToFilter, exclusionPattern)
            .isPresent());
  }

  @Test
  public void testActivateTemplate() throws IllegalPathException {
    final ActivateTemplateStatement activateTemplateStatement =
        new ActivateTemplateStatement(new PartialPath("root.db.device"));
    final ActivateTemplateStatement activateTemplateStatementToFilter =
        new ActivateTemplateStatement(new PartialPath("root.db"));

    Assert.assertEquals(
        activateTemplateStatement,
        new PipeStatementTreePatternParseVisitor()
            .visitActivateTemplate(activateTemplateStatement, prefixPathPattern)
            .orElseThrow(AssertionError::new));
    Assert.assertFalse(
        new PipeStatementTreePatternParseVisitor()
            .visitActivateTemplate(activateTemplateStatementToFilter, prefixPathPattern)
            .isPresent());
    Assert.assertEquals(
        activateTemplateStatement,
        new PipeStatementTreePatternParseVisitor()
            .visitActivateTemplate(activateTemplateStatement, multiplePathPattern)
            .orElseThrow(AssertionError::new));
    Assert.assertFalse(
        new PipeStatementTreePatternParseVisitor()
            .visitActivateTemplate(activateTemplateStatementToFilter, multiplePathPattern)
            .isPresent());
  }

  @Test
  public void testCreateLogicalView() throws IllegalPathException {
    final CreateLogicalViewStatement createLogicalViewStatement = new CreateLogicalViewStatement();
    createLogicalViewStatement.setTargetFullPaths(
        Arrays.asList(new PartialPath("root.db.device.a1"), new PartialPath("root.db1.device.a1")));
    createLogicalViewStatement.setViewExpressions(
        Arrays.asList(
            new TimeSeriesViewOperand("root.sg1.d1"), new TimeSeriesViewOperand("root.sg1.d2")));

    final CreateLogicalViewStatement targetLogicalViewStatement =
        (CreateLogicalViewStatement)
            new PipeStatementTreePatternParseVisitor()
                .visitCreateLogicalView(createLogicalViewStatement, prefixPathPattern)
                .orElseThrow(AssertionError::new);
    Assert.assertEquals(
        Collections.singletonList(new PartialPath("root.db.device.a1")),
        targetLogicalViewStatement.getPaths());
    Assert.assertEquals(
        Collections.singletonList(new TimeSeriesViewOperand("root.sg1.d1")),
        targetLogicalViewStatement.getViewExpressions());
  }
}
