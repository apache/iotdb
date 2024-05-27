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

package org.apache.iotdb.db.pipe.connector;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.pipe.pattern.IoTDBPipePattern;
import org.apache.iotdb.commons.schema.view.viewExpression.leaf.TimeSeriesViewOperand;
import org.apache.iotdb.db.pipe.receiver.visitor.PipeStatementPatternParseVisitor;
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
import java.util.HashMap;

public class PipeStatementPatternParseVisitorTest {

  private final IoTDBPipePattern prefixPathPattern = new IoTDBPipePattern("root.db.device.**");
  private final IoTDBPipePattern fullPathPattern = new IoTDBPipePattern("root.db.device.s1");

  @Test
  public void testCreateTimeSeries() throws IllegalPathException {
    final CreateTimeSeriesStatement createTimeSeriesStatement = new CreateTimeSeriesStatement();
    createTimeSeriesStatement.setPath(new PartialPath("root.db.device.s1"));
    createTimeSeriesStatement.setDataType(TSDataType.FLOAT);
    createTimeSeriesStatement.setEncoding(TSEncoding.RLE);
    createTimeSeriesStatement.setCompressor(CompressionType.SNAPPY);
    createTimeSeriesStatement.setProps(new HashMap<>());
    createTimeSeriesStatement.setTags(new HashMap<>());
    createTimeSeriesStatement.setAttributes(new HashMap<>());
    createTimeSeriesStatement.setAlias("a1");

    final CreateTimeSeriesStatement createTimeSeriesStatementToFilter =
        new CreateTimeSeriesStatement();
    createTimeSeriesStatementToFilter.setPath(new PartialPath("root.db1.device.s1"));
    createTimeSeriesStatementToFilter.setDataType(TSDataType.FLOAT);
    createTimeSeriesStatementToFilter.setEncoding(TSEncoding.RLE);
    createTimeSeriesStatementToFilter.setCompressor(CompressionType.SNAPPY);
    createTimeSeriesStatementToFilter.setProps(new HashMap<>());
    createTimeSeriesStatementToFilter.setTags(new HashMap<>());
    createTimeSeriesStatementToFilter.setAttributes(new HashMap<>());
    createTimeSeriesStatementToFilter.setAlias("a2");

    Assert.assertEquals(
        createTimeSeriesStatement,
        new PipeStatementPatternParseVisitor()
            .visitCreateTimeseries(createTimeSeriesStatement, prefixPathPattern)
            .orElseThrow(AssertionError::new));
    Assert.assertFalse(
        new PipeStatementPatternParseVisitor()
            .visitCreateTimeseries(createTimeSeriesStatementToFilter, prefixPathPattern)
            .isPresent());
  }

  @Test
  public void testCreateAlignedTimeSeries() throws IllegalPathException {
    final CreateAlignedTimeSeriesStatement expectedCreateAlignedTimeSeriesStatement =
        new CreateAlignedTimeSeriesStatement();
    expectedCreateAlignedTimeSeriesStatement.setDevicePath(new PartialPath("root.db.device"));
    expectedCreateAlignedTimeSeriesStatement.setMeasurements(Collections.singletonList("s1"));
    expectedCreateAlignedTimeSeriesStatement.setDataTypes(
        Collections.singletonList(TSDataType.FLOAT));
    expectedCreateAlignedTimeSeriesStatement.setEncodings(
        Collections.singletonList(TSEncoding.RLE));
    expectedCreateAlignedTimeSeriesStatement.setCompressors(
        Collections.singletonList(CompressionType.SNAPPY));
    expectedCreateAlignedTimeSeriesStatement.setTagsList(
        Collections.singletonList(new HashMap<>()));
    expectedCreateAlignedTimeSeriesStatement.setAttributesList(
        Collections.singletonList(new HashMap<>()));
    expectedCreateAlignedTimeSeriesStatement.setAliasList(Collections.singletonList("a1"));

    final CreateAlignedTimeSeriesStatement originalCreateAlignedTimeSeriesStatement =
        new CreateAlignedTimeSeriesStatement();
    originalCreateAlignedTimeSeriesStatement.setDevicePath(new PartialPath("root.db.device"));
    originalCreateAlignedTimeSeriesStatement.setMeasurements(Arrays.asList("s1", "s2"));
    originalCreateAlignedTimeSeriesStatement.setDataTypes(
        Arrays.asList(TSDataType.FLOAT, TSDataType.BOOLEAN));
    originalCreateAlignedTimeSeriesStatement.setEncodings(
        Arrays.asList(TSEncoding.RLE, TSEncoding.PLAIN));
    originalCreateAlignedTimeSeriesStatement.setCompressors(
        Arrays.asList(CompressionType.SNAPPY, CompressionType.SNAPPY));
    originalCreateAlignedTimeSeriesStatement.setTagsList(
        Arrays.asList(new HashMap<>(), new HashMap<>()));
    originalCreateAlignedTimeSeriesStatement.setAttributesList(
        Arrays.asList(new HashMap<>(), new HashMap<>()));
    originalCreateAlignedTimeSeriesStatement.setAliasList(Arrays.asList("a1", "a2"));

    Assert.assertEquals(
        expectedCreateAlignedTimeSeriesStatement,
        new PipeStatementPatternParseVisitor()
            .visitCreateAlignedTimeseries(originalCreateAlignedTimeSeriesStatement, fullPathPattern)
            .orElseThrow(AssertionError::new));
  }

  @Test
  public void testActivateTemplate() throws IllegalPathException {
    final ActivateTemplateStatement activateTemplateStatement =
        new ActivateTemplateStatement(new PartialPath("root.db.device"));
    final ActivateTemplateStatement activateTemplateStatementToFilter =
        new ActivateTemplateStatement(new PartialPath("root.db"));

    Assert.assertEquals(
        activateTemplateStatement,
        new PipeStatementPatternParseVisitor()
            .visitActivateTemplate(activateTemplateStatement, prefixPathPattern)
            .orElseThrow(AssertionError::new));
    Assert.assertFalse(
        new PipeStatementPatternParseVisitor()
            .visitActivateTemplate(activateTemplateStatementToFilter, prefixPathPattern)
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
            new PipeStatementPatternParseVisitor()
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
