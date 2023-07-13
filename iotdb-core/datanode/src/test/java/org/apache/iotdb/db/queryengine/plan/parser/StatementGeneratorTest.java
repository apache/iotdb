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

package org.apache.iotdb.db.queryengine.plan.parser;

import org.apache.iotdb.common.rpc.thrift.TAggregationType;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.db.queryengine.plan.expression.binary.GreaterEqualExpression;
import org.apache.iotdb.db.queryengine.plan.expression.binary.LessThanExpression;
import org.apache.iotdb.db.queryengine.plan.expression.binary.LogicAndExpression;
import org.apache.iotdb.db.queryengine.plan.expression.leaf.ConstantOperand;
import org.apache.iotdb.db.queryengine.plan.expression.leaf.TimeSeriesOperand;
import org.apache.iotdb.db.queryengine.plan.expression.leaf.TimestampOperand;
import org.apache.iotdb.db.queryengine.plan.expression.multi.FunctionExpression;
import org.apache.iotdb.db.queryengine.plan.statement.Statement;
import org.apache.iotdb.db.queryengine.plan.statement.component.ResultColumn;
import org.apache.iotdb.db.queryengine.plan.statement.crud.DeleteDataStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertMultiTabletsStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertRowStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertRowsOfOneDeviceStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertRowsStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertTabletStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.QueryStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.CreateAlignedTimeSeriesStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.CreateMultiTimeSeriesStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.CreateTimeSeriesStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.DatabaseSchemaStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.DeleteDatabaseStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.DeleteTimeSeriesStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.template.BatchActivateTemplateStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.template.CreateSchemaTemplateStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.template.DropSchemaTemplateStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.template.ShowNodesInSchemaTemplateStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.template.UnsetSchemaTemplateStatement;
import org.apache.iotdb.isession.template.TemplateNode;
import org.apache.iotdb.mpp.rpc.thrift.TDeleteModelMetricsReq;
import org.apache.iotdb.mpp.rpc.thrift.TFetchTimeseriesReq;
import org.apache.iotdb.mpp.rpc.thrift.TRecordModelMetricsReq;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.service.rpc.thrift.TSAggregationQueryReq;
import org.apache.iotdb.service.rpc.thrift.TSCreateAlignedTimeseriesReq;
import org.apache.iotdb.service.rpc.thrift.TSCreateMultiTimeseriesReq;
import org.apache.iotdb.service.rpc.thrift.TSCreateSchemaTemplateReq;
import org.apache.iotdb.service.rpc.thrift.TSCreateTimeseriesReq;
import org.apache.iotdb.service.rpc.thrift.TSDeleteDataReq;
import org.apache.iotdb.service.rpc.thrift.TSDropSchemaTemplateReq;
import org.apache.iotdb.service.rpc.thrift.TSInsertRecordReq;
import org.apache.iotdb.service.rpc.thrift.TSInsertRecordsOfOneDeviceReq;
import org.apache.iotdb.service.rpc.thrift.TSInsertRecordsReq;
import org.apache.iotdb.service.rpc.thrift.TSInsertStringRecordsOfOneDeviceReq;
import org.apache.iotdb.service.rpc.thrift.TSInsertStringRecordsReq;
import org.apache.iotdb.service.rpc.thrift.TSInsertTabletReq;
import org.apache.iotdb.service.rpc.thrift.TSInsertTabletsReq;
import org.apache.iotdb.service.rpc.thrift.TSLastDataQueryReq;
import org.apache.iotdb.service.rpc.thrift.TSQueryTemplateReq;
import org.apache.iotdb.service.rpc.thrift.TSRawDataQueryReq;
import org.apache.iotdb.service.rpc.thrift.TSUnsetSchemaTemplateReq;
import org.apache.iotdb.session.template.MeasurementNode;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;

import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;

import static org.apache.iotdb.db.schemaengine.template.TemplateQueryType.SHOW_MEASUREMENTS;
import static org.apache.iotdb.tsfile.file.metadata.enums.CompressionType.SNAPPY;
import static org.junit.Assert.assertEquals;

public class StatementGeneratorTest {

  @Test
  public void testRawDataQuery() throws IllegalPathException {
    TSRawDataQueryReq req =
        new TSRawDataQueryReq(
            101L, Arrays.asList("root.sg.d1.s1", "root.sg.d1.s2"), 100L, 200L, 102L);
    Statement statement = StatementGenerator.createStatement(req, ZonedDateTime.now().getOffset());
    QueryStatement queryStatement = (QueryStatement) statement;
    assertEquals(
        Arrays.asList(new PartialPath("root.sg.d1.s2"), new PartialPath("root.sg.d1.s1")),
        queryStatement.getPaths());
    assertEquals(
        new LogicAndExpression(
            new GreaterEqualExpression(
                new TimestampOperand(), new ConstantOperand(TSDataType.INT64, "100")),
            new LessThanExpression(
                new TimestampOperand(), new ConstantOperand(TSDataType.INT64, "200"))),
        queryStatement.getWhereCondition().getPredicate());
  }

  @Test
  public void testLastDataQuery() throws IllegalPathException {
    TSLastDataQueryReq req =
        new TSLastDataQueryReq(101L, Arrays.asList("root.sg.d1.s1", "root.sg.d1.s2"), 200L, 102L);
    Statement statement = StatementGenerator.createStatement(req, ZonedDateTime.now().getOffset());
    QueryStatement queryStatement = (QueryStatement) statement;
    assertEquals(
        Arrays.asList(new PartialPath("root.sg.d1.s2"), new PartialPath("root.sg.d1.s1")),
        queryStatement.getPaths());
    assertEquals(
        new GreaterEqualExpression(
            new TimestampOperand(), new ConstantOperand(TSDataType.INT64, "200")),
        queryStatement.getWhereCondition().getPredicate());
  }

  @Test
  public void testAggregationQuery() throws IllegalPathException {
    TSAggregationQueryReq req =
        new TSAggregationQueryReq(
            101L,
            102L,
            Arrays.asList("root.sg.d1.s1", "root.sg.d1.s2"),
            Arrays.asList(TAggregationType.AVG, TAggregationType.COUNT));
    Statement statement = StatementGenerator.createStatement(req, ZonedDateTime.now().getOffset());
    QueryStatement queryStatement = (QueryStatement) statement;
    assertEquals(
        Arrays.asList(new PartialPath("root.sg.d1.s2"), new PartialPath("root.sg.d1.s1")),
        queryStatement.getPaths());
    assertEquals(
        new ResultColumn(
            new FunctionExpression(
                "AVG",
                new LinkedHashMap<>(),
                Collections.singletonList(
                    new TimeSeriesOperand(new PartialPath("root.sg.d1.s1"))))),
        queryStatement.getSelectComponent().getResultColumns().get(0));
    assertEquals(
        new ResultColumn(
            new FunctionExpression(
                "COUNT",
                new LinkedHashMap<>(),
                Collections.singletonList(
                    new TimeSeriesOperand(new PartialPath("root.sg.d1.s2"))))),
        queryStatement.getSelectComponent().getResultColumns().get(1));
  }

  @Test
  public void testInsertRecord() throws QueryProcessException, IllegalPathException {
    TSInsertRecordReq req =
        new TSInsertRecordReq(
            101L,
            "root.sg.d1",
            Arrays.asList("s1", "s2"),
            ByteBuffer.wrap(new byte[] {0, 0, 0, 0}),
            1000L);
    InsertRowStatement statement = StatementGenerator.createStatement(req);
    assertEquals(1000L, statement.getTime());
  }

  @Test
  public void testInsertTablet() throws IllegalPathException {
    TSInsertTabletReq req =
        new TSInsertTabletReq(
            101L,
            "root.sg.d1",
            Collections.singletonList("s1"),
            ByteBuffer.wrap(new byte[] {0, 0, 0, 0}),
            ByteBuffer.wrap(new byte[] {0, 0, 0, 0, 0, 0, 0, 0}),
            Collections.singletonList(1),
            1);
    InsertTabletStatement statement = StatementGenerator.createStatement(req);
    assertEquals(0L, statement.getMinTime());
  }

  @Test
  public void testInsertTablets() throws IllegalPathException {
    TSInsertTabletsReq req =
        new TSInsertTabletsReq(
            101L,
            Collections.singletonList("root.sg.d1"),
            Collections.singletonList(Collections.singletonList("s1")),
            Collections.singletonList(ByteBuffer.wrap(new byte[] {0, 0, 0, 0})),
            Collections.singletonList(ByteBuffer.wrap(new byte[] {0, 0, 0, 0, 0, 0, 0, 0})),
            Collections.singletonList(Collections.singletonList(1)),
            Collections.singletonList(1));
    InsertMultiTabletsStatement statement = StatementGenerator.createStatement(req);
    assertEquals(Collections.singletonList(new PartialPath("root.sg.d1.s1")), statement.getPaths());
  }

  @Test
  public void testInsertRecords() throws QueryProcessException, IllegalPathException {
    TSInsertRecordsReq req =
        new TSInsertRecordsReq(
            101L,
            Collections.singletonList("root.sg.d1"),
            Collections.singletonList(Collections.singletonList("s1")),
            Collections.singletonList(ByteBuffer.wrap(new byte[] {0, 0, 0, 0})),
            Collections.singletonList(1L));
    InsertRowsStatement statement = StatementGenerator.createStatement(req);
    assertEquals(Collections.singletonList(new PartialPath("root.sg.d1.s1")), statement.getPaths());
  }

  @Test
  public void testInsertStringRecords() throws IllegalPathException {
    TSInsertStringRecordsReq req =
        new TSInsertStringRecordsReq(
            101L,
            Collections.singletonList("root.sg.d1"),
            Collections.singletonList(Collections.singletonList("s1")),
            Collections.singletonList(Collections.singletonList("1")),
            Collections.singletonList(1L));
    InsertRowsStatement statement = StatementGenerator.createStatement(req);
    assertEquals(Collections.singletonList(new PartialPath("root.sg.d1.s1")), statement.getPaths());
  }

  @Test
  public void testInsertRecordsOfOneDevice() throws IllegalPathException, QueryProcessException {
    TSInsertRecordsOfOneDeviceReq req =
        new TSInsertRecordsOfOneDeviceReq(
            101L,
            "root.sg.d1",
            Collections.singletonList(Collections.singletonList("s1")),
            Collections.singletonList(ByteBuffer.wrap(new byte[] {0, 0, 0, 0})),
            Collections.singletonList(1L));
    InsertRowsOfOneDeviceStatement statement = StatementGenerator.createStatement(req);
    assertEquals(Collections.singletonList(new PartialPath("root.sg.d1.s1")), statement.getPaths());
  }

  @Test
  public void testInsertStringRecordsOfOneDevice() throws IllegalPathException {
    TSInsertStringRecordsOfOneDeviceReq req =
        new TSInsertStringRecordsOfOneDeviceReq(
            101L,
            "root.sg.d1",
            Collections.singletonList(Collections.singletonList("s1")),
            Collections.singletonList(Collections.singletonList("1")),
            Collections.singletonList(1L));
    InsertRowsOfOneDeviceStatement statement = StatementGenerator.createStatement(req);
    assertEquals(Collections.singletonList(new PartialPath("root.sg.d1.s1")), statement.getPaths());
  }

  @Test
  public void testCreateDatabaseSchema() throws IllegalPathException {
    DatabaseSchemaStatement statement = StatementGenerator.createStatement("root.db");
    assertEquals(new PartialPath("root.db"), statement.getDatabasePath());
  }

  @Test
  public void testCreateTimeSeries() throws IllegalPathException {
    TSCreateTimeseriesReq req =
        new TSCreateTimeseriesReq(
            1L,
            "root.db.d1.s1",
            TSDataType.INT64.getType(),
            TSEncoding.PLAIN.ordinal(),
            SNAPPY.ordinal());
    CreateTimeSeriesStatement statement = StatementGenerator.createStatement(req);
    assertEquals(new PartialPath("root.db.d1.s1"), statement.getPath());
    assertEquals(TSDataType.INT64, statement.getDataType());
    assertEquals(TSEncoding.PLAIN, statement.getEncoding());
    assertEquals(SNAPPY, statement.getCompressor());
  }

  @Test
  public void testCreateAlignedTimeSeries() throws IllegalPathException {
    TSCreateAlignedTimeseriesReq req =
        new TSCreateAlignedTimeseriesReq(
            1L,
            "root.db.d1",
            Arrays.asList("s1", "s2"),
            Arrays.asList(TSDataType.INT64.ordinal(), TSDataType.INT32.ordinal()),
            Arrays.asList(TSEncoding.PLAIN.ordinal(), TSEncoding.RLE.ordinal()),
            Arrays.asList(SNAPPY.ordinal(), SNAPPY.ordinal()));
    CreateAlignedTimeSeriesStatement statement = StatementGenerator.createStatement(req);
    assertEquals(
        Arrays.asList(new PartialPath("root.db.d1.s1"), new PartialPath("root.db.d1.s2")),
        statement.getPaths());
    assertEquals(Arrays.asList(TSDataType.INT64, TSDataType.INT32), statement.getDataTypes());
    assertEquals(Arrays.asList(TSEncoding.PLAIN, TSEncoding.RLE), statement.getEncodings());
    assertEquals(Arrays.asList(SNAPPY, SNAPPY), statement.getCompressors());
  }

  @Test
  public void testCreateMultiTimeSeries() throws IllegalPathException {
    TSCreateMultiTimeseriesReq req =
        new TSCreateMultiTimeseriesReq(
            1L,
            Arrays.asList("root.db.d1.s1", "root.db.d1.s2"),
            Arrays.asList(TSDataType.INT64.ordinal(), TSDataType.INT32.ordinal()),
            Arrays.asList(TSEncoding.PLAIN.ordinal(), TSEncoding.RLE.ordinal()),
            Arrays.asList(SNAPPY.ordinal(), SNAPPY.ordinal()));
    CreateMultiTimeSeriesStatement statement = StatementGenerator.createStatement(req);
    assertEquals(
        Arrays.asList(new PartialPath("root.db.d1.s1"), new PartialPath("root.db.d1.s2")),
        statement.getPaths());
    assertEquals(Arrays.asList(TSDataType.INT64, TSDataType.INT32), statement.getDataTypes());
    assertEquals(Arrays.asList(TSEncoding.PLAIN, TSEncoding.RLE), statement.getEncodings());
    assertEquals(Arrays.asList(SNAPPY, SNAPPY), statement.getCompressors());
  }

  @Test
  public void testDeleteDatabase() throws IllegalPathException {
    DeleteDatabaseStatement statement =
        StatementGenerator.createStatement(Collections.singletonList("root.db"));
    assertEquals(Collections.singletonList("root.db"), statement.getPrefixPath());
  }

  @Test
  public void testDeleteData() throws IllegalPathException {
    TSDeleteDataReq req =
        new TSDeleteDataReq(1L, Arrays.asList("root.sg.d1.s1", "root.sg.d1.s2"), 1L, 100L);
    DeleteDataStatement statement = StatementGenerator.createStatement(req);
    assertEquals(
        Arrays.asList(new PartialPath("root.sg.d1.s1"), new PartialPath("root.sg.d1.s2")),
        statement.getPaths());
    assertEquals(1L, statement.getDeleteStartTime());
    assertEquals(100L, statement.getDeleteEndTime());
  }

  @Test
  public void testCreateSchemaTemplate()
      throws MetadataException, IOException, StatementExecutionException {
    org.apache.iotdb.isession.template.Template template = getTemplate();
    TSCreateSchemaTemplateReq req = new TSCreateSchemaTemplateReq();
    req.setName(template.getName());
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    template.serialize(baos);
    req.setSerializedTemplate(baos.toByteArray());
    baos.close();
    CreateSchemaTemplateStatement statement = StatementGenerator.createStatement(req);
    assertEquals("test-template", statement.getName());
    assertEquals(Arrays.asList("y", "x"), statement.getMeasurements());
  }

  @Test
  public void testQueryTemplate() {
    TSQueryTemplateReq req = new TSQueryTemplateReq(1L, "test", SHOW_MEASUREMENTS.ordinal());
    ShowNodesInSchemaTemplateStatement statement =
        (ShowNodesInSchemaTemplateStatement) StatementGenerator.createStatement(req);
    assertEquals("test", statement.getTemplateName());
  }

  @Test
  public void testUnsetSchemaTemplate() throws IllegalPathException {
    TSUnsetSchemaTemplateReq req = new TSUnsetSchemaTemplateReq(1L, "root.sg.d1", "test");
    UnsetSchemaTemplateStatement statement = StatementGenerator.createStatement(req);
    assertEquals("test", statement.getTemplateName());
  }

  @Test
  public void testDropSchemaTemplate() {
    TSDropSchemaTemplateReq req = new TSDropSchemaTemplateReq(1L, "test-template");
    DropSchemaTemplateStatement statement = StatementGenerator.createStatement(req);
    assertEquals("test-template", statement.getTemplateName());
  }

  @Test
  public void testBatchActivateTemplate() throws IllegalPathException {
    BatchActivateTemplateStatement statement =
        StatementGenerator.createBatchActivateTemplateStatement(
            Collections.singletonList("root.sg.d1"));
    assertEquals(
        Collections.singletonList(new PartialPath("root.sg.d1")), statement.getDevicePathList());
  }

  @Test
  public void testDeleteTimeSeries() throws IllegalPathException {
    DeleteTimeSeriesStatement statement =
        StatementGenerator.createDeleteTimeSeriesStatement(Collections.singletonList("root.sg.d1"));
    assertEquals(Collections.singletonList(new PartialPath("root.sg.d1")), statement.getPaths());
  }

  @Test
  public void testInsertRecordModelMetrics() throws IllegalPathException {
    TRecordModelMetricsReq recordModelMetricsReq =
        new TRecordModelMetricsReq(
            "modelId",
            "trialId",
            Collections.singletonList("metrics"),
            1L,
            Collections.singletonList(1.0));
    InsertRowStatement statement = StatementGenerator.createStatement(recordModelMetricsReq);
    assertEquals(1L, statement.getTime());
  }

  @Test
  public void testFetchTimeseries() throws IllegalPathException {
    TFetchTimeseriesReq req = new TFetchTimeseriesReq("select * from root.sg.d1.s1");
    Statement statement = StatementGenerator.createStatement(req, ZonedDateTime.now().getOffset());
    assertEquals("root.sg.d1.s1.*", statement.getPaths().get(0).getFullPath());
  }

  @Test
  public void testDeleteModelMetrics() throws IllegalPathException {
    TDeleteModelMetricsReq req = new TDeleteModelMetricsReq("model");
    DeleteTimeSeriesStatement statement = StatementGenerator.createStatement(req);
    assertEquals(
        Collections.singletonList(new PartialPath("root.__system.ml.exp.model.**")),
        statement.getPaths());
  }

  private org.apache.iotdb.isession.template.Template getTemplate()
      throws StatementExecutionException {
    org.apache.iotdb.isession.template.Template sessionTemplate =
        new org.apache.iotdb.isession.template.Template("test-template", false);

    TemplateNode mNodeX =
        new MeasurementNode("x", TSDataType.FLOAT, TSEncoding.RLE, CompressionType.SNAPPY);
    TemplateNode mNodeY =
        new MeasurementNode("y", TSDataType.FLOAT, TSEncoding.RLE, CompressionType.SNAPPY);

    sessionTemplate.addToTemplate(mNodeX);
    sessionTemplate.addToTemplate(mNodeY);

    return sessionTemplate;
  }

  @Test
  public void rawDataQueryTest() {
    String sql = "SELECT s1, s2 FROM root.sg1.d1 WHERE time > 1 and s3 > 2 LIMIT 10 OFFSET 11";
    checkQueryStatement(
        sql,
        Arrays.asList("s1", "s2"),
        Collections.singletonList("root.sg1.d1"),
        "Time > 1 & s3 > 2",
        10,
        11);
  }

  @Test
  public void groupByTagWithDuplicatedKeysTest() {
    try {
      checkQueryStatement(
          "SELECT avg(*) FROM root.sg.** GROUP BY TAGS(k1, k2, k1)",
          Collections.emptyList(),
          Collections.emptyList(),
          "",
          10,
          10);
      Assert.fail();
    } catch (SemanticException e) {
      assertEquals("duplicated key in GROUP BY TAGS: k1", e.getMessage());
    }
  }

  // TODO: add more tests

  private void checkQueryStatement(
      String sql,
      List<String> selectExprList,
      List<String> fromPrefixPaths,
      String wherePredicateString,
      int rowLimit,
      int rowOffset) {
    QueryStatement statement =
        (QueryStatement) StatementGenerator.createStatement(sql, ZonedDateTime.now().getOffset());

    // check SELECT clause
    int cnt = 0;
    for (ResultColumn resultColumn : statement.getSelectComponent().getResultColumns()) {
      String selectExpr = resultColumn.getExpression().toString();
      assertEquals(selectExprList.get(cnt++), selectExpr);
    }
    assertEquals(selectExprList.size(), statement.getSelectComponent().getResultColumns().size());

    // check FROM clause
    cnt = 0;
    for (PartialPath path : statement.getFromComponent().getPrefixPaths()) {
      assertEquals(fromPrefixPaths.get(cnt++), path.toString());
    }
    assertEquals(fromPrefixPaths.size(), statement.getFromComponent().getPrefixPaths().size());

    // check WHERE clause
    assertEquals(
        wherePredicateString, statement.getWhereCondition().getPredicate().getExpressionString());

    // check LIMIT & OFFSET clause
    assertEquals(rowLimit, statement.getRowLimit());
    assertEquals(rowOffset, statement.getRowOffset());

    // TODO: add more clause
  }
}
