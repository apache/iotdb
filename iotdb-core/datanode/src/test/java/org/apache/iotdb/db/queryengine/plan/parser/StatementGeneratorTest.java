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
import org.apache.iotdb.commons.auth.entity.PrivilegeType;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.db.queryengine.plan.expression.binary.GreaterEqualExpression;
import org.apache.iotdb.db.queryengine.plan.expression.binary.LessThanExpression;
import org.apache.iotdb.db.queryengine.plan.expression.binary.LogicAndExpression;
import org.apache.iotdb.db.queryengine.plan.expression.leaf.ConstantOperand;
import org.apache.iotdb.db.queryengine.plan.expression.leaf.TimeSeriesOperand;
import org.apache.iotdb.db.queryengine.plan.expression.leaf.TimestampOperand;
import org.apache.iotdb.db.queryengine.plan.expression.multi.FunctionExpression;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.ColumnSchema;
import org.apache.iotdb.db.queryengine.plan.relational.type.InternalTypeManager;
import org.apache.iotdb.db.queryengine.plan.statement.Statement;
import org.apache.iotdb.db.queryengine.plan.statement.StatementTestUtils;
import org.apache.iotdb.db.queryengine.plan.statement.StatementType;
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
import org.apache.iotdb.db.queryengine.plan.statement.metadata.view.CreateLogicalViewStatement;
import org.apache.iotdb.db.queryengine.plan.statement.sys.AuthorStatement;
import org.apache.iotdb.isession.template.TemplateNode;
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

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.common.type.TypeFactory;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.BitMap;
import org.apache.tsfile.write.record.Tablet;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.iotdb.db.schemaengine.template.TemplateQueryType.SHOW_MEASUREMENTS;
import static org.apache.tsfile.file.metadata.enums.CompressionType.SNAPPY;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class StatementGeneratorTest {

  @Test
  public void testRawDataQuery() throws IllegalPathException {
    TSRawDataQueryReq req =
        new TSRawDataQueryReq(
            101L, Arrays.asList("root.sg.d1.s1", "root.sg.d1.s2"), 100L, 200L, 102L);
    Statement statement = StatementGenerator.createStatement(req);
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
    Statement statement = StatementGenerator.createStatement(req);
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
    Statement statement = StatementGenerator.createStatement(req);
    QueryStatement queryStatement = (QueryStatement) statement;
    assertEquals(
        Arrays.asList(new PartialPath("root.sg.d1.s2"), new PartialPath("root.sg.d1.s1")),
        queryStatement.getPaths());
    assertEquals(
        new ResultColumn(
            new FunctionExpression(
                "AVG",
                new LinkedHashMap<>(),
                Collections.singletonList(new TimeSeriesOperand(new PartialPath("root.sg.d1.s1")))),
            ResultColumn.ColumnType.AGGREGATION),
        queryStatement.getSelectComponent().getResultColumns().get(0));
    assertEquals(
        new ResultColumn(
            new FunctionExpression(
                "COUNT",
                new LinkedHashMap<>(),
                Collections.singletonList(new TimeSeriesOperand(new PartialPath("root.sg.d1.s2")))),
            ResultColumn.ColumnType.AGGREGATION),
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
  public void testInsertRelationalTablet() throws IllegalPathException {
    List<String> measurements = Arrays.asList("id1", "attr1", "m1");
    List<TSDataType> dataTypes = Arrays.asList(TSDataType.TEXT, TSDataType.TEXT, TSDataType.DOUBLE);
    List<Tablet.ColumnType> tsfileColumnCategories =
        Arrays.asList(
            Tablet.ColumnType.ID, Tablet.ColumnType.ATTRIBUTE, Tablet.ColumnType.MEASUREMENT);
    List<TsTableColumnCategory> columnCategories =
        tsfileColumnCategories.stream()
            .map(TsTableColumnCategory::fromTsFileColumnType)
            .collect(Collectors.toList());
    TSInsertTabletReq req =
        new TSInsertTabletReq(
            101L,
            "root.sg.d1",
            measurements,
            ByteBuffer.wrap(new byte[128]),
            ByteBuffer.wrap(new byte[128]),
            dataTypes.stream().map(d -> (int) d.serialize()).collect(Collectors.toList()),
            1);
    req.setColumnCategories(
        tsfileColumnCategories.stream().map(c -> (byte) c.ordinal()).collect(Collectors.toList()));
    req.setWriteToTable(true);

    final InsertTabletStatement statement = StatementGenerator.createStatement(req);
    assertEquals(measurements, Arrays.asList(statement.getMeasurements()));
    assertEquals(dataTypes, Arrays.asList(statement.getDataTypes()));
    assertEquals(columnCategories, Arrays.asList(statement.getColumnCategories()));
    assertTrue(statement.isWriteToTable());
  }

  @Test
  public void testTabletInsertColumn() {
    final InsertTabletStatement insertTabletStatement =
        StatementTestUtils.genInsertTabletStatement(false);
    // insert at head
    int insertPos = 0;

    ColumnSchema columnSchema =
        new ColumnSchema(
            "s1", TypeFactory.getType(TSDataType.STRING), false, TsTableColumnCategory.ID);
    insertTabletStatement.insertColumn(insertPos, columnSchema);
    assertEquals(4, insertTabletStatement.getMeasurements().length);
    assertEquals(columnSchema.getName(), insertTabletStatement.getMeasurements()[insertPos]);
    assertEquals(
        InternalTypeManager.getTSDataType(columnSchema.getType()),
        insertTabletStatement.getDataType(insertPos));
    assertEquals(
        columnSchema.getColumnCategory(), insertTabletStatement.getColumnCategories()[insertPos]);
    final Object[] column1 = (Object[]) insertTabletStatement.getColumns()[insertPos];
    for (Object o : column1) {
      assertNull(o);
    }

    // insert at middle
    insertPos = 2;
    columnSchema =
        new ColumnSchema(
            "s2", TypeFactory.getType(TSDataType.INT64), false, TsTableColumnCategory.ATTRIBUTE);
    insertTabletStatement.insertColumn(insertPos, columnSchema);
    assertEquals(5, insertTabletStatement.getMeasurements().length);
    assertEquals(columnSchema.getName(), insertTabletStatement.getMeasurements()[insertPos]);
    assertEquals(
        InternalTypeManager.getTSDataType(columnSchema.getType()),
        insertTabletStatement.getDataType(insertPos));
    assertEquals(
        columnSchema.getColumnCategory(), insertTabletStatement.getColumnCategories()[insertPos]);
    final long[] column2 = (long[]) insertTabletStatement.getColumns()[insertPos];
    for (long o : column2) {
      assertEquals(0, o);
    }

    // insert at last
    insertPos = 5;
    columnSchema =
        new ColumnSchema(
            "s3",
            TypeFactory.getType(TSDataType.BOOLEAN),
            false,
            TsTableColumnCategory.MEASUREMENT);
    insertTabletStatement.insertColumn(insertPos, columnSchema);
    assertEquals(6, insertTabletStatement.getMeasurements().length);
    assertEquals(columnSchema.getName(), insertTabletStatement.getMeasurements()[insertPos]);
    assertEquals(
        InternalTypeManager.getTSDataType(columnSchema.getType()),
        insertTabletStatement.getDataType(insertPos));
    assertEquals(
        columnSchema.getColumnCategory(), insertTabletStatement.getColumnCategories()[insertPos]);
    final boolean[] column3 = (boolean[]) insertTabletStatement.getColumns()[insertPos];
    for (boolean o : column3) {
      assertFalse(o);
    }

    // illegal insertion
    ColumnSchema finalColumnSchema = columnSchema;
    assertThrows(
        ArrayIndexOutOfBoundsException.class,
        () -> insertTabletStatement.insertColumn(-1, finalColumnSchema));
    assertThrows(
        ArrayIndexOutOfBoundsException.class,
        () -> insertTabletStatement.insertColumn(7, finalColumnSchema));
  }

  @Test
  public void testInsertTabletSwapColumn() {
    final String[] columnNames = StatementTestUtils.genColumnNames();
    final Object[] columns = StatementTestUtils.genColumns();
    final TSDataType[] tsDataTypes = StatementTestUtils.genDataTypes();
    final TsTableColumnCategory[] columnCategories = StatementTestUtils.genColumnCategories();

    final InsertTabletStatement insertTabletStatement =
        StatementTestUtils.genInsertTabletStatement(false);
    BitMap[] bitMaps = new BitMap[columnNames.length];
    for (int i = 0; i < bitMaps.length; i++) {
      bitMaps[i] = new BitMap(3);
      bitMaps[i].mark(i);
    }
    insertTabletStatement.setBitMaps(bitMaps);

    // [0, 1, 2] -> [2, 1, 0]
    insertTabletStatement.swapColumn(0, 2);
    assertEquals(3, insertTabletStatement.getMeasurements().length);
    assertEquals(columnNames[0], insertTabletStatement.getMeasurements()[2]);
    assertEquals(columnNames[2], insertTabletStatement.getMeasurements()[0]);
    assertEquals(tsDataTypes[0], insertTabletStatement.getDataType(2));
    assertEquals(tsDataTypes[2], insertTabletStatement.getDataType(0));
    assertEquals(columnCategories[0], insertTabletStatement.getColumnCategories()[2]);
    assertEquals(columnCategories[2], insertTabletStatement.getColumnCategories()[0]);
    assertArrayEquals(
        ((double[]) columns[2]), ((double[]) insertTabletStatement.getColumns()[0]), 0.0001);
    assertArrayEquals(((Binary[]) columns[0]), ((Binary[]) insertTabletStatement.getColumns()[2]));
    assertTrue(insertTabletStatement.getBitMaps()[0].isMarked(2));
    assertTrue(insertTabletStatement.getBitMaps()[2].isMarked(0));

    // [2, 1, 0] -> [1, 2, 0]
    insertTabletStatement.swapColumn(0, 1);
    assertEquals(3, insertTabletStatement.getMeasurements().length);
    assertEquals(columnNames[1], insertTabletStatement.getMeasurements()[0]);
    assertEquals(columnNames[2], insertTabletStatement.getMeasurements()[1]);
    assertEquals(tsDataTypes[1], insertTabletStatement.getDataType(0));
    assertEquals(tsDataTypes[2], insertTabletStatement.getDataType(1));
    assertEquals(columnCategories[1], insertTabletStatement.getColumnCategories()[0]);
    assertEquals(columnCategories[2], insertTabletStatement.getColumnCategories()[1]);
    assertArrayEquals(((Binary[]) columns[1]), ((Binary[]) insertTabletStatement.getColumns()[0]));
    assertArrayEquals(
        ((double[]) columns[2]), ((double[]) insertTabletStatement.getColumns()[1]), 0.0001);
    assertTrue(insertTabletStatement.getBitMaps()[0].isMarked(1));
    assertTrue(insertTabletStatement.getBitMaps()[1].isMarked(2));

    // [1, 2, 0] -> [1, 2, 0]
    insertTabletStatement.swapColumn(1, 1);
    assertEquals(3, insertTabletStatement.getMeasurements().length);
    assertEquals(columnNames[1], insertTabletStatement.getMeasurements()[0]);
    assertEquals(columnNames[2], insertTabletStatement.getMeasurements()[1]);
    assertEquals(tsDataTypes[1], insertTabletStatement.getDataType(0));
    assertEquals(tsDataTypes[2], insertTabletStatement.getDataType(1));
    assertEquals(columnCategories[1], insertTabletStatement.getColumnCategories()[0]);
    assertEquals(columnCategories[2], insertTabletStatement.getColumnCategories()[1]);
    assertArrayEquals(((Binary[]) columns[1]), ((Binary[]) insertTabletStatement.getColumns()[0]));
    assertArrayEquals(
        ((double[]) columns[2]), ((double[]) insertTabletStatement.getColumns()[1]), 0.0001);
    assertTrue(insertTabletStatement.getBitMaps()[0].isMarked(1));
    assertTrue(insertTabletStatement.getBitMaps()[1].isMarked(2));

    // illegal
    assertThrows(
        ArrayIndexOutOfBoundsException.class, () -> insertTabletStatement.swapColumn(-1, 1));
    assertThrows(
        ArrayIndexOutOfBoundsException.class, () -> insertTabletStatement.swapColumn(3, 1));
    assertThrows(
        ArrayIndexOutOfBoundsException.class, () -> insertTabletStatement.swapColumn(1, -1));
    assertThrows(
        ArrayIndexOutOfBoundsException.class, () -> insertTabletStatement.swapColumn(1, 3));
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

  private AuthorStatement createAuthDclStmt(String sql) {
    Statement stmt = StatementGenerator.createStatement(sql, ZonedDateTime.now().getOffset());
    AuthorStatement roleDcl = (AuthorStatement) stmt;
    return roleDcl;
  }

  @Test
  public void testDCLUserOperation() {
    // 1. create user and drop user
    AuthorStatement userDcl = createAuthDclStmt("create user `user1` 'password1';");
    assertEquals("user1", userDcl.getUserName());
    assertEquals(Collections.emptyList(), userDcl.getPaths());
    assertEquals("password1", userDcl.getPassWord());
    assertEquals(StatementType.CREATE_USER, userDcl.getType());

    userDcl = createAuthDclStmt("drop user `user1`;");
    assertEquals("user1", userDcl.getUserName());
    assertEquals(Collections.emptyList(), userDcl.getPaths());
    assertEquals(StatementType.DELETE_USER, userDcl.getType());

    // 2.update user's password
    userDcl = createAuthDclStmt("alter user `user1` set password 'password2';");
    assertEquals("user1", userDcl.getUserName());
    /**
     * [ BUG ] We didn't save the old password in the statement. If userA has logged in with
     * session_A. Session_B's alter password operation cannot block session A's alter password
     * operation because we only check the user's password before they log in.
     */
    // assertEquals("password1", userDcl.getPassWord());
    assertEquals("password2", userDcl.getNewPassword());
    assertEquals(StatementType.MODIFY_PASSWORD, userDcl.getType());
  }

  @Test
  public void testDCLROLEOperation() {
    // 1. create role and drop role.
    AuthorStatement roleDcl = createAuthDclStmt("create role role1;");
    assertEquals("role1", roleDcl.getRoleName());
    assertEquals(StatementType.CREATE_ROLE, roleDcl.getType());

    roleDcl = createAuthDclStmt("drop role role1;");
    assertEquals(StatementType.DELETE_ROLE, roleDcl.getType());
    assertEquals("role1", roleDcl.getRoleName());

    // 2. grant and revoke role.
    roleDcl = createAuthDclStmt("grant role `role1` to `user1`;");
    assertEquals(StatementType.GRANT_USER_ROLE, roleDcl.getType());
    assertEquals("role1", roleDcl.getRoleName());
    assertEquals("user1", roleDcl.getUserName());

    roleDcl = createAuthDclStmt("revoke role `role1` from `user1`;");
    assertEquals(StatementType.REVOKE_USER_ROLE, roleDcl.getType());
    assertEquals("role1", roleDcl.getRoleName());
    assertEquals("user1", roleDcl.getUserName());
  }

  @FunctionalInterface
  interface grantRevokeCheck {

    void checkParser(String privilege, String name, boolean isuser, String path, boolean grantOpt);
  }

  /** This test will check grant/revoke simple privilege on/from paths. */
  @Test
  public void testNormalGrantRevoke() {
    grantRevokeCheck testGrant =
        (privilege, name, isuser, path, grantOpt) -> {
          String sql = "grant %s on %s to %s %s %s ;";
          sql =
              String.format(
                  sql,
                  privilege,
                  path,
                  isuser ? "USER" : "ROLE",
                  name,
                  grantOpt ? "with grant option" : "");
          AuthorStatement aclStmt = createAuthDclStmt(sql);
          assertEquals(
              isuser ? StatementType.GRANT_USER_PRIVILEGE : StatementType.GRANT_ROLE_PRIVILEGE,
              aclStmt.getType());
          assertEquals(path, aclStmt.getPaths().get(0).toString());
          assertEquals(name, isuser ? aclStmt.getUserName() : aclStmt.getRoleName());
          assertEquals(grantOpt, aclStmt.getGrantOpt());
          assertEquals(privilege, aclStmt.getPrivilegeList()[0]);
        };

    String name = "test1";
    String path = "root.**";
    String pathErr = "root.t1.**";
    String pathsErr = "root.**, root.t1.**";

    // 1. check simple privilege grant to user/role with/without grant option.
    for (PrivilegeType privilege : PrivilegeType.values()) {
      testGrant.checkParser(privilege.toString(), name, true, path, true);
      testGrant.checkParser(privilege.toString(), name, true, path, false);
      testGrant.checkParser(privilege.toString(), name, false, path, true);
      testGrant.checkParser(privilege.toString(), name, false, path, false);
      // 2. if grant stmt has system privilege, path should be root.**
      if (!privilege.isPathRelevant()) {
        assertThrows(
            SemanticException.class,
            () ->
                createAuthDclStmt(
                    String.format("GRANT %s on %s to USER `user1`;", privilege, pathErr)));
        assertThrows(
            SemanticException.class,
            () ->
                createAuthDclStmt(
                    String.format("GRANT %s on %s to USER `user1`;", privilege, pathsErr)));
      }
    }

    grantRevokeCheck testRevoke =
        (privilege, username, isuser, targtePath, grantOpt) -> {
          String sql = "revoke %s on %s from %s %s";
          sql = String.format(sql, privilege, targtePath, isuser ? "USER" : "ROLE", username);
          AuthorStatement aclStmt = createAuthDclStmt(sql);
          assertEquals(
              isuser ? StatementType.REVOKE_USER_PRIVILEGE : StatementType.REVOKE_ROLE_PRIVILEGE,
              aclStmt.getType());
          assertEquals(path, aclStmt.getPaths().get(0).toString());
          assertEquals(username, isuser ? aclStmt.getUserName() : aclStmt.getRoleName());
          assertFalse(aclStmt.getGrantOpt());
          assertEquals(privilege, aclStmt.getPrivilegeList()[0]);
        };

    // 3. check simple privilege revoke from user/role on simple path
    for (PrivilegeType type : PrivilegeType.values()) {
      testRevoke.checkParser(type.toString(), name, true, path, false);
      testRevoke.checkParser(type.toString(), name, false, path, false);

      // 4. check system privilege revoke from user on wrong paths.
      if (!type.isPathRelevant()) {
        assertThrows(
            SemanticException.class,
            () ->
                createAuthDclStmt(
                    String.format("revoke %s on %s FROM USER `user1`;", type, pathErr)));
        assertThrows(
            SemanticException.class,
            () ->
                createAuthDclStmt(
                    String.format("revoke %s on %s FROM USER `user1`;", type, pathsErr)));
      }
    }
  }

  @Test
  public void testComplexGrantRevoke() {
    // 1. test complex privilege on single path :"root.**"
    Set<String> allPriv = new HashSet<>();
    for (PrivilegeType type : PrivilegeType.values()) {
      allPriv.add(type.toString());
    }

    for (PrivilegeType type : PrivilegeType.values()) {
      {
        AuthorStatement stmt =
            createAuthDclStmt(
                String.format("GRANT ALL,%s on root.** to USER `user1` with grant option", type));
        assertEquals(allPriv, new HashSet<>(Arrays.asList(stmt.getPrivilegeList())));
        Assert.assertTrue(stmt.getGrantOpt());
        assertEquals(StatementType.GRANT_USER_PRIVILEGE, stmt.getType());
      }
      {
        AuthorStatement stmt =
            createAuthDclStmt(String.format("REVOKE ALL,%s on root.** from USER `user1`;", type));
        assertEquals(allPriv, new HashSet<>(Arrays.asList(stmt.getPrivilegeList())));
        assertEquals(StatementType.REVOKE_USER_PRIVILEGE, stmt.getType());
      }
      {
        AuthorStatement stmt =
            createAuthDclStmt(String.format("GRANT ALL,%s on root.** to ROLE `role1`;", type));
        assertEquals(allPriv, new HashSet<>(Arrays.asList(stmt.getPrivilegeList())));
        assertFalse(stmt.getGrantOpt());
        assertEquals(StatementType.GRANT_ROLE_PRIVILEGE, stmt.getType());
      }
    }

    AuthorStatement stmt =
        createAuthDclStmt("GRANT ALL ON root.** to user `user1` with grant option;");
    assertEquals(allPriv, new HashSet<>(Arrays.asList(stmt.getPrivilegeList())));
    Assert.assertTrue(stmt.getGrantOpt());
    assertEquals(allPriv, new HashSet<>(Arrays.asList(stmt.getPrivilegeList())));

    // 2. complex privilege on a single wrong path
    assertThrows(
        SemanticException.class,
        () -> createAuthDclStmt("grant all on root.t1.** to USER `user1` with grant option;"));
    assertThrows(
        SemanticException.class,
        () -> createAuthDclStmt("grant all on root.t1.** to ROLE `user1` with grant option;"));
    assertThrows(
        SemanticException.class,
        () ->
            createAuthDclStmt(
                "grant all,READ_DATA on root.t1.** to USER `user1` with grant option"));
    assertThrows(
        SemanticException.class,
        () ->
            createAuthDclStmt(
                "grant all,READ_DATA on root.t1.** to ROLE `user1` with grant option"));
    assertThrows(
        SemanticException.class,
        () -> createAuthDclStmt("grant all on root.t1.** to USER `user1`;"));
    assertThrows(
        SemanticException.class,
        () -> createAuthDclStmt("grant all on root.t1.** to ROLE `user1`;"));
    assertThrows(
        SemanticException.class,
        () -> createAuthDclStmt("grant all,READ_DATA on root.t1.** to USER `user1`;"));
    assertThrows(
        SemanticException.class,
        () -> createAuthDclStmt("grant all,READ_DATA on root.t1.** to ROLE `user1`;"));
    assertThrows(
        SemanticException.class,
        () -> createAuthDclStmt("revoke all on root.t1.** from USER `user1`;"));
    assertThrows(
        SemanticException.class,
        () -> createAuthDclStmt("revoke all on root.t1.** from ROLE `user1`;"));
    assertThrows(
        SemanticException.class,
        () -> createAuthDclStmt("revoke all,READ_DATA on root.t1.** from USER `user1`;"));
    assertThrows(
        SemanticException.class,
        () -> createAuthDclStmt("revoke all,READ_DATA on root.t1.** from ROLE `user1`;"));
    try {
      createAuthDclStmt("grant all on root.t1.**, root.** to USER `user1` with grant option;");
    } catch (SemanticException e) {
      assertEquals("[ALL] can only be set on path: root.**", e.getMessage());
    }

    try {
      createAuthDclStmt("grant MANAGE_ROLE on root.t1.** to USER `user1` with grant option;");
    } catch (SemanticException e) {
      assertEquals("[MANAGE_ROLE] can only be set on path: root.**", e.getMessage());
    }

    // 3. complex privilege on complex paths.
    assertThrows(
        SemanticException.class,
        () ->
            createAuthDclStmt(
                "grant all on root.t1.**, root.** to USER `user1` with grant option;"));
    assertThrows(
        SemanticException.class,
        () ->
            createAuthDclStmt(
                "grant MANAGE_ROLE on root.t1.**, root.** to USER `user1` with grant option;"));

    // 4.  READ privilege can be parsed successfully.
    stmt = createAuthDclStmt("GRANT READ ON root.** TO USER `user1`;");
    Set<String> readSet = new HashSet<>();
    readSet.add("READ_DATA");
    readSet.add("READ_SCHEMA");
    assertEquals(readSet, new HashSet<>(Arrays.asList(stmt.getPrivilegeList())));

    stmt = createAuthDclStmt("GRANT READ,READ_DATA ON root.** TO USER `user1`;");
    assertEquals(readSet, new HashSet<>(Arrays.asList(stmt.getPrivilegeList())));

    stmt = createAuthDclStmt("GRANT READ,READ_DATA ON root.**,root.t1.t2 TO USER `user1`;");
    assertEquals(readSet, new HashSet<>(Arrays.asList(stmt.getPrivilegeList())));
    assertEquals(2, stmt.getPaths().size());

    // 5. WRITE privilege can be parsed successfully.
    stmt = createAuthDclStmt("GRANT WRITE ON root.** TO USER `user1`;");
    Set<String> writeSet = new HashSet<>();
    writeSet.add("WRITE_DATA");
    writeSet.add("WRITE_SCHEMA");
    assertEquals(writeSet, new HashSet<>(Arrays.asList(stmt.getPrivilegeList())));

    stmt = createAuthDclStmt("GRANT WRITE,WRITE_DATA ON root.** TO USER `user1`;");
    assertEquals(writeSet, new HashSet<>(Arrays.asList(stmt.getPrivilegeList())));

    stmt = createAuthDclStmt("GRANT WRITE,WRITE ON root.**,root.t1.t2 TO USER `user1`;");
    assertEquals(writeSet, new HashSet<>(Arrays.asList(stmt.getPrivilegeList())));
    assertEquals(2, stmt.getPaths().size());
  }

  @Test
  public void testListThings() {
    // 1. list user
    AuthorStatement stmt = createAuthDclStmt("LIST USER;");
    assertEquals(StatementType.LIST_USER, stmt.getType());
    assertEquals(null, stmt.getRoleName());

    // 2. list user of role
    stmt = createAuthDclStmt("LIST USER OF ROLE `role1`;");
    assertEquals(StatementType.LIST_USER, stmt.getType());
    assertEquals("role1", stmt.getRoleName());

    // 3. list role
    stmt = createAuthDclStmt("LIST ROLE;");
    assertEquals(StatementType.LIST_ROLE, stmt.getType());
    assertEquals(null, stmt.getUserName());

    // 4. list role of user
    stmt = createAuthDclStmt("LIST ROLE OF USER `user1`;");
    assertEquals(StatementType.LIST_ROLE, stmt.getType());
    assertEquals("user1", stmt.getUserName());

    // 5. list privileges of user
    stmt = createAuthDclStmt("LIST PRIVILEGES OF USER `user1`;");
    assertEquals(StatementType.LIST_USER_PRIVILEGE, stmt.getType());
    assertEquals("user1", stmt.getUserName());

    // 6. list privileges of role
    stmt = createAuthDclStmt("LIST PRIVILEGES OF ROLE `role1`;");
    assertEquals(StatementType.LIST_ROLE_PRIVILEGE, stmt.getType());
    assertEquals("role1", stmt.getRoleName());
  }

  private CreateLogicalViewStatement createViewStmt(String sql) {
    Statement stmt = StatementGenerator.createStatement(sql, ZonedDateTime.now().getOffset());
    CreateLogicalViewStatement viewStmt = (CreateLogicalViewStatement) stmt;
    return viewStmt;
  }

  @Test
  public void testCreateView() throws IllegalPathException {
    // 1. create with select
    CreateLogicalViewStatement stmt =
        createViewStmt("create view root.sg.view_dd as select s1 from root.sg.d1;");
    List<PartialPath> path = new ArrayList<>();
    path.add(new PartialPath("root.sg.d1"));
    assertEquals(null, stmt.getSourcePaths().fullPathList);
    assertEquals(path, stmt.getQueryStatement().getFromComponent().getPrefixPaths());

    // 2. create with path
    stmt = createViewStmt("create view root.sg as root.sg.d2;");
    List<PartialPath> path2 = new ArrayList<>();
    path2.add(new PartialPath("root.sg.d2"));
    assertEquals(path2, stmt.getSourcePaths().fullPathList);
    assertEquals(null, stmt.getQueryStatement());
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
