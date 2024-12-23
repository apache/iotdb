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
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory;
import org.apache.iotdb.commons.service.metric.PerformanceOverviewMetrics;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.qp.sql.IoTDBSqlParser;
import org.apache.iotdb.db.qp.sql.SqlLexer;
import org.apache.iotdb.db.queryengine.plan.analyze.cache.schema.DataNodeDevicePathCache;
import org.apache.iotdb.db.queryengine.plan.expression.binary.GreaterEqualExpression;
import org.apache.iotdb.db.queryengine.plan.expression.binary.LessThanExpression;
import org.apache.iotdb.db.queryengine.plan.expression.binary.LogicAndExpression;
import org.apache.iotdb.db.queryengine.plan.expression.leaf.ConstantOperand;
import org.apache.iotdb.db.queryengine.plan.expression.leaf.TimeSeriesOperand;
import org.apache.iotdb.db.queryengine.plan.expression.leaf.TimestampOperand;
import org.apache.iotdb.db.queryengine.plan.expression.multi.FunctionExpression;
import org.apache.iotdb.db.queryengine.plan.statement.Statement;
import org.apache.iotdb.db.queryengine.plan.statement.component.FromComponent;
import org.apache.iotdb.db.queryengine.plan.statement.component.GroupByTimeComponent;
import org.apache.iotdb.db.queryengine.plan.statement.component.ResultColumn;
import org.apache.iotdb.db.queryengine.plan.statement.component.SelectComponent;
import org.apache.iotdb.db.queryengine.plan.statement.component.WhereCondition;
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
import org.apache.iotdb.db.queryengine.plan.statement.metadata.template.SetSchemaTemplateStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.template.ShowNodesInSchemaTemplateStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.template.ShowPathSetTemplateStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.template.ShowPathsUsingTemplateStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.template.ShowSchemaTemplateStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.template.UnsetSchemaTemplateStatement;
import org.apache.iotdb.db.schemaengine.schemaregion.utils.MetaFormatUtils;
import org.apache.iotdb.db.schemaengine.template.TemplateQueryType;
import org.apache.iotdb.db.utils.QueryDataSetUtils;
import org.apache.iotdb.db.utils.TimestampPrecisionUtils;
import org.apache.iotdb.db.utils.constant.SqlConstant;
import org.apache.iotdb.mpp.rpc.thrift.TFetchTimeseriesReq;
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
import org.apache.iotdb.service.rpc.thrift.TSInsertStringRecordReq;
import org.apache.iotdb.service.rpc.thrift.TSInsertStringRecordsOfOneDeviceReq;
import org.apache.iotdb.service.rpc.thrift.TSInsertStringRecordsReq;
import org.apache.iotdb.service.rpc.thrift.TSInsertTabletReq;
import org.apache.iotdb.service.rpc.thrift.TSInsertTabletsReq;
import org.apache.iotdb.service.rpc.thrift.TSLastDataQueryReq;
import org.apache.iotdb.service.rpc.thrift.TSQueryTemplateReq;
import org.apache.iotdb.service.rpc.thrift.TSRawDataQueryReq;
import org.apache.iotdb.service.rpc.thrift.TSSetSchemaTemplateReq;
import org.apache.iotdb.service.rpc.thrift.TSUnsetSchemaTemplateReq;

import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.atn.PredictionMode;
import org.antlr.v4.runtime.tree.ParseTree;
import org.apache.tsfile.common.constant.TsFileConstant;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.apache.tsfile.utils.TimeDuration;
import org.apache.tsfile.write.record.Tablet.ColumnCategory;

import java.nio.ByteBuffer;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Convert SQL and RPC requests to {@link Statement}. */
public class StatementGenerator {

  private static final PerformanceOverviewMetrics PERFORMANCE_OVERVIEW_METRICS =
      PerformanceOverviewMetrics.getInstance();

  private static final DataNodeDevicePathCache DEVICE_PATH_CACHE =
      DataNodeDevicePathCache.getInstance();

  private StatementGenerator() {
    // forbidding instantiation
  }

  public static Statement createStatement(String sql, ZoneId zoneId) {
    return invokeParser(sql, zoneId);
  }

  public static Statement createStatement(TSRawDataQueryReq rawDataQueryReq)
      throws IllegalPathException {
    final long startTime = System.nanoTime();
    // construct query statement
    SelectComponent selectComponent = new SelectComponent();
    FromComponent fromComponent = new FromComponent();
    WhereCondition whereCondition = new WhereCondition();

    // iterate the path list and add it to from operator
    for (String pathStr : rawDataQueryReq.getPaths()) {
      PartialPath path;
      if (rawDataQueryReq.isLegalPathNodes()) {
        path = new PartialPath(pathStr.split("\\."));
      } else {
        path = new PartialPath(pathStr);
      }
      fromComponent.addPrefixPath(path);
    }
    selectComponent.addResultColumn(
        new ResultColumn(
            new TimeSeriesOperand(new PartialPath("", false)), ResultColumn.ColumnType.RAW));

    // set query filter
    GreaterEqualExpression leftPredicate =
        new GreaterEqualExpression(
            new TimestampOperand(),
            new ConstantOperand(TSDataType.INT64, Long.toString(rawDataQueryReq.getStartTime())));
    LessThanExpression rightPredicate =
        new LessThanExpression(
            new TimestampOperand(),
            new ConstantOperand(TSDataType.INT64, Long.toString(rawDataQueryReq.getEndTime())));
    LogicAndExpression predicate = new LogicAndExpression(leftPredicate, rightPredicate);
    whereCondition.setPredicate(predicate);

    QueryStatement queryStatement = new QueryStatement();
    queryStatement.setSelectComponent(selectComponent);
    queryStatement.setFromComponent(fromComponent);
    queryStatement.setWhereCondition(whereCondition);
    PERFORMANCE_OVERVIEW_METRICS.recordParseCost(System.nanoTime() - startTime);
    return queryStatement;
  }

  public static Statement createStatement(TSLastDataQueryReq lastDataQueryReq)
      throws IllegalPathException {
    final long startTime = System.nanoTime();
    // construct query statement
    SelectComponent selectComponent = new SelectComponent();
    FromComponent fromComponent = new FromComponent();

    selectComponent.setHasLast(true);

    // iterate the path list and add it to from operator
    for (String pathStr : lastDataQueryReq.getPaths()) {
      PartialPath path;
      if (lastDataQueryReq.isLegalPathNodes()) {
        path = new PartialPath(pathStr.split("\\."));
      } else {
        path = new PartialPath(pathStr);
      }
      fromComponent.addPrefixPath(path);
    }
    selectComponent.addResultColumn(
        new ResultColumn(
            new TimeSeriesOperand(new PartialPath("", false)), ResultColumn.ColumnType.RAW));

    QueryStatement lastQueryStatement = new QueryStatement();

    if (lastDataQueryReq.getTime() != Long.MIN_VALUE) {
      // set query filter
      WhereCondition whereCondition = new WhereCondition();
      GreaterEqualExpression predicate =
          new GreaterEqualExpression(
              new TimestampOperand(),
              new ConstantOperand(TSDataType.INT64, Long.toString(lastDataQueryReq.getTime())));
      whereCondition.setPredicate(predicate);
      lastQueryStatement.setWhereCondition(whereCondition);
    }

    lastQueryStatement.setSelectComponent(selectComponent);
    lastQueryStatement.setFromComponent(fromComponent);
    PERFORMANCE_OVERVIEW_METRICS.recordParseCost(System.nanoTime() - startTime);

    return lastQueryStatement;
  }

  public static Statement createStatement(TSAggregationQueryReq req) throws IllegalPathException {
    final long startTime = System.nanoTime();
    QueryStatement queryStatement = new QueryStatement();

    FromComponent fromComponent = new FromComponent();
    fromComponent.addPrefixPath(new PartialPath("", false));
    queryStatement.setFromComponent(fromComponent);

    SelectComponent selectComponent = new SelectComponent();
    List<PartialPath> selectPaths = new ArrayList<>();
    for (String pathStr : req.getPaths()) {
      if (req.isLegalPathNodes()) {
        selectPaths.add(new PartialPath(pathStr.split("\\.")));
      } else {
        selectPaths.add(new PartialPath(pathStr));
      }
    }
    List<TAggregationType> aggregations = req.getAggregations();
    for (int i = 0; i < aggregations.size(); i++) {
      selectComponent.addResultColumn(
          new ResultColumn(
              new FunctionExpression(
                  aggregations.get(i).toString(),
                  new LinkedHashMap<>(),
                  Collections.singletonList(new TimeSeriesOperand(selectPaths.get(i)))),
              ResultColumn.ColumnType.AGGREGATION));
    }
    queryStatement.setSelectComponent(selectComponent);

    if (req.isSetInterval()) {
      GroupByTimeComponent groupByTimeComponent = new GroupByTimeComponent();
      groupByTimeComponent.setStartTime(req.getStartTime());
      groupByTimeComponent.setEndTime(req.getEndTime());
      groupByTimeComponent.setInterval(new TimeDuration(0, req.getInterval()));
      if (req.isSetSlidingStep()) {
        groupByTimeComponent.setSlidingStep(new TimeDuration(0, req.getSlidingStep()));
      } else {
        groupByTimeComponent.setSlidingStep(groupByTimeComponent.getInterval());
      }
      queryStatement.setGroupByTimeComponent(groupByTimeComponent);
    } else if (req.isSetStartTime()) {
      WhereCondition whereCondition = new WhereCondition();
      GreaterEqualExpression leftPredicate =
          new GreaterEqualExpression(
              new TimestampOperand(),
              new ConstantOperand(TSDataType.INT64, Long.toString(req.getStartTime())));
      LessThanExpression rightPredicate =
          new LessThanExpression(
              new TimestampOperand(),
              new ConstantOperand(TSDataType.INT64, Long.toString(req.getEndTime())));
      LogicAndExpression predicate = new LogicAndExpression(leftPredicate, rightPredicate);
      whereCondition.setPredicate(predicate);
      queryStatement.setWhereCondition(whereCondition);
    }
    PERFORMANCE_OVERVIEW_METRICS.recordParseCost(System.nanoTime() - startTime);
    return queryStatement;
  }

  public static InsertRowStatement createStatement(TSInsertRecordReq insertRecordReq)
      throws IllegalPathException, QueryProcessException {
    final long startTime = System.nanoTime();
    // construct insert statement
    InsertRowStatement insertStatement = new InsertRowStatement();
    insertStatement.setDevicePath(
        DEVICE_PATH_CACHE.getPartialPath(insertRecordReq.getPrefixPath()));
    TimestampPrecisionUtils.checkTimestampPrecision(insertRecordReq.getTimestamp());
    insertStatement.setTime(insertRecordReq.getTimestamp());
    insertStatement.setMeasurements(insertRecordReq.getMeasurements().toArray(new String[0]));
    insertStatement.setAligned(insertRecordReq.isAligned);
    insertStatement.fillValues(insertRecordReq.values);
    if (insertRecordReq.isSetIsWriteToTable()) {
      insertStatement.setWriteToTable(insertRecordReq.isIsWriteToTable());
      if (!insertRecordReq.isSetColumnCategoryies()
          || insertRecordReq.getColumnCategoryiesSize() != insertRecordReq.getMeasurementsSize()) {
        throw new IllegalArgumentException(
            "Missing or invalid column categories for table " + "insertion");
      }
      TsTableColumnCategory[] columnCategories =
          new TsTableColumnCategory[insertRecordReq.getColumnCategoryies().size()];
      for (int i = 0; i < columnCategories.length; i++) {
        columnCategories[i] =
            TsTableColumnCategory.fromTsFileColumnType(
                ColumnCategory.values()[insertRecordReq.getColumnCategoryies().get(i).intValue()]);
      }
      insertStatement.setColumnCategories(columnCategories);
    }
    PERFORMANCE_OVERVIEW_METRICS.recordParseCost(System.nanoTime() - startTime);
    return insertStatement;
  }

  public static InsertRowStatement createStatement(TSInsertStringRecordReq insertRecordReq)
      throws IllegalPathException {
    final long startTime = System.nanoTime();
    // construct insert statement
    InsertRowStatement insertStatement = new InsertRowStatement();
    insertStatement.setDevicePath(
        DEVICE_PATH_CACHE.getPartialPath(insertRecordReq.getPrefixPath()));
    TimestampPrecisionUtils.checkTimestampPrecision(insertRecordReq.getTimestamp());
    insertStatement.setTime(insertRecordReq.getTimestamp());
    insertStatement.setMeasurements(insertRecordReq.getMeasurements().toArray(new String[0]));
    insertStatement.setDataTypes(new TSDataType[insertStatement.getMeasurements().length]);
    insertStatement.setValues(insertRecordReq.getValues().toArray(new Object[0]));
    insertStatement.setNeedInferType(true);
    insertStatement.setAligned(insertRecordReq.isAligned);
    PERFORMANCE_OVERVIEW_METRICS.recordParseCost(System.nanoTime() - startTime);
    return insertStatement;
  }

  public static InsertTabletStatement createStatement(TSInsertTabletReq insertTabletReq)
      throws IllegalPathException {
    final long startTime = System.nanoTime();
    // construct insert statement
    InsertTabletStatement insertStatement = new InsertTabletStatement();
    insertStatement.setDevicePath(
        DEVICE_PATH_CACHE.getPartialPath(insertTabletReq.getPrefixPath()));
    insertStatement.setMeasurements(insertTabletReq.getMeasurements().toArray(new String[0]));
    long[] timestamps =
        QueryDataSetUtils.readTimesFromBuffer(insertTabletReq.timestamps, insertTabletReq.size);
    if (timestamps.length != 0) {
      TimestampPrecisionUtils.checkTimestampPrecision(timestamps[timestamps.length - 1]);
    }
    insertStatement.setTimes(timestamps);
    insertStatement.setColumns(
        QueryDataSetUtils.readTabletValuesFromBuffer(
            insertTabletReq.values,
            insertTabletReq.types,
            insertTabletReq.types.size(),
            insertTabletReq.size));
    insertStatement.setBitMaps(
        QueryDataSetUtils.readBitMapsFromBuffer(
                insertTabletReq.values, insertTabletReq.types.size(), insertTabletReq.size)
            .orElse(null));
    insertStatement.setRowCount(insertTabletReq.size);
    TSDataType[] dataTypes = new TSDataType[insertTabletReq.types.size()];
    for (int i = 0; i < insertTabletReq.types.size(); i++) {
      dataTypes[i] = TSDataType.deserialize((byte) insertTabletReq.types.get(i).intValue());
    }
    insertStatement.setDataTypes(dataTypes);
    insertStatement.setAligned(insertTabletReq.isAligned);
    insertStatement.setWriteToTable(insertTabletReq.isWriteToTable());
    if (insertTabletReq.isWriteToTable()) {
      if (!insertTabletReq.isSetColumnCategories()
          || insertTabletReq.getColumnCategoriesSize() != insertTabletReq.getMeasurementsSize()) {
        throw new IllegalArgumentException(
            "Missing or invalid column categories for table " + "insertion");
      }
      TsTableColumnCategory[] columnCategories =
          new TsTableColumnCategory[insertTabletReq.columnCategories.size()];
      for (int i = 0; i < columnCategories.length; i++) {
        columnCategories[i] =
            TsTableColumnCategory.fromTsFileColumnType(
                ColumnCategory.values()[insertTabletReq.columnCategories.get(i).intValue()]);
      }
      insertStatement.setColumnCategories(columnCategories);
    }
    PERFORMANCE_OVERVIEW_METRICS.recordParseCost(System.nanoTime() - startTime);
    return insertStatement;
  }

  public static InsertMultiTabletsStatement createStatement(TSInsertTabletsReq req)
      throws IllegalPathException {
    final long startTime = System.nanoTime();
    // construct insert statement
    InsertMultiTabletsStatement insertStatement = new InsertMultiTabletsStatement();
    List<InsertTabletStatement> insertTabletStatementList = new ArrayList<>();
    for (int i = 0; i < req.prefixPaths.size(); i++) {
      InsertTabletStatement insertTabletStatement = new InsertTabletStatement();
      insertTabletStatement.setDevicePath(DEVICE_PATH_CACHE.getPartialPath(req.prefixPaths.get(i)));
      insertTabletStatement.setMeasurements(req.measurementsList.get(i).toArray(new String[0]));
      long[] timestamps =
          QueryDataSetUtils.readTimesFromBuffer(req.timestampsList.get(i), req.sizeList.get(i));
      if (timestamps.length != 0) {
        TimestampPrecisionUtils.checkTimestampPrecision(timestamps[timestamps.length - 1]);
      }
      insertTabletStatement.setTimes(timestamps);
      insertTabletStatement.setColumns(
          QueryDataSetUtils.readTabletValuesFromBuffer(
              req.valuesList.get(i),
              req.typesList.get(i),
              req.measurementsList.get(i).size(),
              req.sizeList.get(i)));
      insertTabletStatement.setBitMaps(
          QueryDataSetUtils.readBitMapsFromBuffer(
                  req.valuesList.get(i), req.measurementsList.get(i).size(), req.sizeList.get(i))
              .orElse(null));
      insertTabletStatement.setRowCount(req.sizeList.get(i));
      TSDataType[] dataTypes = new TSDataType[req.typesList.get(i).size()];
      for (int j = 0; j < dataTypes.length; j++) {
        dataTypes[j] = TSDataType.deserialize((byte) req.typesList.get(i).get(j).intValue());
      }
      insertTabletStatement.setDataTypes(dataTypes);
      insertTabletStatement.setAligned(req.isAligned);
      // skip empty tablet
      if (insertTabletStatement.isEmpty()) {
        continue;
      }
      insertTabletStatementList.add(insertTabletStatement);
    }
    insertStatement.setInsertTabletStatementList(insertTabletStatementList);
    PERFORMANCE_OVERVIEW_METRICS.recordParseCost(System.nanoTime() - startTime);
    return insertStatement;
  }

  public static InsertRowsStatement createStatement(TSInsertRecordsReq req)
      throws IllegalPathException, QueryProcessException {
    final long startTime = System.nanoTime();
    // construct insert statement
    InsertRowsStatement insertStatement = new InsertRowsStatement();
    List<InsertRowStatement> insertRowStatementList = new ArrayList<>();
    for (int i = 0; i < req.prefixPaths.size(); i++) {
      InsertRowStatement statement = new InsertRowStatement();
      statement.setDevicePath(DEVICE_PATH_CACHE.getPartialPath(req.getPrefixPaths().get(i)));
      statement.setMeasurements(req.getMeasurementsList().get(i).toArray(new String[0]));
      TimestampPrecisionUtils.checkTimestampPrecision(req.getTimestamps().get(i));
      statement.setTime(req.getTimestamps().get(i));
      statement.fillValues(req.valuesList.get(i));
      statement.setAligned(req.isAligned);
      // skip empty statement
      if (statement.isEmpty()) {
        continue;
      }
      insertRowStatementList.add(statement);
    }
    insertStatement.setInsertRowStatementList(insertRowStatementList);
    PERFORMANCE_OVERVIEW_METRICS.recordParseCost(System.nanoTime() - startTime);
    return insertStatement;
  }

  public static InsertRowsStatement createStatement(TSInsertStringRecordsReq req)
      throws IllegalPathException {
    final long startTime = System.nanoTime();
    // construct insert statement
    InsertRowsStatement insertStatement = new InsertRowsStatement();
    List<InsertRowStatement> insertRowStatementList = new ArrayList<>();
    for (int i = 0; i < req.prefixPaths.size(); i++) {
      InsertRowStatement statement = new InsertRowStatement();
      statement.setDevicePath(DEVICE_PATH_CACHE.getPartialPath(req.getPrefixPaths().get(i)));
      addMeasurementAndValue(
          statement, req.getMeasurementsList().get(i), req.getValuesList().get(i));
      statement.setDataTypes(new TSDataType[statement.getMeasurements().length]);
      TimestampPrecisionUtils.checkTimestampPrecision(req.getTimestamps().get(i));
      statement.setTime(req.getTimestamps().get(i));
      statement.setNeedInferType(true);
      statement.setAligned(req.isAligned);
      // skip empty statement
      if (statement.isEmpty()) {
        continue;
      }
      insertRowStatementList.add(statement);
    }
    insertStatement.setInsertRowStatementList(insertRowStatementList);
    PERFORMANCE_OVERVIEW_METRICS.recordParseCost(System.nanoTime() - startTime);
    return insertStatement;
  }

  public static InsertRowsOfOneDeviceStatement createStatement(TSInsertRecordsOfOneDeviceReq req)
      throws IllegalPathException, QueryProcessException {
    final long startTime = System.nanoTime();
    // construct insert statement
    InsertRowsOfOneDeviceStatement insertStatement = new InsertRowsOfOneDeviceStatement();
    insertStatement.setDevicePath(DEVICE_PATH_CACHE.getPartialPath(req.prefixPath));
    List<InsertRowStatement> insertRowStatementList = new ArrayList<>();
    // req.timestamps sorted on session side
    if (req.timestamps.size() != 0) {
      TimestampPrecisionUtils.checkTimestampPrecision(
          req.timestamps.get(req.timestamps.size() - 1));
    }
    for (int i = 0; i < req.timestamps.size(); i++) {
      InsertRowStatement statement = new InsertRowStatement();
      statement.setDevicePath(insertStatement.getDevicePath());
      statement.setMeasurements(req.measurementsList.get(i).toArray(new String[0]));
      statement.setTime(req.timestamps.get(i));
      statement.fillValues(req.valuesList.get(i));
      statement.setAligned(req.isAligned);
      // skip empty statement
      if (statement.isEmpty()) {
        continue;
      }
      insertRowStatementList.add(statement);
    }
    insertStatement.setInsertRowStatementList(insertRowStatementList);
    PERFORMANCE_OVERVIEW_METRICS.recordParseCost(System.nanoTime() - startTime);
    return insertStatement;
  }

  public static InsertRowsOfOneDeviceStatement createStatement(
      TSInsertStringRecordsOfOneDeviceReq req) throws IllegalPathException {
    final long startTime = System.nanoTime();
    // construct insert statement
    InsertRowsOfOneDeviceStatement insertStatement = new InsertRowsOfOneDeviceStatement();
    insertStatement.setDevicePath(DEVICE_PATH_CACHE.getPartialPath(req.prefixPath));
    List<InsertRowStatement> insertRowStatementList = new ArrayList<>();
    // req.timestamps sorted on session side
    TimestampPrecisionUtils.checkTimestampPrecision(req.timestamps.get(req.timestamps.size() - 1));
    for (int i = 0; i < req.timestamps.size(); i++) {
      InsertRowStatement statement = new InsertRowStatement();
      statement.setDevicePath(insertStatement.getDevicePath());
      addMeasurementAndValue(
          statement, req.getMeasurementsList().get(i), req.getValuesList().get(i));
      statement.setDataTypes(new TSDataType[statement.getMeasurements().length]);
      statement.setTime(req.timestamps.get(i));
      statement.setNeedInferType(true);
      statement.setAligned(req.isAligned);
      // skip empty statement
      if (statement.isEmpty()) {
        continue;
      }
      insertRowStatementList.add(statement);
    }
    insertStatement.setInsertRowStatementList(insertRowStatementList);
    PERFORMANCE_OVERVIEW_METRICS.recordParseCost(System.nanoTime() - startTime);
    return insertStatement;
  }

  public static DatabaseSchemaStatement createStatement(final String database)
      throws IllegalPathException {
    final long startTime = System.nanoTime();
    // construct create database statement
    final DatabaseSchemaStatement statement =
        new DatabaseSchemaStatement(DatabaseSchemaStatement.DatabaseSchemaStatementType.CREATE);
    statement.setDatabasePath(parseDatabaseRawString(database));
    PERFORMANCE_OVERVIEW_METRICS.recordParseCost(System.nanoTime() - startTime);
    return statement;
  }

  public static CreateTimeSeriesStatement createStatement(TSCreateTimeseriesReq req)
      throws IllegalPathException {
    final long startTime = System.nanoTime();
    // construct create timeseries statement
    CreateTimeSeriesStatement statement = new CreateTimeSeriesStatement();
    statement.setPath(new MeasurementPath(req.path));
    statement.setDataType(TSDataType.deserialize((byte) req.dataType));
    statement.setEncoding(TSEncoding.deserialize((byte) req.encoding));
    statement.setCompressor(CompressionType.deserialize((byte) req.compressor));
    statement.setProps(req.props);
    statement.setTags(req.tags);
    statement.setAttributes(req.attributes);
    statement.setAlias(req.measurementAlias);
    PERFORMANCE_OVERVIEW_METRICS.recordParseCost(System.nanoTime() - startTime);
    return statement;
  }

  public static CreateAlignedTimeSeriesStatement createStatement(TSCreateAlignedTimeseriesReq req)
      throws IllegalPathException {
    final long startTime = System.nanoTime();
    // construct create aligned time series statement
    CreateAlignedTimeSeriesStatement statement = new CreateAlignedTimeSeriesStatement();
    statement.setDevicePath(DEVICE_PATH_CACHE.getPartialPath(req.prefixPath));
    List<TSDataType> dataTypes = new ArrayList<>();
    for (Integer dataType : req.dataTypes) {
      dataTypes.add(TSDataType.deserialize(dataType.byteValue()));
    }
    List<TSEncoding> encodings = new ArrayList<>();
    for (Integer encoding : req.encodings) {
      encodings.add(TSEncoding.deserialize(encoding.byteValue()));
    }
    List<CompressionType> compressors = new ArrayList<>();
    for (Integer compressor : req.compressors) {
      compressors.add(CompressionType.deserialize(compressor.byteValue()));
    }
    statement.setMeasurements(req.measurements);
    statement.setDataTypes(dataTypes);
    statement.setEncodings(encodings);
    statement.setCompressors(compressors);
    statement.setTagsList(req.tagsList);
    statement.setAttributesList(req.attributesList);
    statement.setAliasList(req.measurementAlias);
    PERFORMANCE_OVERVIEW_METRICS.recordParseCost(System.nanoTime() - startTime);
    return statement;
  }

  public static CreateMultiTimeSeriesStatement createStatement(TSCreateMultiTimeseriesReq req)
      throws IllegalPathException {
    final long startTime = System.nanoTime();
    // construct create multi timeseries statement
    List<MeasurementPath> paths = new ArrayList<>();
    for (String path : req.paths) {
      paths.add(new MeasurementPath(path));
    }
    List<TSDataType> dataTypes = new ArrayList<>();
    for (Integer dataType : req.dataTypes) {
      dataTypes.add(TSDataType.deserialize(dataType.byteValue()));
    }
    List<TSEncoding> encodings = new ArrayList<>();
    for (Integer encoding : req.encodings) {
      encodings.add(TSEncoding.deserialize(encoding.byteValue()));
    }
    List<CompressionType> compressors = new ArrayList<>();
    for (Integer compressor : req.compressors) {
      compressors.add(CompressionType.deserialize(compressor.byteValue()));
    }
    CreateMultiTimeSeriesStatement statement = new CreateMultiTimeSeriesStatement();
    statement.setPaths(paths);
    statement.setDataTypes(dataTypes);
    statement.setEncodings(encodings);
    statement.setCompressors(compressors);
    statement.setPropsList(req.propsList);
    statement.setTagsList(req.tagsList);
    statement.setAttributesList(req.attributesList);
    statement.setAliasList(req.measurementAliasList);
    PERFORMANCE_OVERVIEW_METRICS.recordParseCost(System.nanoTime() - startTime);
    return statement;
  }

  public static DeleteDatabaseStatement createStatement(List<String> databases)
      throws IllegalPathException {
    final long startTime = System.nanoTime();
    DeleteDatabaseStatement statement = new DeleteDatabaseStatement();
    for (String path : databases) {
      parseDatabaseRawString(path);
    }
    statement.setPrefixPath(databases);
    PERFORMANCE_OVERVIEW_METRICS.recordParseCost(System.nanoTime() - startTime);
    return statement;
  }

  public static DeleteDataStatement createStatement(TSDeleteDataReq req)
      throws IllegalPathException {
    final long startTime = System.nanoTime();
    DeleteDataStatement statement = new DeleteDataStatement();
    List<MeasurementPath> pathList = new ArrayList<>();
    for (String path : req.getPaths()) {
      pathList.add(new MeasurementPath(path));
    }
    statement.setPathList(pathList);
    statement.setDeleteStartTime(req.getStartTime());
    statement.setDeleteEndTime(req.getEndTime());
    PERFORMANCE_OVERVIEW_METRICS.recordParseCost(System.nanoTime() - startTime);
    return statement;
  }

  public static CreateSchemaTemplateStatement createStatement(TSCreateSchemaTemplateReq req)
      throws MetadataException {
    final long startTime = System.nanoTime();
    ByteBuffer buffer = ByteBuffer.wrap(req.getSerializedTemplate());
    Map<String, List<String>> alignedPrefix = new HashMap<>();
    Map<String, List<TSDataType>> alignedDataTypes = new HashMap<>();
    Map<String, List<TSEncoding>> alignedEncodings = new HashMap<>();
    Map<String, List<CompressionType>> alignedCompressions = new HashMap<>();

    List<List<String>> measurements = new ArrayList<>();
    List<List<TSDataType>> dataTypes = new ArrayList<>();
    List<List<TSEncoding>> encodings = new ArrayList<>();
    List<List<CompressionType>> compressors = new ArrayList<>();

    ReadWriteIOUtils.readString(buffer); // skip template name
    boolean isAlign = ReadWriteIOUtils.readBool(buffer);
    if (isAlign) {
      alignedPrefix.put("", new ArrayList<>());
      alignedDataTypes.put("", new ArrayList<>());
      alignedEncodings.put("", new ArrayList<>());
      alignedCompressions.put("", new ArrayList<>());
    }

    while (buffer.position() != buffer.limit()) {
      String prefix = ReadWriteIOUtils.readString(buffer);
      isAlign = ReadWriteIOUtils.readBool(buffer);
      String measurementName = ReadWriteIOUtils.readString(buffer);
      TSDataType dataType = TSDataType.deserialize(ReadWriteIOUtils.readByte(buffer));
      TSEncoding encoding = TSEncoding.deserialize(ReadWriteIOUtils.readByte(buffer));
      CompressionType compressionType =
          CompressionType.deserialize(ReadWriteIOUtils.readByte(buffer));

      if (measurementName == null) {
        throw new MetadataException(
            "The name of a measurement in schema template shall not be null.");
      }

      if (alignedPrefix.containsKey(prefix) && !isAlign) {
        throw new MetadataException("Align designation incorrect at: " + prefix);
      }

      if (isAlign && !alignedPrefix.containsKey(prefix)) {
        alignedPrefix.put(prefix, new ArrayList<>());
        alignedDataTypes.put(prefix, new ArrayList<>());
        alignedEncodings.put(prefix, new ArrayList<>());
        alignedCompressions.put(prefix, new ArrayList<>());
      }

      if (alignedPrefix.containsKey(prefix)) {
        alignedPrefix.get(prefix).add(measurementName);
        alignedDataTypes.get(prefix).add(dataType);
        alignedEncodings.get(prefix).add(encoding);
        alignedCompressions.get(prefix).add(compressionType);
      } else {
        if ("".equals(prefix)) {
          measurements.add(Collections.singletonList(measurementName));
        } else {
          measurements.add(
              Collections.singletonList(prefix + TsFileConstant.PATH_SEPARATOR + measurementName));
        }
        dataTypes.add(Collections.singletonList(dataType));
        encodings.add(Collections.singletonList(encoding));
        compressors.add(Collections.singletonList(compressionType));
      }
    }

    for (Map.Entry<String, List<String>> alignedPrefixEntry : alignedPrefix.entrySet()) {
      String prefix = alignedPrefixEntry.getKey();
      List<String> alignedMeasurements = alignedPrefixEntry.getValue();

      List<String> thisMeasurements = new ArrayList<>();
      List<TSDataType> thisDataTypes = new ArrayList<>();
      List<TSEncoding> thisEncodings = new ArrayList<>();
      List<CompressionType> thisCompressors = new ArrayList<>();

      for (int i = 0; i < alignedMeasurements.size(); i++) {
        if ("".equals(prefix)) {
          thisMeasurements.add(alignedMeasurements.get(i));
        } else {
          thisMeasurements.add(prefix + TsFileConstant.PATH_SEPARATOR + alignedMeasurements.get(i));
        }
        thisDataTypes.add(alignedDataTypes.get(prefix).get(i));
        thisEncodings.add(alignedEncodings.get(prefix).get(i));
        thisCompressors.add(alignedCompressions.get(prefix).get(i));
      }

      measurements.add(thisMeasurements);
      dataTypes.add(thisDataTypes);
      encodings.add(thisEncodings);
      compressors.add(thisCompressors);
    }

    CreateSchemaTemplateStatement statement =
        new CreateSchemaTemplateStatement(
            req.getName(), measurements, dataTypes, encodings, compressors, isAlign);
    PERFORMANCE_OVERVIEW_METRICS.recordParseCost(System.nanoTime() - startTime);
    return statement;
  }

  public static Statement createStatement(TSQueryTemplateReq req) {
    long startTime = System.nanoTime();
    Statement result = null;
    switch (TemplateQueryType.values()[req.getQueryType()]) {
      case SHOW_MEASUREMENTS:
        result = new ShowNodesInSchemaTemplateStatement(req.getName());
        break;
      case SHOW_TEMPLATES:
        result = new ShowSchemaTemplateStatement();
        break;
      case SHOW_SET_TEMPLATES:
        result = new ShowPathSetTemplateStatement(req.getName());
        break;
      case SHOW_USING_TEMPLATES:
        result =
            new ShowPathsUsingTemplateStatement(
                new PartialPath(SqlConstant.getSingleRootArray()), req.getName());
        break;
      default:
        break;
    }
    PERFORMANCE_OVERVIEW_METRICS.recordParseCost(System.nanoTime() - startTime);
    return result;
  }

  public static SetSchemaTemplateStatement createStatement(TSSetSchemaTemplateReq req)
      throws IllegalPathException {
    long startTime = System.nanoTime();
    SetSchemaTemplateStatement statement =
        new SetSchemaTemplateStatement(req.getTemplateName(), new PartialPath(req.getPrefixPath()));
    PERFORMANCE_OVERVIEW_METRICS.recordParseCost(System.nanoTime() - startTime);
    return statement;
  }

  public static UnsetSchemaTemplateStatement createStatement(TSUnsetSchemaTemplateReq req)
      throws IllegalPathException {
    final long startTime = System.nanoTime();
    UnsetSchemaTemplateStatement statement =
        new UnsetSchemaTemplateStatement(
            req.getTemplateName(), new PartialPath(req.getPrefixPath()));
    PERFORMANCE_OVERVIEW_METRICS.recordParseCost(System.nanoTime() - startTime);
    return statement;
  }

  public static DropSchemaTemplateStatement createStatement(TSDropSchemaTemplateReq req) {
    final long startTime = System.nanoTime();
    DropSchemaTemplateStatement statement = new DropSchemaTemplateStatement(req.getTemplateName());
    PERFORMANCE_OVERVIEW_METRICS.recordParseCost(System.nanoTime() - startTime);
    return statement;
  }

  public static BatchActivateTemplateStatement createBatchActivateTemplateStatement(
      List<String> devicePathStringList) throws IllegalPathException {
    final long startTime = System.nanoTime();
    List<PartialPath> devicePathList = new ArrayList<>(devicePathStringList.size());
    for (String pathString : devicePathStringList) {
      devicePathList.add(new PartialPath(pathString));
    }
    BatchActivateTemplateStatement statement = new BatchActivateTemplateStatement(devicePathList);
    PERFORMANCE_OVERVIEW_METRICS.recordParseCost(System.nanoTime() - startTime);
    return statement;
  }

  public static DeleteTimeSeriesStatement createDeleteTimeSeriesStatement(
      List<String> pathPatternStringList) throws IllegalPathException {
    final long startTime = System.nanoTime();
    List<PartialPath> pathPatternList = new ArrayList<>();
    for (String pathPatternString : pathPatternStringList) {
      pathPatternList.add(new PartialPath(pathPatternString));
    }
    DeleteTimeSeriesStatement statement = new DeleteTimeSeriesStatement(pathPatternList);
    PERFORMANCE_OVERVIEW_METRICS.recordParseCost(System.nanoTime() - startTime);
    return statement;
  }

  private static Statement invokeParser(String sql, ZoneId zoneId) {
    long startTime = System.nanoTime();
    try {
      ASTVisitor astVisitor = new ASTVisitor();
      astVisitor.setZoneId(zoneId);

      CharStream charStream1 = CharStreams.fromString(sql);

      SqlLexer lexer1 = new SqlLexer(charStream1);
      lexer1.removeErrorListeners();
      lexer1.addErrorListener(SqlParseError.INSTANCE);

      CommonTokenStream tokens1 = new CommonTokenStream(lexer1);

      IoTDBSqlParser parser1 = new IoTDBSqlParser(tokens1);
      parser1.getInterpreter().setPredictionMode(PredictionMode.SLL);
      parser1.removeErrorListeners();
      parser1.addErrorListener(SqlParseError.INSTANCE);

      ParseTree tree;
      try {
        // STAGE 1: try with simpler/faster SLL(*)
        tree = parser1.singleStatement();
        // if we get here, there was no syntax error and SLL(*) was enough; there is no need to try
        // full LL(*)
      } catch (Exception ex) {
        CharStream charStream2 = CharStreams.fromString(sql);

        SqlLexer lexer2 = new SqlLexer(charStream2);
        lexer2.removeErrorListeners();
        lexer2.addErrorListener(SqlParseError.INSTANCE);

        CommonTokenStream tokens2 = new CommonTokenStream(lexer2);

        org.apache.iotdb.db.qp.sql.IoTDBSqlParser parser2 =
            new org.apache.iotdb.db.qp.sql.IoTDBSqlParser(tokens2);
        parser2.getInterpreter().setPredictionMode(PredictionMode.LL);
        parser2.removeErrorListeners();
        parser2.addErrorListener(SqlParseError.INSTANCE);

        // STAGE 2: parser with full LL(*)
        tree = parser2.singleStatement();
        // if we get here, it's LL not SLL
      }
      return astVisitor.visit(tree);
    } finally {
      PERFORMANCE_OVERVIEW_METRICS.recordParseCost(System.nanoTime() - startTime);
    }
  }

  private static void addMeasurementAndValue(
      InsertRowStatement insertRowStatement, List<String> measurements, List<String> values) {
    List<String> newMeasurements = new ArrayList<>(measurements.size());
    List<Object> newValues = new ArrayList<>(values.size());

    for (int i = 0; i < measurements.size(); ++i) {
      String value = values.get(i);
      if (value.isEmpty()) {
        continue;
      }
      newMeasurements.add(measurements.get(i));
      newValues.add(value);
    }

    insertRowStatement.setValues(newValues.toArray(new Object[0]));
    insertRowStatement.setMeasurements(newMeasurements.toArray(new String[0]));
  }

  private static PartialPath parseDatabaseRawString(final String database)
      throws IllegalPathException {
    final PartialPath databasePath = new PartialPath(database);
    if (databasePath.getNodeLength() < 2) {
      throw new IllegalPathException(database);
    }
    MetaFormatUtils.checkDatabase(database);
    return databasePath;
  }

  public static Statement createStatement(TFetchTimeseriesReq fetchTimeseriesReq, ZoneId zoneId) {
    return invokeParser(fetchTimeseriesReq.getQueryBody(), zoneId);
  }
}
