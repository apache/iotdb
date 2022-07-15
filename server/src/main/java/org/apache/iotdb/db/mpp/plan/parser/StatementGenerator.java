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

package org.apache.iotdb.db.mpp.plan.parser;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.mpp.plan.expression.binary.GreaterEqualExpression;
import org.apache.iotdb.db.mpp.plan.expression.binary.LessThanExpression;
import org.apache.iotdb.db.mpp.plan.expression.binary.LogicAndExpression;
import org.apache.iotdb.db.mpp.plan.expression.leaf.ConstantOperand;
import org.apache.iotdb.db.mpp.plan.expression.leaf.TimeSeriesOperand;
import org.apache.iotdb.db.mpp.plan.expression.leaf.TimestampOperand;
import org.apache.iotdb.db.mpp.plan.statement.Statement;
import org.apache.iotdb.db.mpp.plan.statement.component.FromComponent;
import org.apache.iotdb.db.mpp.plan.statement.component.ResultColumn;
import org.apache.iotdb.db.mpp.plan.statement.component.SelectComponent;
import org.apache.iotdb.db.mpp.plan.statement.component.WhereCondition;
import org.apache.iotdb.db.mpp.plan.statement.crud.DeleteDataStatement;
import org.apache.iotdb.db.mpp.plan.statement.crud.InsertMultiTabletsStatement;
import org.apache.iotdb.db.mpp.plan.statement.crud.InsertRowStatement;
import org.apache.iotdb.db.mpp.plan.statement.crud.InsertRowsOfOneDeviceStatement;
import org.apache.iotdb.db.mpp.plan.statement.crud.InsertRowsStatement;
import org.apache.iotdb.db.mpp.plan.statement.crud.InsertTabletStatement;
import org.apache.iotdb.db.mpp.plan.statement.crud.QueryStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.CreateAlignedTimeSeriesStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.CreateMultiTimeSeriesStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.CreateTimeSeriesStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.DeleteStorageGroupStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.SetStorageGroupStatement;
import org.apache.iotdb.db.qp.sql.IoTDBSqlParser;
import org.apache.iotdb.db.qp.sql.SqlLexer;
import org.apache.iotdb.db.qp.strategy.SQLParseError;
import org.apache.iotdb.db.utils.QueryDataSetUtils;
import org.apache.iotdb.service.rpc.thrift.TSCreateAlignedTimeseriesReq;
import org.apache.iotdb.service.rpc.thrift.TSCreateMultiTimeseriesReq;
import org.apache.iotdb.service.rpc.thrift.TSCreateTimeseriesReq;
import org.apache.iotdb.service.rpc.thrift.TSDeleteDataReq;
import org.apache.iotdb.service.rpc.thrift.TSInsertRecordReq;
import org.apache.iotdb.service.rpc.thrift.TSInsertRecordsOfOneDeviceReq;
import org.apache.iotdb.service.rpc.thrift.TSInsertRecordsReq;
import org.apache.iotdb.service.rpc.thrift.TSInsertStringRecordReq;
import org.apache.iotdb.service.rpc.thrift.TSInsertStringRecordsOfOneDeviceReq;
import org.apache.iotdb.service.rpc.thrift.TSInsertStringRecordsReq;
import org.apache.iotdb.service.rpc.thrift.TSInsertTabletReq;
import org.apache.iotdb.service.rpc.thrift.TSInsertTabletsReq;
import org.apache.iotdb.service.rpc.thrift.TSLastDataQueryReq;
import org.apache.iotdb.service.rpc.thrift.TSRawDataQueryReq;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;

import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.atn.PredictionMode;
import org.antlr.v4.runtime.tree.ParseTree;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;

/** Convert SQL and RPC requests to {@link Statement}. */
public class StatementGenerator {

  public static Statement createStatement(String sql, ZoneId zoneId) {
    return invokeParser(sql, zoneId);
  }

  public static Statement createStatement(TSRawDataQueryReq rawDataQueryReq, ZoneId zoneId)
      throws IllegalPathException {
    // construct query statement
    QueryStatement queryStatement = new QueryStatement();
    SelectComponent selectComponent = new SelectComponent(zoneId);
    FromComponent fromComponent = new FromComponent();
    WhereCondition whereCondition = new WhereCondition();

    // iterate the path list and add it to from operator
    for (String pathStr : rawDataQueryReq.getPaths()) {
      PartialPath path = new PartialPath(pathStr);
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

    queryStatement.setSelectComponent(selectComponent);
    queryStatement.setFromComponent(fromComponent);
    queryStatement.setWhereCondition(whereCondition);
    return queryStatement;
  }

  public static Statement createStatement(TSLastDataQueryReq lastDataQueryReq, ZoneId zoneId)
      throws IllegalPathException {
    // construct query statement
    QueryStatement lastQueryStatement = new QueryStatement();
    SelectComponent selectComponent = new SelectComponent(zoneId);
    FromComponent fromComponent = new FromComponent();
    WhereCondition whereCondition = new WhereCondition();

    selectComponent.setHasLast(true);

    // iterate the path list and add it to from operator
    for (String pathStr : lastDataQueryReq.getPaths()) {
      PartialPath path = new PartialPath(pathStr);
      fromComponent.addPrefixPath(path);
    }
    selectComponent.addResultColumn(
        new ResultColumn(
            new TimeSeriesOperand(new PartialPath("", false)), ResultColumn.ColumnType.RAW));

    // set query filter
    GreaterEqualExpression predicate =
        new GreaterEqualExpression(
            new TimestampOperand(),
            new ConstantOperand(TSDataType.INT64, Long.toString(lastDataQueryReq.getTime())));
    whereCondition.setPredicate(predicate);

    lastQueryStatement.setSelectComponent(selectComponent);
    lastQueryStatement.setFromComponent(fromComponent);
    lastQueryStatement.setWhereCondition(whereCondition);
    return lastQueryStatement;
  }

  public static Statement createStatement(TSInsertRecordReq insertRecordReq)
      throws IllegalPathException, QueryProcessException {
    // construct insert statement
    InsertRowStatement insertStatement = new InsertRowStatement();
    insertStatement.setDevicePath(new PartialPath(insertRecordReq.getPrefixPath()));
    insertStatement.setTime(insertRecordReq.getTimestamp());
    insertStatement.setMeasurements(insertRecordReq.getMeasurements().toArray(new String[0]));
    insertStatement.setAligned(insertRecordReq.isAligned);
    insertStatement.fillValues(insertRecordReq.values);
    return insertStatement;
  }

  public static Statement createStatement(TSInsertStringRecordReq insertRecordReq)
      throws IllegalPathException, QueryProcessException {
    // construct insert statement
    InsertRowStatement insertStatement = new InsertRowStatement();
    insertStatement.setDevicePath(new PartialPath(insertRecordReq.getPrefixPath()));
    insertStatement.setTime(insertRecordReq.getTimestamp());
    insertStatement.setMeasurements(insertRecordReq.getMeasurements().toArray(new String[0]));
    insertStatement.setDataTypes(new TSDataType[insertStatement.getMeasurements().length]);
    insertStatement.setValues(insertRecordReq.getValues().toArray(new Object[0]));
    insertStatement.setNeedInferType(true);
    insertStatement.setAligned(insertRecordReq.isAligned);

    return insertStatement;
  }

  public static Statement createStatement(TSInsertTabletReq insertTabletReq)
      throws IllegalPathException {
    // construct insert statement
    InsertTabletStatement insertStatement = new InsertTabletStatement();
    insertStatement.setDevicePath(new PartialPath(insertTabletReq.getPrefixPath()));
    insertStatement.setMeasurements(insertTabletReq.getMeasurements().toArray(new String[0]));
    insertStatement.setTimes(
        QueryDataSetUtils.readTimesFromBuffer(insertTabletReq.timestamps, insertTabletReq.size));
    insertStatement.setColumns(
        QueryDataSetUtils.readTabletValuesFromBuffer(
            insertTabletReq.values,
            insertTabletReq.types,
            insertTabletReq.types.size(),
            insertTabletReq.size));
    insertStatement.setBitMaps(
        QueryDataSetUtils.readBitMapsFromBuffer(
            insertTabletReq.values, insertTabletReq.types.size(), insertTabletReq.size));
    insertStatement.setRowCount(insertTabletReq.size);
    TSDataType[] dataTypes = new TSDataType[insertTabletReq.types.size()];
    for (int i = 0; i < insertTabletReq.types.size(); i++) {
      dataTypes[i] = TSDataType.values()[insertTabletReq.types.get(i)];
    }
    insertStatement.setDataTypes(dataTypes);
    insertStatement.setAligned(insertTabletReq.isAligned);
    return insertStatement;
  }

  public static Statement createStatement(TSInsertTabletsReq req) throws IllegalPathException {
    // construct insert statement
    InsertMultiTabletsStatement insertStatement = new InsertMultiTabletsStatement();
    List<InsertTabletStatement> insertTabletStatementList = new ArrayList<>();
    for (int i = 0; i < req.prefixPaths.size(); i++) {
      InsertTabletStatement insertTabletStatement = new InsertTabletStatement();
      insertTabletStatement.setDevicePath(new PartialPath(req.prefixPaths.get(i)));
      insertTabletStatement.setMeasurements(req.measurementsList.get(i).toArray(new String[0]));
      insertTabletStatement.setTimes(
          QueryDataSetUtils.readTimesFromBuffer(req.timestampsList.get(i), req.sizeList.get(i)));
      insertTabletStatement.setColumns(
          QueryDataSetUtils.readTabletValuesFromBuffer(
              req.valuesList.get(i),
              req.typesList.get(i),
              req.measurementsList.get(i).size(),
              req.sizeList.get(i)));
      insertTabletStatement.setBitMaps(
          QueryDataSetUtils.readBitMapsFromBuffer(
              req.valuesList.get(i), req.measurementsList.get(i).size(), req.sizeList.get(i)));
      insertTabletStatement.setRowCount(req.sizeList.get(i));
      TSDataType[] dataTypes = new TSDataType[req.typesList.get(i).size()];
      for (int j = 0; j < dataTypes.length; j++) {
        dataTypes[j] = TSDataType.values()[req.typesList.get(i).get(j)];
      }
      insertTabletStatement.setDataTypes(dataTypes);
      insertTabletStatement.setAligned(req.isAligned);

      insertTabletStatementList.add(insertTabletStatement);
    }

    insertStatement.setInsertTabletStatementList(insertTabletStatementList);
    return insertStatement;
  }

  public static Statement createStatement(TSInsertRecordsReq req)
      throws IllegalPathException, QueryProcessException {
    // construct insert statement
    InsertRowsStatement insertStatement = new InsertRowsStatement();
    List<InsertRowStatement> insertRowStatementList = new ArrayList<>();
    for (int i = 0; i < req.prefixPaths.size(); i++) {
      InsertRowStatement statement = new InsertRowStatement();
      statement.setDevicePath(new PartialPath(req.getPrefixPaths().get(i)));
      statement.setMeasurements(req.getMeasurementsList().get(i).toArray(new String[0]));
      statement.setTime(req.getTimestamps().get(i));
      statement.fillValues(req.valuesList.get(i));
      statement.setAligned(req.isAligned);
      insertRowStatementList.add(statement);
    }
    insertStatement.setInsertRowStatementList(insertRowStatementList);
    return insertStatement;
  }

  public static Statement createStatement(TSInsertStringRecordsReq req)
      throws IllegalPathException, QueryProcessException {
    // construct insert statement
    InsertRowsStatement insertStatement = new InsertRowsStatement();
    List<InsertRowStatement> insertRowStatementList = new ArrayList<>();
    for (int i = 0; i < req.prefixPaths.size(); i++) {
      InsertRowStatement statement = new InsertRowStatement();
      statement.setDevicePath(new PartialPath(req.getPrefixPaths().get(i)));
      addMeasurementAndValue(
          statement, req.getMeasurementsList().get(i), req.getValuesList().get(i));
      statement.setDataTypes(new TSDataType[statement.getMeasurements().length]);
      statement.setTime(req.getTimestamps().get(i));
      statement.setNeedInferType(true);
      statement.setAligned(req.isAligned);

      insertRowStatementList.add(statement);
    }
    insertStatement.setInsertRowStatementList(insertRowStatementList);
    return insertStatement;
  }

  public static Statement createStatement(TSInsertRecordsOfOneDeviceReq req)
      throws IllegalPathException, QueryProcessException {
    // construct insert statement
    InsertRowsOfOneDeviceStatement insertStatement = new InsertRowsOfOneDeviceStatement();
    insertStatement.setDevicePath(new PartialPath(req.prefixPath));
    List<InsertRowStatement> insertRowStatementList = new ArrayList<>();
    for (int i = 0; i < req.timestamps.size(); i++) {
      InsertRowStatement statement = new InsertRowStatement();
      statement.setDevicePath(insertStatement.getDevicePath());
      statement.setMeasurements(req.measurementsList.get(i).toArray(new String[0]));
      statement.setTime(req.timestamps.get(i));
      statement.fillValues(req.valuesList.get(i));
      statement.setAligned(req.isAligned);

      insertRowStatementList.add(statement);
    }
    insertStatement.setInsertRowStatementList(insertRowStatementList);
    return insertStatement;
  }

  public static Statement createStatement(TSInsertStringRecordsOfOneDeviceReq req)
      throws IllegalPathException, QueryProcessException {
    // construct insert statement
    InsertRowsOfOneDeviceStatement insertStatement = new InsertRowsOfOneDeviceStatement();
    insertStatement.setDevicePath(new PartialPath(req.prefixPath));
    List<InsertRowStatement> insertRowStatementList = new ArrayList<>();
    for (int i = 0; i < req.timestamps.size(); i++) {
      InsertRowStatement statement = new InsertRowStatement();
      statement.setDevicePath(insertStatement.getDevicePath());
      addMeasurementAndValue(
          statement, req.getMeasurementsList().get(i), req.getValuesList().get(i));
      statement.setDataTypes(new TSDataType[statement.getMeasurements().length]);
      statement.setTime(req.timestamps.get(i));
      statement.setNeedInferType(true);
      statement.setAligned(req.isAligned);

      insertRowStatementList.add(statement);
    }
    insertStatement.setInsertRowStatementList(insertRowStatementList);
    return insertStatement;
  }

  public static Statement createStatement(String storageGroup) throws IllegalPathException {
    // construct set storage group statement
    SetStorageGroupStatement statement = new SetStorageGroupStatement();
    statement.setStorageGroupPath(new PartialPath(storageGroup));
    return statement;
  }

  public static Statement createStatement(TSCreateTimeseriesReq req) throws IllegalPathException {
    // construct create timeseries statement
    CreateTimeSeriesStatement statement = new CreateTimeSeriesStatement();
    statement.setPath(new PartialPath(req.path));
    statement.setDataType(TSDataType.values()[req.dataType]);
    statement.setEncoding(TSEncoding.values()[req.encoding]);
    statement.setCompressor(CompressionType.values()[req.compressor]);
    statement.setProps(req.props);
    statement.setTags(req.tags);
    statement.setAttributes(req.attributes);
    statement.setAlias(req.measurementAlias);
    return statement;
  }

  public static Statement createStatement(TSCreateAlignedTimeseriesReq req)
      throws IllegalPathException {
    // construct create aligned timeseries statement
    CreateAlignedTimeSeriesStatement statement = new CreateAlignedTimeSeriesStatement();
    statement.setDevicePath(new PartialPath(req.prefixPath));
    List<TSDataType> dataTypes = new ArrayList<>();
    for (int dataType : req.dataTypes) {
      dataTypes.add(TSDataType.values()[dataType]);
    }
    List<TSEncoding> encodings = new ArrayList<>();
    for (int encoding : req.encodings) {
      encodings.add(TSEncoding.values()[encoding]);
    }
    List<CompressionType> compressors = new ArrayList<>();
    for (int compressor : req.compressors) {
      compressors.add(CompressionType.values()[compressor]);
    }
    statement.setMeasurements(req.measurements);
    statement.setDataTypes(dataTypes);
    statement.setEncodings(encodings);
    statement.setCompressors(compressors);
    statement.setTagsList(req.tagsList);
    statement.setAttributesList(req.attributesList);
    statement.setAliasList(req.measurementAlias);
    return statement;
  }

  public static Statement createStatement(TSCreateMultiTimeseriesReq req)
      throws IllegalPathException {
    // construct create multi timeseries statement
    List<PartialPath> paths = new ArrayList<>();
    for (String path : req.paths) {
      paths.add(new PartialPath(path));
    }
    List<TSDataType> dataTypes = new ArrayList<>();
    for (int dataType : req.dataTypes) {
      dataTypes.add(TSDataType.values()[dataType]);
    }
    List<TSEncoding> encodings = new ArrayList<>();
    for (int encoding : req.encodings) {
      encodings.add(TSEncoding.values()[encoding]);
    }
    List<CompressionType> compressors = new ArrayList<>();
    for (int compressor : req.compressors) {
      compressors.add(CompressionType.values()[compressor]);
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
    return statement;
  }

  public static Statement createStatement(List<String> storageGroups) {
    DeleteStorageGroupStatement statement = new DeleteStorageGroupStatement();
    statement.setPrefixPath(storageGroups);
    return statement;
  }

  public static DeleteDataStatement createStatement(TSDeleteDataReq req)
      throws IllegalPathException {
    DeleteDataStatement statement = new DeleteDataStatement();
    List<PartialPath> pathList = new ArrayList<>();
    for (String path : req.getPaths()) {
      pathList.add(new PartialPath(path));
    }
    statement.setPathList(pathList);
    statement.setDeleteStartTime(req.getStartTime());
    statement.setDeleteEndTime(req.getEndTime());
    return statement;
  }

  private static Statement invokeParser(String sql, ZoneId zoneId) {
    ASTVisitor astVisitor = new ASTVisitor();
    astVisitor.setZoneId(zoneId);

    CharStream charStream1 = CharStreams.fromString(sql);

    SqlLexer lexer1 = new SqlLexer(charStream1);
    lexer1.removeErrorListeners();
    lexer1.addErrorListener(SQLParseError.INSTANCE);

    CommonTokenStream tokens1 = new CommonTokenStream(lexer1);

    IoTDBSqlParser parser1 = new IoTDBSqlParser(tokens1);
    parser1.getInterpreter().setPredictionMode(PredictionMode.SLL);
    parser1.removeErrorListeners();
    parser1.addErrorListener(SQLParseError.INSTANCE);

    ParseTree tree;
    try {
      // STAGE 1: try with simpler/faster SLL(*)
      tree = parser1.singleStatement();
      // if we get here, there was no syntax error and SLL(*) was enough;
      // there is no need to try full LL(*)
    } catch (Exception ex) {
      CharStream charStream2 = CharStreams.fromString(sql);

      SqlLexer lexer2 = new SqlLexer(charStream2);
      lexer2.removeErrorListeners();
      lexer2.addErrorListener(SQLParseError.INSTANCE);

      CommonTokenStream tokens2 = new CommonTokenStream(lexer2);

      org.apache.iotdb.db.qp.sql.IoTDBSqlParser parser2 =
          new org.apache.iotdb.db.qp.sql.IoTDBSqlParser(tokens2);
      parser2.getInterpreter().setPredictionMode(PredictionMode.LL);
      parser2.removeErrorListeners();
      parser2.addErrorListener(SQLParseError.INSTANCE);

      // STAGE 2: parser with full LL(*)
      tree = parser2.singleStatement();
      // if we get here, it's LL not SLL
    }
    return astVisitor.visit(tree);
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
}
