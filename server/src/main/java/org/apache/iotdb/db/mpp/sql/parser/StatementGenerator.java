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

package org.apache.iotdb.db.mpp.sql.parser;

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.mpp.common.filter.BasicFunctionFilter;
import org.apache.iotdb.db.mpp.common.filter.QueryFilter;
import org.apache.iotdb.db.mpp.sql.constant.FilterConstant;
import org.apache.iotdb.db.mpp.sql.statement.Statement;
import org.apache.iotdb.db.mpp.sql.statement.component.FromComponent;
import org.apache.iotdb.db.mpp.sql.statement.component.ResultColumn;
import org.apache.iotdb.db.mpp.sql.statement.component.SelectComponent;
import org.apache.iotdb.db.mpp.sql.statement.component.WhereCondition;
import org.apache.iotdb.db.mpp.sql.statement.crud.*;
import org.apache.iotdb.db.mpp.sql.statement.metadata.CreateTimeSeriesStatement;
import org.apache.iotdb.db.qp.sql.IoTDBSqlParser;
import org.apache.iotdb.db.qp.sql.SqlLexer;
import org.apache.iotdb.db.qp.strategy.SQLParseError;
import org.apache.iotdb.db.query.expression.unary.TimeSeriesOperand;
import org.apache.iotdb.db.utils.QueryDataSetUtils;
import org.apache.iotdb.service.rpc.thrift.*;
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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.iotdb.commons.conf.IoTDBConstant.TIME;

/** Convert SQL and RPC requests to {@link Statement}. */
public class StatementGenerator {

  public static Statement createStatement(
      String sql, ZoneId zoneId, IoTDBConstant.ClientVersion clientVersion) {
    return invokeParser(sql, zoneId, clientVersion);
  }

  public static Statement createStatement(String sql, ZoneId zoneId) {
    return invokeParser(sql, zoneId, IoTDBConstant.ClientVersion.V_0_13);
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
    selectComponent.addResultColumn(new ResultColumn(new TimeSeriesOperand(new PartialPath(""))));

    // set query filter
    QueryFilter queryFilter = new QueryFilter(FilterConstant.FilterType.KW_AND);
    PartialPath timePath = new PartialPath(TIME);
    queryFilter.setSinglePath(timePath);
    Set<PartialPath> pathSet = new HashSet<>();
    pathSet.add(timePath);
    queryFilter.setIsSingle(true);
    queryFilter.setPathSet(pathSet);

    BasicFunctionFilter left =
        new BasicFunctionFilter(
            FilterConstant.FilterType.GREATERTHANOREQUALTO,
            timePath,
            Long.toString(rawDataQueryReq.getStartTime()));
    BasicFunctionFilter right =
        new BasicFunctionFilter(
            FilterConstant.FilterType.LESSTHAN,
            timePath,
            Long.toString(rawDataQueryReq.getEndTime()));
    queryFilter.addChildOperator(left);
    queryFilter.addChildOperator(right);
    whereCondition.setQueryFilter(queryFilter);

    queryStatement.setSelectComponent(selectComponent);
    queryStatement.setFromComponent(fromComponent);
    queryStatement.setWhereCondition(whereCondition);
    return queryStatement;
  }

  public static Statement createStatement(TSLastDataQueryReq lastDataQueryReq, ZoneId zoneId)
      throws IllegalPathException {
    // construct query statement
    LastQueryStatement lastQueryStatement = new LastQueryStatement();
    SelectComponent selectComponent = new SelectComponent(zoneId);
    FromComponent fromComponent = new FromComponent();
    WhereCondition whereCondition = new WhereCondition();

    // iterate the path list and add it to from operator
    for (String pathStr : lastDataQueryReq.getPaths()) {
      PartialPath path = new PartialPath(pathStr);
      fromComponent.addPrefixPath(path);
    }
    selectComponent.addResultColumn(new ResultColumn(new TimeSeriesOperand(new PartialPath(""))));

    // set query filter
    PartialPath timePath = new PartialPath(TIME);
    BasicFunctionFilter basicFunctionFilter =
        new BasicFunctionFilter(
            FilterConstant.FilterType.GREATERTHANOREQUALTO,
            timePath,
            Long.toString(lastDataQueryReq.getTime()));
    whereCondition.setQueryFilter(basicFunctionFilter);

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

    insertStatement.fillValues(insertRecordReq.values);
    insertStatement.setMeasurements(insertRecordReq.getMeasurements().toArray(new String[0]));
    insertStatement.setAligned(insertRecordReq.isAligned);
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
        QueryDataSetUtils.readValuesFromBuffer(
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
          QueryDataSetUtils.readValuesFromBuffer(
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

  public static Statement createStatement(TSCreateTimeseriesReq req) throws IllegalPathException {
    // construct create timseries statement
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

  private static Statement invokeParser(
      String sql, ZoneId zoneId, IoTDBConstant.ClientVersion clientVersion) {
    ASTVisitor astVisitor = new ASTVisitor();
    astVisitor.setZoneId(zoneId);
    astVisitor.setClientVersion(clientVersion);

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
