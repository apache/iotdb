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
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.mpp.common.filter.BasicFunctionFilter;
import org.apache.iotdb.db.mpp.common.filter.QueryFilter;
import org.apache.iotdb.db.mpp.sql.constant.FilterConstant;
import org.apache.iotdb.db.mpp.sql.statement.Statement;
import org.apache.iotdb.db.mpp.sql.statement.component.FromComponent;
import org.apache.iotdb.db.mpp.sql.statement.component.ResultColumn;
import org.apache.iotdb.db.mpp.sql.statement.component.SelectComponent;
import org.apache.iotdb.db.mpp.sql.statement.component.WhereCondition;
import org.apache.iotdb.db.mpp.sql.statement.crud.InsertStatement;
import org.apache.iotdb.db.mpp.sql.statement.crud.LastQueryStatement;
import org.apache.iotdb.db.mpp.sql.statement.crud.QueryStatement;
import org.apache.iotdb.db.mpp.sql.statement.metadata.CreateTimeSeriesStatement;
import org.apache.iotdb.db.qp.sql.IoTDBSqlParser;
import org.apache.iotdb.db.qp.sql.SqlLexer;
import org.apache.iotdb.db.qp.strategy.SQLParseError;
import org.apache.iotdb.db.query.expression.unary.TimeSeriesOperand;
import org.apache.iotdb.service.rpc.thrift.TSCreateTimeseriesReq;
import org.apache.iotdb.service.rpc.thrift.TSInsertRecordReq;
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
import java.util.HashSet;
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
      throws IllegalPathException {
    // construct insert statement
    InsertStatement insertStatement = new InsertStatement();
    insertStatement.setDevice(new PartialPath(insertRecordReq.getPrefixPath()));
    insertStatement.setTimes(new long[] {insertRecordReq.getTimestamp()});

    // TODO: set values after unifying SQL and RPC requests
    // insertStatement.setValuesList(insertRecordReq.getValues());
    insertStatement.setMeasurementList(insertRecordReq.getMeasurements().toArray(new String[0]));
    insertStatement.setAligned(insertStatement.isAligned());
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
}
