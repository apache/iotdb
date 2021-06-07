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
package org.apache.iotdb.db.qp.strategy;

import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.constant.SQLConstant;
import org.apache.iotdb.db.qp.logical.Operator;
import org.apache.iotdb.db.qp.logical.crud.BasicFunctionOperator;
import org.apache.iotdb.db.qp.logical.crud.FilterOperator;
import org.apache.iotdb.db.qp.logical.crud.FromOperator;
import org.apache.iotdb.db.qp.logical.crud.QueryOperator;
import org.apache.iotdb.db.qp.logical.crud.SelectOperator;
import org.apache.iotdb.db.qp.sql.IoTDBSqlVisitor;
import org.apache.iotdb.db.qp.sql.SqlBaseLexer;
import org.apache.iotdb.db.qp.sql.SqlBaseParser;
import org.apache.iotdb.db.query.expression.ResultColumn;
import org.apache.iotdb.db.query.expression.unary.TimeSeriesOperand;
import org.apache.iotdb.service.rpc.thrift.TSLastDataQueryReq;
import org.apache.iotdb.service.rpc.thrift.TSRawDataQueryReq;

import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.atn.PredictionMode;
import org.antlr.v4.runtime.misc.ParseCancellationException;
import org.antlr.v4.runtime.tree.ParseTree;

import java.time.ZoneId;
import java.util.HashSet;
import java.util.Set;

import static org.apache.iotdb.db.conf.IoTDBConstant.TIME;

/** LogicalGenerator. */
public class LogicalGenerator {

  public static Operator generate(String sql, ZoneId zoneId) throws ParseCancellationException {
    IoTDBSqlVisitor ioTDBSqlVisitor = new IoTDBSqlVisitor();
    ioTDBSqlVisitor.setZoneId(zoneId);
    CharStream charStream1 = CharStreams.fromString(sql);
    SqlBaseLexer lexer1 = new SqlBaseLexer(charStream1);
    lexer1.removeErrorListeners();
    lexer1.addErrorListener(SQLParseError.INSTANCE);
    CommonTokenStream tokens1 = new CommonTokenStream(lexer1);
    SqlBaseParser parser1 = new SqlBaseParser(tokens1);
    parser1.getInterpreter().setPredictionMode(PredictionMode.SLL);
    parser1.removeErrorListeners();
    parser1.addErrorListener(SQLParseError.INSTANCE);
    ParseTree tree;
    try {
      tree = parser1.singleStatement(); // STAGE 1
    } catch (Exception ex) {
      CharStream charStream2 = CharStreams.fromString(sql);
      SqlBaseLexer lexer2 = new SqlBaseLexer(charStream2);
      lexer2.removeErrorListeners();
      lexer2.addErrorListener(SQLParseError.INSTANCE);
      CommonTokenStream tokens2 = new CommonTokenStream(lexer2);
      SqlBaseParser parser2 = new SqlBaseParser(tokens2);
      parser2.getInterpreter().setPredictionMode(PredictionMode.LL);
      parser2.removeErrorListeners();
      parser2.addErrorListener(SQLParseError.INSTANCE);
      tree = parser2.singleStatement(); // STAGE 2
      // if we parse ok, it's LL not SLL
    }
    return ioTDBSqlVisitor.visit(tree);
  }

  public static Operator generate(TSRawDataQueryReq rawDataQueryReq, ZoneId zoneId)
      throws IllegalPathException {
    // construct query operator and set its global time filter
    QueryOperator queryOp = new QueryOperator(SQLConstant.TOK_QUERY);
    FromOperator fromOp = new FromOperator(SQLConstant.TOK_FROM);
    SelectOperator selectOp = new SelectOperator(SQLConstant.TOK_SELECT, zoneId);

    // iterate the path list and add it to from operator
    for (String p : rawDataQueryReq.getPaths()) {
      PartialPath path = new PartialPath(p);
      fromOp.addPrefixTablePath(path);
    }
    selectOp.addResultColumn(new ResultColumn(new TimeSeriesOperand(new PartialPath(""))));

    queryOp.setSelectOperator(selectOp);
    queryOp.setFromOperator(fromOp);

    // set time filter operator
    FilterOperator filterOp = new FilterOperator(SQLConstant.KW_AND);
    PartialPath timePath = new PartialPath(TIME);
    filterOp.setSinglePath(timePath);
    Set<PartialPath> pathSet = new HashSet<>();
    pathSet.add(timePath);
    filterOp.setIsSingle(true);
    filterOp.setPathSet(pathSet);

    BasicFunctionOperator left =
        new BasicFunctionOperator(
            SQLConstant.GREATERTHANOREQUALTO,
            timePath,
            Long.toString(rawDataQueryReq.getStartTime()));
    BasicFunctionOperator right =
        new BasicFunctionOperator(
            SQLConstant.LESSTHAN, timePath, Long.toString(rawDataQueryReq.getEndTime()));
    filterOp.addChildOperator(left);
    filterOp.addChildOperator(right);

    queryOp.setFilterOperator(filterOp);

    return queryOp;
  }

  public static Operator generate(TSLastDataQueryReq req, ZoneId zoneId)
      throws IllegalPathException {
    // construct query operator and set its global time filter
    SelectOperator selectOp = new SelectOperator(SQLConstant.TOK_SELECT, zoneId);
    FromOperator fromOp = new FromOperator(SQLConstant.TOK_FROM);
    QueryOperator queryOp = new QueryOperator(SQLConstant.TOK_QUERY);

    selectOp.addResultColumn(new ResultColumn(new TimeSeriesOperand(new PartialPath(""))));
    selectOp.markAsLastQuery();

    for (String p : req.getPaths()) {
      PartialPath path = new PartialPath(p);
      fromOp.addPrefixTablePath(path);
    }

    queryOp.setSelectOperator(selectOp);
    queryOp.setFromOperator(fromOp);

    PartialPath timePath = new PartialPath(TIME);

    BasicFunctionOperator basicFunctionOperator =
        new BasicFunctionOperator(
            SQLConstant.GREATERTHANOREQUALTO, timePath, Long.toString(req.getTime()));
    queryOp.setFilterOperator(basicFunctionOperator);

    return queryOp;
  }

  private LogicalGenerator() {}
}
