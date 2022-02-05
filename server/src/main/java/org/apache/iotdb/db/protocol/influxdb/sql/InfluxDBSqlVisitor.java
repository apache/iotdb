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
package org.apache.iotdb.db.protocol.influxdb.sql;

import org.apache.iotdb.db.protocol.influxdb.expression.ResultColumn;
import org.apache.iotdb.db.protocol.influxdb.expression.binary.*;
import org.apache.iotdb.db.protocol.influxdb.expression.unary.FunctionExpression;
import org.apache.iotdb.db.protocol.influxdb.expression.unary.NegationExpression;
import org.apache.iotdb.db.protocol.influxdb.expression.unary.NodeExpression;
import org.apache.iotdb.db.protocol.influxdb.operator.*;
import org.apache.iotdb.db.qp.constant.FilterConstant;
import org.apache.iotdb.db.qp.constant.SQLConstant;
import org.apache.iotdb.db.qp.utils.DatetimeUtils;
import org.apache.iotdb.db.query.expression.Expression;

public class InfluxDBSqlVisitor extends InfluxDBSqlParserBaseVisitor<Operator> {

  private QueryOperator queryOp;

  @Override
  public Operator visitSingleStatement(InfluxDBSqlParser.SingleStatementContext ctx) {
    return visit(ctx.statement());
  }

  @Override
  public Operator visitSelectStatement(InfluxDBSqlParser.SelectStatementContext ctx) {
    queryOp = new QueryOperator();
    parseSelectClause(ctx.selectClause());
    parseFromClause(ctx.fromClause());
    if (ctx.whereClause() != null) {
      WhereComponent whereComponent = parseWhereClause(ctx.whereClause());
      queryOp.setWhereComponent(whereComponent);
    }
    return queryOp;
  }

  public void parseSelectClause(InfluxDBSqlParser.SelectClauseContext ctx) {
    SelectComponent selectComponent = new SelectComponent();
    for (InfluxDBSqlParser.ResultColumnContext resultColumnContext : ctx.resultColumn()) {
      selectComponent.addResultColumn(parseResultColumn(resultColumnContext));
    }
    queryOp.setSelectComponent(selectComponent);
  }

  private void parseFromClause(InfluxDBSqlParser.FromClauseContext fromClause) {
    FromComponent fromComponent = new FromComponent();

    for (InfluxDBSqlParser.NodeNameContext nodeName : fromClause.nodeName()) {
      fromComponent.addNodeName(nodeName.getText());
    }
    queryOp.setFromComponent(fromComponent);
  }

  private WhereComponent parseWhereClause(InfluxDBSqlParser.WhereClauseContext ctx) {
    FilterOperator whereOp = new FilterOperator();
    whereOp.addChildOperator(parseOrExpression(ctx.orExpression()));
    return new WhereComponent(whereOp.getChildren().get(0));
  }

  private FilterOperator parseOrExpression(InfluxDBSqlParser.OrExpressionContext ctx) {
    if (ctx.andExpression().size() == 1) {
      return parseAndExpression(ctx.andExpression(0));
    }
    FilterOperator binaryOp = new FilterOperator(FilterConstant.FilterType.KW_OR);
    if (ctx.andExpression().size() > 2) {
      binaryOp.addChildOperator(parseAndExpression(ctx.andExpression(0)));
      binaryOp.addChildOperator(parseAndExpression(ctx.andExpression(1)));
      for (int i = 2; i < ctx.andExpression().size(); i++) {
        FilterOperator operator = new FilterOperator(FilterConstant.FilterType.KW_OR);
        operator.addChildOperator(binaryOp);
        operator.addChildOperator(parseAndExpression(ctx.andExpression(i)));
        binaryOp = operator;
      }
    } else {
      for (InfluxDBSqlParser.AndExpressionContext andExpressionContext : ctx.andExpression()) {
        binaryOp.addChildOperator(parseAndExpression(andExpressionContext));
      }
    }
    return binaryOp;
  }

  private FilterOperator parseAndExpression(InfluxDBSqlParser.AndExpressionContext ctx) {
    if (ctx.predicate().size() == 1) {
      return parsePredicate(ctx.predicate(0));
    }
    FilterOperator binaryOp = new FilterOperator(FilterConstant.FilterType.KW_AND);
    int size = ctx.predicate().size();
    if (size > 2) {
      binaryOp.addChildOperator(parsePredicate(ctx.predicate(0)));
      binaryOp.addChildOperator(parsePredicate(ctx.predicate(1)));
      for (int i = 2; i < size; i++) {
        FilterOperator op = new FilterOperator(FilterConstant.FilterType.KW_AND);
        op.addChildOperator(binaryOp);
        op.addChildOperator(parsePredicate(ctx.predicate(i)));
        binaryOp = op;
      }
    } else {
      for (InfluxDBSqlParser.PredicateContext predicateContext : ctx.predicate()) {
        binaryOp.addChildOperator(parsePredicate(predicateContext));
      }
    }
    return binaryOp;
  }

  private FilterOperator parsePredicate(InfluxDBSqlParser.PredicateContext ctx) {
    if (ctx.OPERATOR_NOT() != null) {
      FilterOperator notOp = new FilterOperator(FilterConstant.FilterType.KW_NOT);
      notOp.addChildOperator(parseOrExpression(ctx.orExpression()));
      return notOp;
    } else if (ctx.LR_BRACKET() != null && ctx.OPERATOR_NOT() == null) {
      return parseOrExpression(ctx.orExpression());
    } else {
      String keyName = null;
      if (ctx.TIME() != null || ctx.TIMESTAMP() != null) {
        keyName = SQLConstant.RESERVED_TIME;
      }
      if (ctx.nodeName() != null) {
        keyName = ctx.nodeName().getText();
      }
      if (keyName == null) {
        throw new IllegalArgumentException("keyName is null, please check the sql");
      }
      return parseBasicFunctionOperator(ctx, keyName);
    }
  }

  private ResultColumn parseResultColumn(
      InfluxDBSqlParser.ResultColumnContext resultColumnContext) {
    return new ResultColumn(
        parseExpression(resultColumnContext.expression()),
        resultColumnContext.AS() == null ? null : resultColumnContext.ID().getText());
  }

  private Expression parseExpression(InfluxDBSqlParser.ExpressionContext context) {
    // unary
    if (context.functionName != null) {
      return parseFunctionExpression(context);
    }
    if (context.nodeName() != null) {
      return new NodeExpression(context.nodeName().getText());
    }

    if (context.literal != null) {
      return new NodeExpression(context.literal.getText());
    }
    if (context.unary != null) {
      return context.MINUS() != null
          ? new NegationExpression(parseExpression(context.expression(0)))
          : parseExpression(context.expression(0));
    }

    // binary
    Expression leftExpression = parseExpression(context.leftExpression);
    Expression rightExpression = parseExpression(context.rightExpression);
    if (context.STAR() != null) {
      return new MultiplicationExpression(leftExpression, rightExpression);
    }
    if (context.DIV() != null) {
      return new DivisionExpression(leftExpression, rightExpression);
    }
    if (context.MOD() != null) {
      return new ModuloExpression(leftExpression, rightExpression);
    }
    if (context.PLUS() != null) {
      return new AdditionExpression(leftExpression, rightExpression);
    }
    if (context.MINUS() != null) {
      return new SubtractionExpression(leftExpression, rightExpression);
    }
    throw new UnsupportedOperationException();
  }

  private Expression parseFunctionExpression(InfluxDBSqlParser.ExpressionContext functionClause) {

    FunctionExpression functionExpression =
        new FunctionExpression(functionClause.functionName.getText());

    // expressions
    for (InfluxDBSqlParser.ExpressionContext expression : functionClause.expression()) {
      functionExpression.addExpression(parseExpression(expression));
    }

    // attributes
    for (InfluxDBSqlParser.FunctionAttributeContext functionAttribute :
        functionClause.functionAttribute()) {
      functionExpression.addAttribute(
          removeStringQuote(functionAttribute.functionAttributeKey.getText()),
          removeStringQuote(functionAttribute.functionAttributeValue.getText()));
    }

    return functionExpression;
  }

  private static String removeStringQuote(String src) {
    if (src.charAt(0) == '\'' && src.charAt(src.length() - 1) == '\'') {
      return src.substring(1, src.length() - 1);
    } else if (src.charAt(0) == '\"' && src.charAt(src.length() - 1) == '\"') {
      return src.substring(1, src.length() - 1);
    } else {
      throw new IllegalArgumentException("error format for string with quote:" + src);
    }
  }

  /**
   * parse time expression, which is addition and subtraction expression of duration time, now() or
   * DataTimeFormat time.
   *
   * <p>eg. now() + 1d - 2h
   */
  private static Long parseDateExpression(InfluxDBSqlParser.DateExpressionContext ctx) {
    long time;
    time = parseTimeFormat(ctx.getChild(0).getText());
    for (int i = 1; i < ctx.getChildCount(); i = i + 2) {
      if (ctx.getChild(i).getText().equals("+")) {
        time += DatetimeUtils.convertDurationStrToLong(time, ctx.getChild(i + 1).getText());
      } else {
        time -= DatetimeUtils.convertDurationStrToLong(time, ctx.getChild(i + 1).getText());
      }
    }
    return time;
  }

  /** function for parsing time format. */
  public static long parseTimeFormat(String timestampStr) {
    if (timestampStr == null || timestampStr.trim().equals("")) {
      throw new IllegalArgumentException("input timestamp cannot be empty");
    }
    if (timestampStr.equalsIgnoreCase(SQLConstant.NOW_FUNC)) {
      return DatetimeUtils.currentTime();
    }
    throw new IllegalArgumentException(
        String.format(
            "Input time format %s error. "
                + "Input like yyyy-MM-dd HH:mm:ss, yyyy-MM-ddTHH:mm:ss or "
                + "refer to user document for more info.",
            timestampStr));
  }

  private static FilterOperator parseBasicFunctionOperator(
      InfluxDBSqlParser.PredicateContext ctx, String keyName) {
    BasicFunctionOperator basic;
    if (ctx.constant().dateExpression() != null) {
      if (!keyName.equals(SQLConstant.RESERVED_TIME)) {
        throw new IllegalArgumentException("Date can only be used to time");
      }
      basic =
          new BasicFunctionOperator(
              FilterConstant.lexerToFilterType.get(ctx.comparisonOperator().type.getType()),
              keyName,
              Long.toString(parseDateExpression(ctx.constant().dateExpression())));
    } else {
      basic =
          new BasicFunctionOperator(
              FilterConstant.lexerToFilterType.get(ctx.comparisonOperator().type.getType()),
              keyName,
              ctx.constant().getText());
    }
    return basic;
  }
}
