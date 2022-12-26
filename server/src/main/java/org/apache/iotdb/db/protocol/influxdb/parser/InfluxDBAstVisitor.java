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
package org.apache.iotdb.db.protocol.influxdb.parser;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.constant.SqlConstant;
import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.db.mpp.plan.analyze.ExpressionAnalyzer;
import org.apache.iotdb.db.mpp.plan.expression.Expression;
import org.apache.iotdb.db.mpp.plan.expression.binary.AdditionExpression;
import org.apache.iotdb.db.mpp.plan.expression.binary.DivisionExpression;
import org.apache.iotdb.db.mpp.plan.expression.binary.EqualToExpression;
import org.apache.iotdb.db.mpp.plan.expression.binary.GreaterEqualExpression;
import org.apache.iotdb.db.mpp.plan.expression.binary.GreaterThanExpression;
import org.apache.iotdb.db.mpp.plan.expression.binary.LessEqualExpression;
import org.apache.iotdb.db.mpp.plan.expression.binary.LessThanExpression;
import org.apache.iotdb.db.mpp.plan.expression.binary.LogicAndExpression;
import org.apache.iotdb.db.mpp.plan.expression.binary.LogicOrExpression;
import org.apache.iotdb.db.mpp.plan.expression.binary.ModuloExpression;
import org.apache.iotdb.db.mpp.plan.expression.binary.MultiplicationExpression;
import org.apache.iotdb.db.mpp.plan.expression.binary.NonEqualExpression;
import org.apache.iotdb.db.mpp.plan.expression.binary.SubtractionExpression;
import org.apache.iotdb.db.mpp.plan.expression.leaf.ConstantOperand;
import org.apache.iotdb.db.mpp.plan.expression.leaf.TimeSeriesOperand;
import org.apache.iotdb.db.mpp.plan.expression.multi.FunctionExpression;
import org.apache.iotdb.db.mpp.plan.expression.unary.LogicNotExpression;
import org.apache.iotdb.db.mpp.plan.expression.unary.NegationExpression;
import org.apache.iotdb.db.mpp.plan.statement.Statement;
import org.apache.iotdb.db.mpp.plan.statement.component.FromComponent;
import org.apache.iotdb.db.mpp.plan.statement.component.ResultColumn;
import org.apache.iotdb.db.mpp.plan.statement.component.WhereCondition;
import org.apache.iotdb.db.protocol.influxdb.statement.InfluxQueryStatement;
import org.apache.iotdb.db.protocol.influxdb.statement.InfluxSelectComponent;
import org.apache.iotdb.db.qp.sql.InfluxDBSqlParser;
import org.apache.iotdb.db.qp.sql.InfluxDBSqlParserBaseVisitor;
import org.apache.iotdb.db.utils.DateTimeUtils;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

public class InfluxDBAstVisitor extends InfluxDBSqlParserBaseVisitor<Statement> {

  private static final IoTDBConfig CONFIG = IoTDBDescriptor.getInstance().getConfig();

  @Override
  public Statement visitSingleStatement(InfluxDBSqlParser.SingleStatementContext ctx) {
    return visit(ctx.statement());
  }

  @Override
  public Statement visitSelectStatement(InfluxDBSqlParser.SelectStatementContext ctx) {
    InfluxQueryStatement queryStatement = new InfluxQueryStatement();
    queryStatement.setSelectComponent(parseSelectClause(ctx.selectClause()));
    queryStatement.setFromComponent(parseFromClause(ctx.fromClause()));
    if (ctx.whereClause() != null) {
      queryStatement.setWhereCondition(parseWhereClause(ctx.whereClause()));
    }
    return queryStatement;
  }

  public InfluxSelectComponent parseSelectClause(InfluxDBSqlParser.SelectClauseContext ctx) {
    InfluxSelectComponent influxSelectComponent = new InfluxSelectComponent();
    for (InfluxDBSqlParser.ResultColumnContext resultColumnContext : ctx.resultColumn()) {
      influxSelectComponent.addResultColumn(parseResultColumn(resultColumnContext));
    }
    return influxSelectComponent;
  }

  private FromComponent parseFromClause(InfluxDBSqlParser.FromClauseContext fromClause) {
    FromComponent fromComponent = new FromComponent();
    for (InfluxDBSqlParser.NodeNameContext nodeName : fromClause.nodeName()) {
      fromComponent.addPrefixPath(new PartialPath(nodeName.getText(), false));
    }
    return fromComponent;
  }

  private WhereCondition parseWhereClause(InfluxDBSqlParser.WhereClauseContext ctx) {
    return new WhereCondition(parsePredicate(ctx.predicate()));
  }

  private ResultColumn parseResultColumn(InfluxDBSqlParser.ResultColumnContext ctx) {
    Expression selectExpression = parseExpression(ctx.expression());
    ResultColumn.ColumnType columnType =
        ExpressionAnalyzer.identifyOutputColumnType(selectExpression, true);
    if (ctx.AS() != null) {
      return new ResultColumn(selectExpression, ctx.identifier().getText(), columnType);
    } else {
      return new ResultColumn(selectExpression, columnType);
    }
  }

  private Expression parseExpression(InfluxDBSqlParser.ExpressionContext context) {
    // LR_BRACKET unaryInBracket=expression RR_BRACKET
    if (context.unaryInBracket != null) {
      return parseExpression(context.unaryInBracket);
    }

    // (PLUS | MINUS) unaryAfterSign=expression
    if (context.unaryAfterSign != null) {
      return context.MINUS() != null
          ? new NegationExpression(parseExpression(context.expression(0)))
          : parseExpression(context.expression(0));
    }

    // leftExpression=expression (STAR | DIV | MOD) rightExpression=expression
    // leftExpression=expression (PLUS | MINUS) rightExpression=expression
    if (context.leftExpression != null && context.rightExpression != null) {
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
    }

    // functionName=nodeName LR_BRACKET expression (COMMA expression)* functionAttribute* RR_BRACKET
    if (context.functionName != null) {
      return parseFunctionExpression(context);
    }

    // nodeName || constant
    if (context.nodeName() != null || context.constant() != null) {
      return new TimeSeriesOperand(new PartialPath(context.nodeName().getText(), false));
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

  private Expression parsePredicate(InfluxDBSqlParser.PredicateContext context) {
    if (context.predicateInBracket != null) {
      return parsePredicate(context.predicateInBracket);
    }

    if (context.OPERATOR_NOT() != null) {
      return new LogicNotExpression(parsePredicate(context.predicateAfterUnaryOperator));
    }

    if (context.leftPredicate != null && context.rightPredicate != null) {
      Expression leftExpression = parsePredicate(context.leftPredicate);
      Expression rightExpression = parsePredicate(context.rightPredicate);

      if (context.OPERATOR_GT() != null) {
        return new GreaterThanExpression(leftExpression, rightExpression);
      }
      if (context.OPERATOR_GTE() != null) {
        return new GreaterEqualExpression(leftExpression, rightExpression);
      }
      if (context.OPERATOR_LT() != null) {
        return new LessThanExpression(leftExpression, rightExpression);
      }
      if (context.OPERATOR_LTE() != null) {
        return new LessEqualExpression(leftExpression, rightExpression);
      }
      if (context.OPERATOR_SEQ() != null) {
        return new EqualToExpression(leftExpression, rightExpression);
      }
      if (context.OPERATOR_NEQ() != null) {
        return new NonEqualExpression(leftExpression, rightExpression);
      }
      if (context.OPERATOR_AND() != null) {
        return new LogicAndExpression(leftExpression, rightExpression);
      }
      if (context.OPERATOR_OR() != null) {
        return new LogicOrExpression(leftExpression, rightExpression);
      }
      throw new UnsupportedOperationException();
    }

    if (context.nodeName() != null) {
      return new TimeSeriesOperand(new PartialPath(context.nodeName().getText(), false));
    }

    if (context.time != null) {
      return new TimeSeriesOperand(SqlConstant.TIME_PATH);
    }

    if (context.constant() != null) {
      return parseConstantOperand(context.constant());
    }

    throw new UnsupportedOperationException();
  }

  private Expression parseConstantOperand(InfluxDBSqlParser.ConstantContext constantContext) {
    String text = constantContext.getText();
    if (constantContext.BOOLEAN_LITERAL() != null) {
      return new ConstantOperand(TSDataType.BOOLEAN, text);
    } else if (constantContext.STRING_LITERAL() != null) {
      return new ConstantOperand(TSDataType.TEXT, parseStringLiteral(text));
    } else if (constantContext.INTEGER_LITERAL() != null) {
      return new ConstantOperand(TSDataType.INT64, text);
    } else if (constantContext.realLiteral() != null) {
      return parseRealLiteral(text);
    } else if (constantContext.dateExpression() != null) {
      return new ConstantOperand(
          TSDataType.INT64, String.valueOf(parseDateExpression(constantContext.dateExpression())));
    } else {
      throw new SemanticException("Unsupported constant operand: " + text);
    }
  }

  private String parseStringLiteral(String src) {
    if (2 <= src.length()) {
      // do not unescape string
      String unWrappedString = src.substring(1, src.length() - 1);
      if (src.charAt(0) == '\"' && src.charAt(src.length() - 1) == '\"') {
        // replace "" with "
        String replaced = unWrappedString.replace("\"\"", "\"");
        return replaced.length() == 0 ? "" : replaced;
      }
      if ((src.charAt(0) == '\'' && src.charAt(src.length() - 1) == '\'')) {
        // replace '' with '
        String replaced = unWrappedString.replace("''", "'");
        return replaced.length() == 0 ? "" : replaced;
      }
    }
    return src;
  }

  private Expression parseRealLiteral(String value) {
    // 3.33 is float by default
    return new ConstantOperand(
        CONFIG.getFloatingStringInferType().equals(TSDataType.DOUBLE)
            ? TSDataType.DOUBLE
            : TSDataType.FLOAT,
        value);
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
        time += DateTimeUtils.convertDurationStrToLong(time, ctx.getChild(i + 1).getText());
      } else {
        time -= DateTimeUtils.convertDurationStrToLong(time, ctx.getChild(i + 1).getText());
      }
    }
    return time;
  }

  /** function for parsing time format. */
  public static long parseTimeFormat(String timestampStr) {
    if (timestampStr == null || timestampStr.trim().equals("")) {
      throw new IllegalArgumentException("input timestamp cannot be empty");
    }
    if (timestampStr.equalsIgnoreCase(SqlConstant.NOW_FUNC)) {
      return DateTimeUtils.currentTime();
    }
    throw new IllegalArgumentException(
        String.format(
            "Input time format %s error. "
                + "Input like yyyy-MM-dd HH:mm:ss, yyyy-MM-ddTHH:mm:ss or "
                + "refer to user document for more info.",
            timestampStr));
  }
}
