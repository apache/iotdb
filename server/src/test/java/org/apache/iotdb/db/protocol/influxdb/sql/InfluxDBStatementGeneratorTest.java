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

import org.apache.iotdb.db.mpp.plan.expression.Expression;
import org.apache.iotdb.db.mpp.plan.expression.ExpressionType;
import org.apache.iotdb.db.mpp.plan.expression.binary.BinaryExpression;
import org.apache.iotdb.db.mpp.plan.expression.leaf.TimeSeriesOperand;
import org.apache.iotdb.db.mpp.plan.statement.component.ResultColumn;
import org.apache.iotdb.db.mpp.plan.statement.component.WhereCondition;
import org.apache.iotdb.db.protocol.influxdb.parser.InfluxDBStatementGenerator;
import org.apache.iotdb.db.protocol.influxdb.statement.InfluxQueryStatement;

import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class InfluxDBStatementGeneratorTest {
  @Test
  public void testParserSql1() {
    InfluxQueryStatement statement =
        (InfluxQueryStatement) InfluxDBStatementGenerator.generate("SELECT * FROM h2o_feet");
    List<ResultColumn> resultColumnList = statement.getSelectComponent().getResultColumns();
    assertEquals(resultColumnList.size(), 1);
    TimeSeriesOperand timeSeriesOperand =
        (TimeSeriesOperand) resultColumnList.get(0).getExpression();
    assertEquals(timeSeriesOperand.getPath().getFullPath(), "*");
    assertEquals(statement.getFromComponent().getPrefixPaths().get(0).getFullPath(), "h2o_feet");
    assertNull(statement.getWhereCondition());
  }

  @Test
  public void testParserSql2() {
    InfluxQueryStatement statement =
        (InfluxQueryStatement)
            InfluxDBStatementGenerator.generate("SELECT a,b,c FROM h2o_feet where a>1 and b<1");
    List<ResultColumn> resultColumnList = statement.getSelectComponent().getResultColumns();
    assertEquals(resultColumnList.size(), 3);
    TimeSeriesOperand timeSeriesOperand =
        (TimeSeriesOperand) resultColumnList.get(0).getExpression();
    assertEquals(timeSeriesOperand.getPath().getFullPath(), "a");

    WhereCondition whereCondition = statement.getWhereCondition();
    Expression predicate = whereCondition.getPredicate();
    assertEquals(predicate.getExpressionType(), ExpressionType.LOGIC_AND);

    Expression leftPredicate = ((BinaryExpression) predicate).getLeftExpression();
    Expression rightPredicate = ((BinaryExpression) predicate).getRightExpression();

    assertEquals(leftPredicate.getExpressionType(), ExpressionType.GREATER_THAN);
    assertEquals(
        ((BinaryExpression) leftPredicate).getLeftExpression().getExpressionType(),
        ExpressionType.TIMESERIES);
    assertEquals(
        ((BinaryExpression) leftPredicate).getRightExpression().getExpressionType(),
        ExpressionType.CONSTANT);
    assertEquals(leftPredicate.toString(), "a > 1");

    assertEquals(rightPredicate.getExpressionType(), ExpressionType.LESS_THAN);
    assertEquals(
        ((BinaryExpression) rightPredicate).getLeftExpression().getExpressionType(),
        ExpressionType.TIMESERIES);
    assertEquals(
        ((BinaryExpression) rightPredicate).getRightExpression().getExpressionType(),
        ExpressionType.CONSTANT);
    assertEquals(rightPredicate.toString(), "b < 1");
  }
}
