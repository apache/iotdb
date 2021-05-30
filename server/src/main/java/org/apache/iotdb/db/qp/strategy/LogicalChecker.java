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

import org.apache.iotdb.db.exception.query.LogicalOperatorException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.logical.Operator;
import org.apache.iotdb.db.qp.logical.crud.QueryOperator;
import org.apache.iotdb.db.qp.logical.crud.SelectOperator;
import org.apache.iotdb.db.query.expression.Expression;
import org.apache.iotdb.db.query.expression.ResultColumn;
import org.apache.iotdb.db.query.expression.unary.TimeSeriesOperand;

public class LogicalChecker {

  /**
   * TODO: make check() an abstract method and call check() in this method or outside the
   * LogicalChecker.
   */
  public static void check(Operator operator) throws LogicalOperatorException {
    if (operator instanceof QueryOperator) {
      checkQueryOperator((QueryOperator) operator);
    }
  }

  private static void checkQueryOperator(QueryOperator queryOperator)
      throws LogicalOperatorException {
    checkSelectOperator(queryOperator);
  }

  private static void checkSelectOperator(QueryOperator queryOperator)
      throws LogicalOperatorException {
    checkLast(queryOperator);
    checkAggregation(queryOperator);
    checkAlignByDevice(queryOperator);
  }

  private static void checkLast(QueryOperator queryOperator) throws LogicalOperatorException {
    SelectOperator selectOperator = queryOperator.getSelectOperator();
    if (!selectOperator.isLastQuery()) {
      return;
    }

    for (ResultColumn resultColumn : selectOperator.getResultColumns()) {
      Expression expression = resultColumn.getExpression();
      if (!(expression instanceof TimeSeriesOperand)) {
        throw new LogicalOperatorException("Last queries can only be applied on raw time series.");
      }
    }
  }

  private static void checkAggregation(QueryOperator queryOperator)
      throws LogicalOperatorException {
    SelectOperator selectOperator = queryOperator.getSelectOperator();
    if (!selectOperator.hasAggregationFunction()) {
      return;
    }

    for (ResultColumn resultColumn : selectOperator.getResultColumns()) {
      Expression expression = resultColumn.getExpression();
      if (expression instanceof TimeSeriesOperand) {
        throw new LogicalOperatorException(
            "Common queries and aggregated queries are not allowed to appear at the same time");
      }
    }
  }

  private static void checkAlignByDevice(QueryOperator queryOperator)
      throws LogicalOperatorException {
    if (!queryOperator.isAlignByDevice()) {
      return;
    }

    SelectOperator selectOperator = queryOperator.getSelectOperator();
    if (selectOperator.hasTimeSeriesGeneratingFunction()) {
      throw new LogicalOperatorException("ALIGN BY DEVICE clause is not supported in UDF queries.");
    }

    for (PartialPath path : selectOperator.getPaths()) {
      String device = path.getDevice();
      if (!device.isEmpty()) {
        throw new LogicalOperatorException(
            "The paths of the SELECT clause can only be single level. In other words, "
                + "the paths of the SELECT clause can only be measurements or STAR, without DOT."
                + " For more details please refer to the SQL document.");
      }
    }
  }

  private LogicalChecker() {}
}
