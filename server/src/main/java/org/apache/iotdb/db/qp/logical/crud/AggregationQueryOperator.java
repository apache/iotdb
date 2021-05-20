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
package org.apache.iotdb.db.qp.logical.crud;

import org.apache.iotdb.db.exception.query.LogicalOperatorException;
import org.apache.iotdb.db.query.expression.Expression;
import org.apache.iotdb.db.query.expression.ResultColumn;
import org.apache.iotdb.db.query.expression.unary.TimeSeriesOperand;

public class AggregationQueryOperator extends QueryOperator {

  public static final String ERROR_MESSAGE1 =
      "Common queries and aggregated queries are not allowed to appear at the same time";

  public AggregationQueryOperator() {}

  public AggregationQueryOperator(QueryOperator queryOperator) {
    this.selectComponent = queryOperator.getSelectComponent();
    this.fromComponent = queryOperator.getFromComponent();
    this.filterOperator = queryOperator.getFilterOperator();
    this.specialClauseComponent = queryOperator.getSpecialClauseComponent();
  }

  @Override
  public void check() throws LogicalOperatorException {
    super.check();

    if (!isAlignByTime()) {
      throw new LogicalOperatorException("AGGREGATION doesn't support disable align clause.");
    }

    for (ResultColumn resultColumn : selectComponent.getResultColumns()) {
      Expression expression = resultColumn.getExpression();
      if (expression instanceof TimeSeriesOperand) {
        throw new LogicalOperatorException(ERROR_MESSAGE1);
      }
    }
  }
}
