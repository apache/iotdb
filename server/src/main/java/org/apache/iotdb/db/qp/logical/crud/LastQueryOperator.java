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
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.crud.LastQueryPlan;
import org.apache.iotdb.db.qp.strategy.PhysicalGenerator;
import org.apache.iotdb.db.query.expression.Expression;
import org.apache.iotdb.db.query.expression.ResultColumn;
import org.apache.iotdb.db.query.expression.unary.TimeSeriesOperand;

public class LastQueryOperator extends QueryOperator {

  public LastQueryOperator() {}

  public LastQueryOperator(QueryOperator queryOperator) {
    super(queryOperator);
  }

  @Override
  public void check() throws LogicalOperatorException {
    super.check();

    if (isAlignByDevice()) {
      throw new LogicalOperatorException("Last query doesn't support align by device.");
    }

    if (!isAlignByTime()) {
      throw new LogicalOperatorException("Disable align cannot be applied to LAST query.");
    }

    for (ResultColumn resultColumn : selectComponent.getResultColumns()) {
      Expression expression = resultColumn.getExpression();
      if (!(expression instanceof TimeSeriesOperand)) {
        throw new LogicalOperatorException("Last queries can only be applied on raw time series.");
      }
    }
  }

  @Override
  public PhysicalPlan generatePhysicalPlan(PhysicalGenerator generator)
      throws QueryProcessException {
    return super.generateRawDataQueryPlan(generator, new LastQueryPlan());
  }
}
