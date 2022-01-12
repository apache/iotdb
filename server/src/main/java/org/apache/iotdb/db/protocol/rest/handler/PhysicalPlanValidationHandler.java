/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iotdb.db.protocol.rest.handler;

import org.apache.iotdb.db.exception.query.LogicalOperatorException;
import org.apache.iotdb.db.qp.logical.crud.LastQueryOperator;
import org.apache.iotdb.db.qp.logical.crud.QueryOperator;
import org.apache.iotdb.db.qp.logical.crud.SpecialClauseComponent;
import org.apache.iotdb.db.query.expression.ResultColumn;

import java.util.List;

public class PhysicalPlanValidationHandler {

  public static void checkRestQuery(QueryOperator queryOperator) throws LogicalOperatorException {

    // disable align
    if (!queryOperator.isAlignByTime()) {
      throw new LogicalOperatorException("disable align clauses are not supported.");
    }
  }

  public static void checkGrafanaExp(QueryOperator queryOperator) throws LogicalOperatorException {

    if (queryOperator.isAlignByDevice()) {
      throw new LogicalOperatorException("align by device clauses are not supported.");
    }

    // disable align
    if (!queryOperator.isAlignByTime()) {
      throw new LogicalOperatorException("disable align clauses are not supported.");
    }

    if (queryOperator instanceof LastQueryOperator) {
      throw new LogicalOperatorException("last clauses are not supported.");
    }

    if (queryOperator.getSpecialClauseComponent() != null) {
      SpecialClauseComponent specialClauseComponent = queryOperator.getSpecialClauseComponent();
      if (!specialClauseComponent.isAscending()) {
        throw new LogicalOperatorException("order by time desc clauses are not supported.");
      }

      if (specialClauseComponent.getLevels() != null
          && specialClauseComponent.getLevels().length > 0) {
        List<ResultColumn> resultColumnList = queryOperator.getSelectComponent().getResultColumns();
        for (ResultColumn resultColumn : resultColumnList) {
          if (resultColumn.getExpression().getExpressionString().contains("count")) {
            throw new LogicalOperatorException(
                "count timeseries with group by level clauses are not supported.");
          }
        }
      }
    }
  }
}
