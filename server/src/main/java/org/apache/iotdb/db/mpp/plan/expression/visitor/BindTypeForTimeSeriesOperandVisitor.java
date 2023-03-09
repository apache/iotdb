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

package org.apache.iotdb.db.mpp.plan.expression.visitor;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.db.mpp.common.header.ColumnHeader;
import org.apache.iotdb.db.mpp.plan.expression.Expression;
import org.apache.iotdb.db.mpp.plan.expression.leaf.LeafOperand;
import org.apache.iotdb.db.mpp.plan.expression.leaf.TimeSeriesOperand;
import org.apache.iotdb.db.mpp.plan.expression.multi.FunctionExpression;

import java.util.ArrayList;
import java.util.List;

import static org.apache.iotdb.db.mpp.plan.analyze.ExpressionUtils.reconstructFunctionExpression;
import static org.apache.iotdb.db.mpp.plan.analyze.ExpressionUtils.reconstructTimeSeriesOperand;

public class BindTypeForTimeSeriesOperandVisitor extends ReconstructVisitor<List<ColumnHeader>> {
  @Override
  public Expression visitFunctionExpression(
      FunctionExpression predicate, List<ColumnHeader> columnHeaders) {
    List<Expression> expressions = predicate.getExpressions();
    List<Expression> childrenExpressions = new ArrayList<>();
    for (Expression expression : expressions) {
      childrenExpressions.add(process(expression, columnHeaders));
    }
    return reconstructFunctionExpression(predicate, childrenExpressions);
  }

  @Override
  public Expression visitTimeSeriesOperand(
      TimeSeriesOperand predicate, List<ColumnHeader> columnHeaders) {
    String oldPathString = predicate.getPath().getFullPath();
    // There are not too many TimeSeriesOperand and columnHeaders in our case,
    // so we use `for loop` instead of map to get the matched columnHeader for oldPath here.
    for (ColumnHeader columnHeader : columnHeaders) {
      if (oldPathString.equalsIgnoreCase(columnHeader.getColumnName())) {
        try {
          return reconstructTimeSeriesOperand(
              new MeasurementPath(columnHeader.getColumnName(), columnHeader.getColumnType()));
        } catch (IllegalPathException ignored) {
        }
      }
    }
    throw new SemanticException(String.format("please ensure input[%s] is correct", oldPathString));
  }

  @Override
  public Expression visitLeafOperand(LeafOperand leafOperand, List<ColumnHeader> context) {
    return leafOperand;
  }
}
