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

package org.apache.iotdb.db.queryengine.plan.expression.visitor;

import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.db.queryengine.plan.expression.Expression;
import org.apache.iotdb.db.queryengine.plan.expression.leaf.LeafOperand;
import org.apache.iotdb.db.queryengine.plan.expression.multi.FunctionExpression;

import java.util.Collections;
import java.util.List;

import static org.apache.iotdb.db.utils.constant.SqlConstant.COUNT_TIME;

public class CountTimeAggregationAmountVisitor extends CollectVisitor {

  public static final String COUNT_TIME_ONLY_SUPPORT_ONE_WILDCARD =
      "The parameter of count_time aggregation can only be '*'.";

  @Override
  public List<Expression> visitFunctionExpression(
      FunctionExpression functionExpression, Void context) {
    if (COUNT_TIME.equalsIgnoreCase(functionExpression.getFunctionName())) {
      if (!"*".equals(functionExpression.getExpressions().get(0).toString())) {
        throw new SemanticException(COUNT_TIME_ONLY_SUPPORT_ONE_WILDCARD);
      }
      return Collections.singletonList(functionExpression);
    }

    return Collections.emptyList();
  }

  @Override
  public List<Expression> visitLeafOperand(LeafOperand leafOperand, Void context) {
    return Collections.emptyList();
  }
}
