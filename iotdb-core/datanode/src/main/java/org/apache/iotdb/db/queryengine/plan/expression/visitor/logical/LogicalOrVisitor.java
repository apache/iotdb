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

package org.apache.iotdb.db.queryengine.plan.expression.visitor.logical;

import org.apache.iotdb.db.queryengine.plan.expression.Expression;
import org.apache.iotdb.db.queryengine.plan.expression.binary.BinaryExpression;
import org.apache.iotdb.db.queryengine.plan.expression.multi.FunctionExpression;
import org.apache.iotdb.db.queryengine.plan.expression.other.CaseWhenThenExpression;
import org.apache.iotdb.db.queryengine.plan.expression.ternary.TernaryExpression;
import org.apache.iotdb.db.queryengine.plan.expression.unary.UnaryExpression;
import org.apache.iotdb.db.queryengine.plan.expression.visitor.ExpressionAnalyzeVisitor;

public abstract class LogicalOrVisitor<C> extends ExpressionAnalyzeVisitor<Boolean, C> {

  @Override
  public Boolean visitTernaryExpression(TernaryExpression ternaryExpression, C context) {
    return process(ternaryExpression.getFirstExpression(), context)
        || process(ternaryExpression.getSecondExpression(), context)
        || process(ternaryExpression.getThirdExpression(), context);
  }

  @Override
  public Boolean visitBinaryExpression(BinaryExpression binaryExpression, C context) {
    return process(binaryExpression.getLeftExpression(), context)
        || process(binaryExpression.getRightExpression(), context);
  }

  @Override
  public Boolean visitUnaryExpression(UnaryExpression unaryExpression, C context) {
    return process(unaryExpression.getExpression(), context);
  }

  @Override
  public Boolean visitCaseWhenThenExpression(
      CaseWhenThenExpression caseWhenThenExpression, C context) {
    for (Expression childExpression : caseWhenThenExpression.getExpressions()) {
      if (Boolean.TRUE.equals(process(childExpression, context))) {
        return Boolean.TRUE;
      }
    }
    return Boolean.FALSE;
  }

  @Override
  public Boolean visitFunctionExpression(FunctionExpression functionExpression, C context) {
    for (Expression childExpression : functionExpression.getExpressions()) {
      if (Boolean.TRUE.equals(process(childExpression, context))) {
        return Boolean.TRUE;
      }
    }
    return Boolean.FALSE;
  }
}
