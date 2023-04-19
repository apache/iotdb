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

package org.apache.iotdb.db.mpp.plan.expression.visitor.ExpressionAnalyzeVisitor.MergeVisitor;

import org.apache.iotdb.db.mpp.plan.expression.binary.BinaryExpression;
import org.apache.iotdb.db.mpp.plan.expression.multi.FunctionExpression;
import org.apache.iotdb.db.mpp.plan.expression.other.CaseWhenThenExpression;
import org.apache.iotdb.db.mpp.plan.expression.ternary.TernaryExpression;
import org.apache.iotdb.db.mpp.plan.expression.unary.UnaryExpression;
import org.apache.iotdb.db.mpp.plan.expression.visitor.ExpressionAnalyzeVisitor.ExpressionAnalyzeVisitor;

import java.util.List;

public abstract class MergeVisitor<R, C> extends ExpressionAnalyzeVisitor<R, C> {
  // The specific merge method should be defined in the subclass.
  abstract R merge(List<R> childResults);

  @Override
  public R visitTernaryExpression(TernaryExpression ternaryExpression, C context) {
    return merge(getResultsFromChild(ternaryExpression, context));
  }

  @Override
  public R visitBinaryExpression(BinaryExpression binaryExpression, C context) {
    return merge(getResultsFromChild(binaryExpression, context));
  }

  @Override
  public R visitUnaryExpression(UnaryExpression unaryExpression, C context) {
    return merge(getResultsFromChild(unaryExpression, context));
  }

  @Override
  public R visitFunctionExpression(FunctionExpression functionExpression, C context) {
    return merge(getResultsFromChild(functionExpression, context));
  }

  @Override
  public R visitCaseWhenThenExpression(CaseWhenThenExpression caseWhenThenExpression, C context) {
    return merge(getResultsFromChild(caseWhenThenExpression, context));
  }
}
