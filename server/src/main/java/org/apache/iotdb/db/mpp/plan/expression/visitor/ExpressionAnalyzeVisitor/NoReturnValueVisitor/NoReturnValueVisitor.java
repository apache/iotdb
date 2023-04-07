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

package org.apache.iotdb.db.mpp.plan.expression.visitor.ExpressionAnalyzeVisitor.NoReturnValueVisitor;

import org.apache.iotdb.db.mpp.plan.expression.binary.BinaryExpression;
import org.apache.iotdb.db.mpp.plan.expression.leaf.LeafOperand;
import org.apache.iotdb.db.mpp.plan.expression.multi.FunctionExpression;
import org.apache.iotdb.db.mpp.plan.expression.other.CaseWhenThenExpression;
import org.apache.iotdb.db.mpp.plan.expression.ternary.TernaryExpression;
import org.apache.iotdb.db.mpp.plan.expression.unary.UnaryExpression;
import org.apache.iotdb.db.mpp.plan.expression.visitor.ExpressionAnalyzeVisitor.ExpressionAnalyzeVisitor;

public abstract class NoReturnValueVisitor<C> extends ExpressionAnalyzeVisitor<Void, C> {
  @Override
  public Void visitTernaryExpression(TernaryExpression ternaryExpression, C context) {
    getResultsFromChild(ternaryExpression, context);
    return null;
  }

  @Override
  public Void visitBinaryExpression(BinaryExpression binaryExpression, C context) {
    getResultsFromChild(binaryExpression, context);
    return null;
  }

  @Override
  public Void visitUnaryExpression(UnaryExpression unaryExpression, C context) {
    getResultsFromChild(unaryExpression, context);
    return null;
  }

  @Override
  public Void visitFunctionExpression(FunctionExpression functionExpression, C context) {
    getResultsFromChild(functionExpression, context);
    return null;
  }

  @Override
  public Void visitCaseWhenThenExpression(
      CaseWhenThenExpression caseWhenThenExpression, C context) {
    getResultsFromChild(caseWhenThenExpression, context);
    return null;
  }

  @Override
  public Void visitLeafOperand(LeafOperand leafOperand, C context) {
    return null;
  }
}
