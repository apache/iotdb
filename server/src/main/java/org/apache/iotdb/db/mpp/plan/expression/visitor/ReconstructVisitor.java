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

import org.apache.iotdb.db.mpp.plan.expression.Expression;
import org.apache.iotdb.db.mpp.plan.expression.binary.BinaryExpression;
import org.apache.iotdb.db.mpp.plan.expression.leaf.LeafOperand;
import org.apache.iotdb.db.mpp.plan.expression.ternary.TernaryExpression;
import org.apache.iotdb.db.mpp.plan.expression.unary.UnaryExpression;

import java.util.List;

import static org.apache.iotdb.db.mpp.plan.analyze.ExpressionUtils.reconstructBinaryExpression;
import static org.apache.iotdb.db.mpp.plan.analyze.ExpressionUtils.reconstructTernaryExpression;
import static org.apache.iotdb.db.mpp.plan.analyze.ExpressionUtils.reconstructUnaryExpression;

/**
 * Collect result from child, then reconstruct. For example, two child each give me 1 result, I
 * should use them to reconstruct 1 new result to upper level.
 */
public abstract class ReconstructVisitor<C> extends ExpressionAnalyzeVisitor<Expression, C> {
  @Override
  public Expression visitTernaryExpression(TernaryExpression ternaryExpression, C context) {
    List<Expression> childResults = getResultsFromChild(ternaryExpression, context);
    return reconstructTernaryExpression(
        ternaryExpression, childResults.get(0), childResults.get(1), childResults.get(2));
  }

  @Override
  public Expression visitBinaryExpression(BinaryExpression binaryExpression, C context) {
    List<Expression> childResults = getResultsFromChild(binaryExpression, context);
    return reconstructBinaryExpression(
        binaryExpression.getExpressionType(), childResults.get(0), childResults.get(1));
  }

  @Override
  public Expression visitUnaryExpression(UnaryExpression unaryExpression, C context) {
    List<Expression> childResults = getResultsFromChild(unaryExpression, context);
    return reconstructUnaryExpression(unaryExpression, childResults.get(0));
  }

  @Override
  public Expression visitLeafOperand(LeafOperand leafOperand, C context) {
    return leafOperand;
  }
}
