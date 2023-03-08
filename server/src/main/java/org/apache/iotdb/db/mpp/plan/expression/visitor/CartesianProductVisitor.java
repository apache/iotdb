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
import org.apache.iotdb.db.mpp.plan.expression.ternary.TernaryExpression;
import org.apache.iotdb.db.mpp.plan.expression.unary.UnaryExpression;

import java.util.ArrayList;
import java.util.List;

import static org.apache.iotdb.db.mpp.plan.analyze.ExpressionUtils.cartesianProductAllKindsOfExpression;

public abstract class CartesianProductVisitor<C>
    extends ExpressionAnalyzeVisitor<List<Expression>, C> {
  private List<Expression> cartesianProductFromChild(Expression expression, C context) {
    List<List<Expression>> childResultsList = new ArrayList<>();
    expression.getExpressions().forEach(child -> childResultsList.add(process(child, context)));
    return cartesianProductAllKindsOfExpression(expression, childResultsList);
  }

  @Override
  public List<Expression> visitTernaryExpression(TernaryExpression ternaryExpression, C context) {
    return cartesianProductFromChild(ternaryExpression, context);
  }

  @Override
  public List<Expression> visitBinaryExpression(BinaryExpression binaryExpression, C context) {
    return cartesianProductFromChild(binaryExpression, context);
  }

  @Override
  public List<Expression> visitUnaryExpression(UnaryExpression unaryExpression, C context) {
    return cartesianProductFromChild(unaryExpression, context);
  }
}
