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

import org.apache.iotdb.db.mpp.plan.analyze.ExpressionAnalyzer;
import org.apache.iotdb.db.mpp.plan.expression.Expression;
import org.apache.iotdb.db.mpp.plan.expression.binary.BinaryExpression;
import org.apache.iotdb.db.mpp.plan.expression.binary.WhenThenExpression;
import org.apache.iotdb.db.mpp.plan.expression.other.CaseWhenThenExpression;
import org.apache.iotdb.db.mpp.plan.expression.ternary.TernaryExpression;
import org.apache.iotdb.db.mpp.plan.expression.unary.UnaryExpression;

import java.util.ArrayList;
import java.util.List;

import static org.apache.iotdb.db.mpp.plan.analyze.ExpressionUtils.reconstructBinaryExpressions;
import static org.apache.iotdb.db.mpp.plan.analyze.ExpressionUtils.reconstructCaseWhenThenExpressions;
import static org.apache.iotdb.db.mpp.plan.analyze.ExpressionUtils.reconstructTernaryExpressions;
import static org.apache.iotdb.db.mpp.plan.analyze.ExpressionUtils.reconstructUnaryExpressions;

public abstract class CartesianProductVisitor<C>
    extends ExpressionAnalyzeVisitor<List<Expression>, C> {
  @Override
  public List<Expression> visitTernaryExpression(TernaryExpression ternaryExpression, C context) {
    List<List<Expression>> childResultsList = getResultsFromChild(ternaryExpression, context);
    return reconstructTernaryExpressions(
        ternaryExpression,
        childResultsList.get(0),
        childResultsList.get(1),
        childResultsList.get(2));
  }

  @Override
  public List<Expression> visitBinaryExpression(BinaryExpression binaryExpression, C context) {
    if (binaryExpression instanceof WhenThenExpression) {
      if (ExpressionAnalyzer.checkHasWildCard(((WhenThenExpression) binaryExpression).getThen())) {
        throw new IllegalArgumentException("Cannot use wildcard in then-expression");
      }
    }
    List<List<Expression>> childResultsList = getResultsFromChild(binaryExpression, context);
    return reconstructBinaryExpressions(
        binaryExpression.getExpressionType(), childResultsList.get(0), childResultsList.get(1));
  }

  @Override
  public List<Expression> visitUnaryExpression(UnaryExpression unaryExpression, C context) {
    List<List<Expression>> childResultsList = getResultsFromChild(unaryExpression, context);
    return reconstructUnaryExpressions(unaryExpression, childResultsList.get(0));
  }

  @Override
  public List<Expression> visitCaseWhenThenExpression(CaseWhenThenExpression caseWhenThenExpression, C context) {
    List<List<Expression>> childResultsList = getResultsFromChild(caseWhenThenExpression, context);
    int len = childResultsList.size();
    List<List<WhenThenExpression>> whenThenLists = new ArrayList<>();
    for (List<Expression> list : childResultsList.subList(0, len-1)) {
      List<WhenThenExpression> newList = new ArrayList<>();
      for (Expression expression : list) {
        newList.add((WhenThenExpression) expression);
      }
      whenThenLists.add(newList);
    }
    List<Expression> elseLists = childResultsList.get(len-1);
    return reconstructCaseWhenThenExpressions(whenThenLists, elseLists);
  }
}
