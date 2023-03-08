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

/**
 * Simply collects result from child-expression. For example, two child give me 3 and 4 results, I
 * will return 3+4 = 7 results to upper level.
 */
public abstract class CollectVisitor extends ExpressionAnalyzeVisitor<List<Expression>, Void> {
  List<Expression> mergeList(List<List<Expression>> listOfList) {
    List<Expression> result = new ArrayList<>();
    listOfList.forEach(result::addAll);
    return result;
  }

  @Override
  public List<Expression> visitTernaryExpression(
      TernaryExpression ternaryExpression, Void context) {
    return mergeList(getResultsFromChild(ternaryExpression, null));
  }

  @Override
  public List<Expression> visitBinaryExpression(BinaryExpression binaryExpression, Void context) {
    return mergeList(getResultsFromChild(binaryExpression, null));
  }

  @Override
  public List<Expression> visitUnaryExpression(UnaryExpression unaryExpression, Void context) {
    return mergeList(getResultsFromChild(unaryExpression, null));
  }
}
