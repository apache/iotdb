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

package org.apache.iotdb.db.query.udf.core.layer;

import org.apache.iotdb.db.query.expression.Expression;
import org.apache.iotdb.db.query.expression.unary.FunctionExpression;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

public class LayerMemoryAssigner {

  private final float memoryBudgetInMB;

  private final Map<Expression, Integer> expressionReferenceCount;

  private float memoryBudgetForSingleReference;

  public LayerMemoryAssigner(float memoryBudgetInMB) {
    expressionReferenceCount = new HashMap<>();
    this.memoryBudgetInMB = memoryBudgetInMB;
  }

  public void increaseExpressionReference(Expression expression) {
    expressionReferenceCount.put(
        expression,
        expressionReferenceCount.containsKey(expression)
            ? 1 + expressionReferenceCount.get(expression)
            : 1);
  }

  public void build() {
    int memoryPartitions = 0;
    for (Entry<Expression, Integer> expressionReferenceEntry :
        expressionReferenceCount.entrySet()) {
      memoryPartitions +=
          expressionReferenceEntry.getValue()
              * (expressionReferenceEntry.getKey() instanceof FunctionExpression ? 2 : 1);
    }
    memoryBudgetForSingleReference =
        memoryPartitions == 0 ? memoryBudgetInMB : memoryBudgetInMB / memoryPartitions;
  }

  public int getReference(Expression expression) {
    return expressionReferenceCount.get(expression);
  }

  public float assign() {
    return memoryBudgetForSingleReference;
  }
}
