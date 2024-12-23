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
package org.apache.iotdb.db.queryengine.plan.relational.planner.ir;

import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Expression;

import java.util.Map;

public class ExpressionNodeInliner extends ExpressionRewriter<Void> {

  public static Expression replaceExpression(
      Expression expression, Map<? extends Expression, ? extends Expression> mappings) {
    return ExpressionTreeRewriter.rewriteWith(new ExpressionNodeInliner(mappings), expression);
  }

  private final Map<? extends Expression, ? extends Expression> mappings;

  public ExpressionNodeInliner(Map<? extends Expression, ? extends Expression> mappings) {
    this.mappings = mappings;
  }

  @Override
  protected Expression rewriteExpression(
      Expression node, Void context, ExpressionTreeRewriter<Void> treeRewriter) {
    return mappings.get(node);
  }
}
