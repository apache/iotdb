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

package org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.rule;

import org.apache.iotdb.db.queryengine.plan.relational.metadata.Metadata;
import org.apache.iotdb.db.queryengine.plan.relational.planner.ir.ExpressionTreeRewriter;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.LogicalExpression;

import static org.apache.iotdb.db.queryengine.plan.relational.planner.ir.IrUtils.combinePredicates;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.ir.IrUtils.extractPredicates;

/**
 * Flattens and removes duplicate conjuncts or disjuncts. E.g.,
 *
 * <p>a = 1 AND a = 1
 *
 * <p>rewrites to:
 *
 * <p>a = 1
 */
public class RemoveDuplicateConditions extends ExpressionRewriteRuleSet {
  public RemoveDuplicateConditions(Metadata metadata) {
    super(createRewrite(metadata));
  }

  private static ExpressionRewriter createRewrite(Metadata metadata) {
    return (expression, context) ->
        ExpressionTreeRewriter.rewriteWith(new Visitor(metadata), expression);
  }

  private static class Visitor
      extends org.apache.iotdb.db.queryengine.plan.relational.planner.ir.ExpressionRewriter<Void> {
    private final Metadata metadata;

    public Visitor(Metadata metadata) {
      this.metadata = metadata;
    }

    @Override
    public Expression rewriteLogicalExpression(
        LogicalExpression node, Void context, ExpressionTreeRewriter<Void> treeRewriter) {
      return combinePredicates(node.getOperator(), extractPredicates(node));
    }
  }
}
