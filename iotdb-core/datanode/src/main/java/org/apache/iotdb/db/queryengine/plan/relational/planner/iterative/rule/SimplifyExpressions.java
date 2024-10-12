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

import org.apache.iotdb.db.queryengine.common.SessionInfo;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.NodeRef;
import org.apache.iotdb.db.queryengine.plan.relational.planner.IrExpressionInterpreter;
import org.apache.iotdb.db.queryengine.plan.relational.planner.IrTypeAnalyzer;
import org.apache.iotdb.db.queryengine.plan.relational.planner.LiteralEncoder;
import org.apache.iotdb.db.queryengine.plan.relational.planner.NoOpSymbolResolver;
import org.apache.iotdb.db.queryengine.plan.relational.planner.PlannerContext;
import org.apache.iotdb.db.queryengine.plan.relational.planner.SymbolAllocator;
import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.Rule;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.SymbolReference;

import com.google.common.collect.ImmutableSet;
import org.apache.tsfile.read.common.type.Type;

import java.util.Map;
import java.util.Set;

import static java.util.Objects.requireNonNull;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.ir.ExtractCommonPredicatesExpressionRewriter.extractCommonPredicates;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.ir.NormalizeOrExpressionRewriter.normalizeOrExpression;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.rule.PushDownNegationsExpressionRewriter.pushDownNegations;

public class SimplifyExpressions extends ExpressionRewriteRuleSet {
  public static Expression rewrite(
      Expression expression,
      SessionInfo session,
      SymbolAllocator symbolAllocator,
      PlannerContext plannerContext,
      IrTypeAnalyzer typeAnalyzer) {
    requireNonNull(plannerContext, "plannerContext is null");
    requireNonNull(typeAnalyzer, "typeAnalyzer is null");
    if (expression instanceof SymbolReference) {
      return expression;
    }
    Map<NodeRef<Expression>, Type> expressionTypes =
        typeAnalyzer.getTypes(session, symbolAllocator.getTypes(), expression);
    expression = pushDownNegations(plannerContext.getMetadata(), expression, expressionTypes);
    expression = extractCommonPredicates(expression);
    expression = normalizeOrExpression(expression);
    expressionTypes = typeAnalyzer.getTypes(session, symbolAllocator.getTypes(), expression);
    IrExpressionInterpreter interpreter =
        new IrExpressionInterpreter(expression, plannerContext, session, expressionTypes);
    Object optimized = interpreter.optimize(NoOpSymbolResolver.INSTANCE);
    return new LiteralEncoder(plannerContext)
        .toExpression(optimized, expressionTypes.get(NodeRef.of(expression)));
  }

  public SimplifyExpressions(PlannerContext plannerContext, IrTypeAnalyzer typeAnalyzer) {
    super(createRewrite(plannerContext, typeAnalyzer));
  }

  @Override
  public Set<Rule<?>> rules() {
    return ImmutableSet.of(
        projectExpressionRewrite(), filterExpressionRewrite()
        // TODO add it back after we support JoinNode
        //        joinExpressionRewrite(),
        // TODO add it back after we support ValuesNode
        //        valuesExpressionRewrite(),
        // TODO add it back after we support PatternRecognitionNode
        //        patternRecognitionExpressionRewrite()
        ); // ApplyNode and AggregationNode are not supported, because ExpressionInterpreter doesn't
    // support them
  }

  private static ExpressionRewriter createRewrite(
      PlannerContext plannerContext, IrTypeAnalyzer typeAnalyzer) {
    requireNonNull(plannerContext, "plannerContext is null");
    requireNonNull(typeAnalyzer, "typeAnalyzer is null");

    return (expression, context) ->
        rewrite(
            expression,
            context.getSessionInfo(),
            context.getSymbolAllocator(),
            plannerContext,
            typeAnalyzer);
  }
}
