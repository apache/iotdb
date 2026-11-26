/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.queryengine.plan.relational.planner.ir;

import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.ArithmeticBinaryExpression;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.BetweenPredicate;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Cast;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.CoalesceExpression;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.ComparisonExpression;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.FunctionCall;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.InPredicate;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.IsNullPredicate;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.LogicalExpression;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.NotExpression;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.NullIfExpression;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Row;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.SearchedCaseExpression;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.SimpleCaseExpression;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.WhenClause;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.AstVisitor;

public abstract class DefaultTraversalVisitor<C> implements AstVisitor<Void, C> {
  @Override
  public Void visitCast(Cast node, C context) {
    process(node.getExpression(), context);
    return null;
  }

  @Override
  public Void visitArithmeticBinary(ArithmeticBinaryExpression node, C context) {
    process(node.getLeft(), context);
    process(node.getRight(), context);

    return null;
  }

  @Override
  public Void visitBetweenPredicate(BetweenPredicate node, C context) {
    process(node.getValue(), context);
    process(node.getMin(), context);
    process(node.getMax(), context);

    return null;
  }

  @Override
  public Void visitCoalesceExpression(CoalesceExpression node, C context) {
    for (Expression operand : node.getOperands()) {
      process(operand, context);
    }

    return null;
  }

  //    @Override
  //    public Void visitSubscriptExpression(SubscriptExpression node, C context)
  //    {
  //        process(node.getBase(), context);
  //        process(node.getIndex(), context);
  //
  //        return null;
  //    }

  @Override
  public Void visitComparisonExpression(ComparisonExpression node, C context) {
    process(node.getLeft(), context);
    process(node.getRight(), context);

    return null;
  }

  @Override
  public Void visitInPredicate(InPredicate node, C context) {
    process(node.getValue(), context);
    process(node.getValueList(), context);

    return null;
  }

  @Override
  public Void visitFunctionCall(FunctionCall node, C context) {
    for (Expression argument : node.getArguments()) {
      process(argument, context);
    }

    return null;
  }

  @Override
  public Void visitSimpleCaseExpression(SimpleCaseExpression node, C context) {
    process(node.getOperand(), context);
    for (WhenClause clause : node.getWhenClauses()) {
      process(clause.getOperand(), context);
      process(clause.getResult(), context);
    }

    node.getDefaultValue().ifPresent(value -> process(value, context));

    return null;
  }

  @Override
  public Void visitNullIfExpression(NullIfExpression node, C context) {
    process(node.getFirst(), context);
    process(node.getSecond(), context);

    return null;
  }

  //    @Override
  //    public Void visitBindExpression(BindExpression node, C context)
  //    {
  //        for (Expression value : node.getValues()) {
  //            process(value, context);
  //        }
  //        process(node.getFunction(), context);
  //
  //        return null;
  //    }

  //    @Override
  //    public Void visitArithmeticNegation(ArithmeticNegation node, C context)
  //    {
  //        process(node.getValue(), context);
  //        return null;
  //    }

  @Override
  public Void visitNotExpression(NotExpression node, C context) {
    process(node.getValue(), context);
    return null;
  }

  @Override
  public Void visitSearchedCaseExpression(SearchedCaseExpression node, C context) {
    for (WhenClause clause : node.getWhenClauses()) {
      process(clause.getOperand(), context);
      process(clause.getResult(), context);
    }
    node.getDefaultValue().ifPresent(value -> process(value, context));

    return null;
  }

  @Override
  public Void visitIsNullPredicate(IsNullPredicate node, C context) {
    process(node.getValue(), context);
    return null;
  }

  @Override
  public Void visitLogicalExpression(LogicalExpression node, C context) {
    for (Expression child : node.getTerms()) {
      process(child, context);
    }

    return null;
  }

  @Override
  public Void visitRow(Row node, C context) {
    for (Expression expression : node.getItems()) {
      process(expression, context);
    }
    return null;
  }

  //    @Override
  //    public Void visitLambdaExpression(LambdaExpression node, C context)
  //    {
  //        process(node.getBody(), context);
  //
  //        return null;
  //    }
}
