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

import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ArithmeticBinaryExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.AstVisitor;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.BetweenPredicate;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Cast;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.CoalesceExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ComparisonExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.FunctionCall;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.InPredicate;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.IsNotNullPredicate;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.IsNullPredicate;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.LogicalExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.NotExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.NullIfExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Row;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.SearchedCaseExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.SimpleCaseExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.SymbolReference;

public abstract class IrVisitor<R, C> extends AstVisitor<R, C> {
  public R process(Expression node) {
    return process(node, null);
  }

  public R process(Expression node, C context) {
    return node.accept(this, context);
  }

  protected R visitExpression(Expression node, C context) {
    return null;
  }

  protected R visitArithmeticBinary(ArithmeticBinaryExpression node, C context) {
    return visitExpression(node, context);
  }

  protected R visitBetweenPredicate(BetweenPredicate node, C context) {
    return visitExpression(node, context);
  }

  protected R visitCoalesceExpression(CoalesceExpression node, C context) {
    return visitExpression(node, context);
  }

  protected R visitComparisonExpression(ComparisonExpression node, C context) {
    return visitExpression(node, context);
  }

  protected R visitInPredicate(InPredicate node, C context) {
    return visitExpression(node, context);
  }

  protected R visitFunctionCall(FunctionCall node, C context) {
    return visitExpression(node, context);
  }

  protected R visitSimpleCaseExpression(SimpleCaseExpression node, C context) {
    return visitExpression(node, context);
  }

  protected R visitNullIfExpression(NullIfExpression node, C context) {
    return visitExpression(node, context);
  }

  protected R visitNotExpression(NotExpression node, C context) {
    return visitExpression(node, context);
  }

  protected R visitSearchedCaseExpression(SearchedCaseExpression node, C context) {
    return visitExpression(node, context);
  }

  protected R visitIsNotNullPredicate(IsNotNullPredicate node, C context) {
    return visitExpression(node, context);
  }

  protected R visitIsNullPredicate(IsNullPredicate node, C context) {
    return visitExpression(node, context);
  }

  protected R visitLogicalExpression(LogicalExpression node, C context) {
    return visitExpression(node, context);
  }

  protected R visitRow(Row node, C context) {
    return visitExpression(node, context);
  }

  protected R visitCast(Cast node, C context) {
    return visitExpression(node, context);
  }

  protected R visitSymbolReference(SymbolReference node, C context) {
    return visitExpression(node, context);
  }
}
