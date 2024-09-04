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

import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ArithmeticBinaryExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ArithmeticUnaryExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.BetweenPredicate;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Cast;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.CoalesceExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ComparisonExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.DereferenceExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.FieldReference;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.FunctionCall;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.GenericDataType;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Identifier;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.IfExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.InListExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.InPredicate;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.IsNotNullPredicate;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.IsNullPredicate;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.LikePredicate;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Literal;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.LogicalExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.NotExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.NullIfExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Parameter;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.QuantifiedComparisonExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Row;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.SearchedCaseExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.SimpleCaseExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.SymbolReference;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Trim;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.WhenClause;

public class ExpressionRewriter<C> {

  protected Expression rewriteExpression(
      Expression node, C context, ExpressionTreeRewriter<C> treeRewriter) {
    return null;
  }

  public Expression rewriteFieldReference(
      FieldReference node, C context, ExpressionTreeRewriter<C> treeRewriter) {
    return rewriteExpression(node, context, treeRewriter);
  }

  public Expression rewriteIdentifier(
      Identifier node, C context, ExpressionTreeRewriter<C> treeRewriter) {
    return rewriteExpression(node, context, treeRewriter);
  }

  public Expression rewriteRow(Row node, C context, ExpressionTreeRewriter<C> treeRewriter) {
    return rewriteExpression(node, context, treeRewriter);
  }

  public Expression rewriteArithmeticUnary(
      ArithmeticUnaryExpression node, C context, ExpressionTreeRewriter<C> treeRewriter) {
    return rewriteExpression(node, context, treeRewriter);
  }

  public Expression rewriteArithmeticBinary(
      ArithmeticBinaryExpression node, C context, ExpressionTreeRewriter<C> treeRewriter) {
    return rewriteExpression(node, context, treeRewriter);
  }

  public Expression rewriteComparisonExpression(
      ComparisonExpression node, C context, ExpressionTreeRewriter<C> treeRewriter) {
    return rewriteExpression(node, context, treeRewriter);
  }

  public Expression rewriteBetweenPredicate(
      BetweenPredicate node, C context, ExpressionTreeRewriter<C> treeRewriter) {
    return rewriteExpression(node, context, treeRewriter);
  }

  public Expression rewriteLogicalExpression(
      LogicalExpression node, C context, ExpressionTreeRewriter<C> treeRewriter) {
    return rewriteExpression(node, context, treeRewriter);
  }

  public Expression rewriteNotExpression(
      NotExpression node, C context, ExpressionTreeRewriter<C> treeRewriter) {
    return rewriteExpression(node, context, treeRewriter);
  }

  public Expression rewriteIsNotNullPredicate(
      IsNotNullPredicate node, C context, ExpressionTreeRewriter<C> treeRewriter) {
    return rewriteExpression(node, context, treeRewriter);
  }

  public Expression rewriteIsNullPredicate(
      IsNullPredicate node, C context, ExpressionTreeRewriter<C> treeRewriter) {
    return rewriteExpression(node, context, treeRewriter);
  }

  public Expression rewriteNullIfExpression(
      NullIfExpression node, C context, ExpressionTreeRewriter<C> treeRewriter) {
    return rewriteExpression(node, context, treeRewriter);
  }

  public Expression rewriteIfExpression(
      IfExpression node, C context, ExpressionTreeRewriter<C> treeRewriter) {
    return rewriteExpression(node, context, treeRewriter);
  }

  public Expression rewriteSearchedCaseExpression(
      SearchedCaseExpression node, C context, ExpressionTreeRewriter<C> treeRewriter) {
    return rewriteExpression(node, context, treeRewriter);
  }

  public Expression rewriteSimpleCaseExpression(
      SimpleCaseExpression node, C context, ExpressionTreeRewriter<C> treeRewriter) {
    return rewriteExpression(node, context, treeRewriter);
  }

  public Expression rewriteWhenClause(
      WhenClause node, C context, ExpressionTreeRewriter<C> treeRewriter) {
    return rewriteExpression(node, context, treeRewriter);
  }

  public Expression rewriteCoalesceExpression(
      CoalesceExpression node, C context, ExpressionTreeRewriter<C> treeRewriter) {
    return rewriteExpression(node, context, treeRewriter);
  }

  public Expression rewriteInListExpression(
      InListExpression node, C context, ExpressionTreeRewriter<C> treeRewriter) {
    return rewriteExpression(node, context, treeRewriter);
  }

  public Expression rewriteFunctionCall(
      FunctionCall node, C context, ExpressionTreeRewriter<C> treeRewriter) {
    return rewriteExpression(node, context, treeRewriter);
  }

  public Expression rewriteTrim(Trim node, C context, ExpressionTreeRewriter<C> treeRewriter) {
    return rewriteExpression(node, context, treeRewriter);
  }

  public Expression rewriteDereferenceExpression(
      DereferenceExpression node, C context, ExpressionTreeRewriter<C> treeRewriter) {
    return rewriteExpression(node, context, treeRewriter);
  }

  //    public Expression rewriteBindExpression(BindExpression node, C context,
  // ExpressionTreeRewriter<C> treeRewriter) {
  //        return rewriteExpression(node, context, treeRewriter);
  //    }
  public Expression rewriteLikePredicate(
      LikePredicate node, C context, ExpressionTreeRewriter<C> treeRewriter) {
    return rewriteExpression(node, context, treeRewriter);
  }

  public Expression rewriteLiteral(
      Literal node, C context, ExpressionTreeRewriter<C> treeRewriter) {
    return rewriteExpression(node, context, treeRewriter);
  }

  public Expression rewriteInPredicate(
      InPredicate node, C context, ExpressionTreeRewriter<C> treeRewriter) {
    return rewriteExpression(node, context, treeRewriter);
  }

  //    public Expression rewriteConstant(Constant node, C context, ExpressionTreeRewriter<C>
  // treeRewriter) {
  //        return rewriteExpression(node, context, treeRewriter);
  //    }

  public Expression rewriteCast(Cast node, C context, ExpressionTreeRewriter<C> treeRewriter) {
    return rewriteExpression(node, context, treeRewriter);
  }

  public Expression rewriteSymbolReference(
      SymbolReference node, C context, ExpressionTreeRewriter<C> treeRewriter) {
    return rewriteExpression(node, context, treeRewriter);
  }

  public Expression rewriteParameter(
      Parameter node, C context, ExpressionTreeRewriter<C> treeRewriter) {
    return rewriteExpression(node, context, treeRewriter);
  }

  public Expression rewriteQuantifiedComparison(
      QuantifiedComparisonExpression node, C context, ExpressionTreeRewriter<C> treeRewriter) {
    return rewriteExpression(node, context, treeRewriter);
  }

  public Expression rewriteGenericDataType(
      GenericDataType node, C context, ExpressionTreeRewriter<C> treeRewriter) {
    return rewriteExpression(node, context, treeRewriter);
  }
}
