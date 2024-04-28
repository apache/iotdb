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

import org.apache.iotdb.db.relational.sql.tree.ArithmeticBinaryExpression;
import org.apache.iotdb.db.relational.sql.tree.BetweenPredicate;
import org.apache.iotdb.db.relational.sql.tree.Cast;
import org.apache.iotdb.db.relational.sql.tree.CoalesceExpression;
import org.apache.iotdb.db.relational.sql.tree.ComparisonExpression;
import org.apache.iotdb.db.relational.sql.tree.Expression;
import org.apache.iotdb.db.relational.sql.tree.FunctionCall;
import org.apache.iotdb.db.relational.sql.tree.InPredicate;
import org.apache.iotdb.db.relational.sql.tree.IsNullPredicate;
import org.apache.iotdb.db.relational.sql.tree.LogicalExpression;
import org.apache.iotdb.db.relational.sql.tree.NotExpression;
import org.apache.iotdb.db.relational.sql.tree.NullIfExpression;
import org.apache.iotdb.db.relational.sql.tree.Row;
import org.apache.iotdb.db.relational.sql.tree.SearchedCaseExpression;
import org.apache.iotdb.db.relational.sql.tree.SimpleCaseExpression;
import org.apache.iotdb.db.relational.sql.tree.SymbolReference;

public class ExpressionRewriter<C> {

  protected Expression rewriteExpression(
      Expression node, C context, ExpressionTreeRewriter<C> treeRewriter) {
    return null;
    //    throw new IllegalStateException(
    //        String.format("%s is not supported in ExpressionRewriter yet",
    // node.getClass().getName()));
  }

  public Expression rewriteRow(Row node, C context, ExpressionTreeRewriter<C> treeRewriter) {
    return rewriteExpression(node, context, treeRewriter);
  }

  //    public Expression rewriteArithmeticUnary(ArithmeticNegation node, C context,
  // ExpressionTreeRewriter<C> treeRewriter) {
  //        return rewriteExpression(node, context, treeRewriter);
  //    }

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

  public Expression rewriteIsNullPredicate(
      IsNullPredicate node, C context, ExpressionTreeRewriter<C> treeRewriter) {
    return rewriteExpression(node, context, treeRewriter);
  }

  public Expression rewriteNullIfExpression(
      NullIfExpression node, C context, ExpressionTreeRewriter<C> treeRewriter) {
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

  public Expression rewriteCoalesceExpression(
      CoalesceExpression node, C context, ExpressionTreeRewriter<C> treeRewriter) {
    return rewriteExpression(node, context, treeRewriter);
  }

  public Expression rewriteFunctionCall(
      FunctionCall node, C context, ExpressionTreeRewriter<C> treeRewriter) {
    return rewriteExpression(node, context, treeRewriter);
  }

  //    public Expression rewriteBindExpression(BindExpression node, C context,
  // ExpressionTreeRewriter<C> treeRewriter) {
  //        return rewriteExpression(node, context, treeRewriter);
  //    }

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
}
