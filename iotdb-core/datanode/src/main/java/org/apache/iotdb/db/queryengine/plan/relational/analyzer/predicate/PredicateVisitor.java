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

package org.apache.iotdb.db.queryengine.plan.relational.analyzer.predicate;

import org.apache.iotdb.db.queryengine.plan.relational.sql.tree.AstVisitor;
import org.apache.iotdb.db.queryengine.plan.relational.sql.tree.BetweenPredicate;
import org.apache.iotdb.db.queryengine.plan.relational.sql.tree.ComparisonExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.tree.Expression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.tree.IfExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.tree.InPredicate;
import org.apache.iotdb.db.queryengine.plan.relational.sql.tree.IsNotNullPredicate;
import org.apache.iotdb.db.queryengine.plan.relational.sql.tree.IsNullPredicate;
import org.apache.iotdb.db.queryengine.plan.relational.sql.tree.LikePredicate;
import org.apache.iotdb.db.queryengine.plan.relational.sql.tree.LogicalExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.tree.NotExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.tree.NullIfExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.tree.SearchedCaseExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.tree.SimpleCaseExpression;

/**
 * This class provides a visitor of {@link Expression}, which can be extended to create a visitor
 * which only needs to handle a subset of the available methods.
 *
 * @param <R> The return type of the visit operation.
 * @param <C> The context information during visiting.
 */
public abstract class PredicateVisitor<R, C> extends AstVisitor<R, C> {

  @Override
  public R visitExpression(Expression expression, C context) {
    throw new IllegalArgumentException(
        "Unsupported expression: " + expression.getClass().getSimpleName());
  }

  @Override
  protected abstract R visitInPredicate(InPredicate node, C context);

  @Override
  protected abstract R visitIsNullPredicate(IsNullPredicate node, C context);

  @Override
  protected abstract R visitIsNotNullPredicate(IsNotNullPredicate node, C context);

  @Override
  protected abstract R visitLikePredicate(LikePredicate node, C context);

  @Override
  protected abstract R visitLogicalExpression(LogicalExpression node, C context);

  @Override
  protected abstract R visitNotExpression(NotExpression node, C context);

  @Override
  protected abstract R visitComparisonExpression(ComparisonExpression node, C context);

  @Override
  protected abstract R visitSimpleCaseExpression(SimpleCaseExpression node, C context);

  @Override
  protected abstract R visitSearchedCaseExpression(SearchedCaseExpression node, C context);

  @Override
  protected abstract R visitIfExpression(IfExpression node, C context);

  @Override
  protected abstract R visitNullIfExpression(NullIfExpression node, C context);

  @Override
  protected abstract R visitBetweenPredicate(BetweenPredicate node, C context);
}
