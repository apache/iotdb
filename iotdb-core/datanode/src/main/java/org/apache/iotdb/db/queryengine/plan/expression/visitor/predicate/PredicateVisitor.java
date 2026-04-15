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

package org.apache.iotdb.db.queryengine.plan.expression.visitor.predicate;

import org.apache.iotdb.db.queryengine.plan.expression.Expression;
import org.apache.iotdb.db.queryengine.plan.expression.binary.EqualToExpression;
import org.apache.iotdb.db.queryengine.plan.expression.binary.GreaterEqualExpression;
import org.apache.iotdb.db.queryengine.plan.expression.binary.GreaterThanExpression;
import org.apache.iotdb.db.queryengine.plan.expression.binary.LessEqualExpression;
import org.apache.iotdb.db.queryengine.plan.expression.binary.LessThanExpression;
import org.apache.iotdb.db.queryengine.plan.expression.binary.LogicAndExpression;
import org.apache.iotdb.db.queryengine.plan.expression.binary.LogicOrExpression;
import org.apache.iotdb.db.queryengine.plan.expression.binary.NonEqualExpression;
import org.apache.iotdb.db.queryengine.plan.expression.other.GroupByTimeExpression;
import org.apache.iotdb.db.queryengine.plan.expression.ternary.BetweenExpression;
import org.apache.iotdb.db.queryengine.plan.expression.unary.InExpression;
import org.apache.iotdb.db.queryengine.plan.expression.unary.IsNullExpression;
import org.apache.iotdb.db.queryengine.plan.expression.unary.LikeExpression;
import org.apache.iotdb.db.queryengine.plan.expression.unary.LogicNotExpression;
import org.apache.iotdb.db.queryengine.plan.expression.unary.RegularExpression;
import org.apache.iotdb.db.queryengine.plan.expression.visitor.ExpressionVisitor;

/**
 * This class provides a visitor of {@link Expression}, which can be extended to create a visitor
 * which only needs to handle a subset of the available methods.
 *
 * @param <R> The return type of the visit operation.
 * @param <C> The context information during visiting.
 */
public abstract class PredicateVisitor<R, C> extends ExpressionVisitor<R, C> {

  @Override
  public R visitExpression(Expression expression, C context) {
    throw new IllegalArgumentException(
        "Unsupported expression type: " + expression.getExpressionType());
  }

  @Override
  public abstract R visitInExpression(InExpression inExpression, C context);

  @Override
  public abstract R visitIsNullExpression(IsNullExpression isNullExpression, C context);

  @Override
  public abstract R visitLikeExpression(LikeExpression likeExpression, C context);

  @Override
  public abstract R visitRegularExpression(RegularExpression regularExpression, C context);

  @Override
  public abstract R visitLogicNotExpression(LogicNotExpression logicNotExpression, C context);

  @Override
  public abstract R visitLogicAndExpression(LogicAndExpression logicAndExpression, C context);

  @Override
  public abstract R visitLogicOrExpression(LogicOrExpression logicOrExpression, C context);

  @Override
  public abstract R visitEqualToExpression(EqualToExpression equalToExpression, C context);

  @Override
  public abstract R visitNonEqualExpression(NonEqualExpression nonEqualExpression, C context);

  @Override
  public abstract R visitGreaterThanExpression(
      GreaterThanExpression greaterThanExpression, C context);

  @Override
  public abstract R visitGreaterEqualExpression(
      GreaterEqualExpression greaterEqualExpression, C context);

  @Override
  public abstract R visitLessThanExpression(LessThanExpression lessThanExpression, C context);

  @Override
  public abstract R visitLessEqualExpression(LessEqualExpression lessEqualExpression, C context);

  @Override
  public abstract R visitBetweenExpression(BetweenExpression betweenExpression, C context);

  @Override
  public abstract R visitGroupByTimeExpression(
      GroupByTimeExpression groupByTimeExpression, C context);
}
