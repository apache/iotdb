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

/** Extract global time predicate from query predicate. */
public class ExtractGlobalTimePredicateVisitor
    extends PredicateVisitor<Expression, ExtractGlobalTimePredicateVisitor.Context> {

  @Override
  public Expression visitInExpression(InExpression inExpression, Context context) {
    return null;
  }

  @Override
  public Expression visitGroupByTimeExpression(
      GroupByTimeExpression groupByTimeExpression, Context context) {
    return null;
  }

  @Override
  public Expression visitIsNullExpression(IsNullExpression isNullExpression, Context context) {
    return null;
  }

  @Override
  public Expression visitLikeExpression(LikeExpression likeExpression, Context context) {
    return null;
  }

  @Override
  public Expression visitRegularExpression(RegularExpression regularExpression, Context context) {
    return null;
  }

  @Override
  public Expression visitLogicNotExpression(
      LogicNotExpression logicNotExpression, Context context) {
    return null;
  }

  @Override
  public Expression visitLogicAndExpression(
      LogicAndExpression logicAndExpression, Context context) {
    return null;
  }

  @Override
  public Expression visitLogicOrExpression(LogicOrExpression logicOrExpression, Context context) {
    return null;
  }

  @Override
  public Expression visitEqualToExpression(EqualToExpression equalToExpression, Context context) {
    return null;
  }

  @Override
  public Expression visitNonEqualExpression(
      NonEqualExpression nonEqualExpression, Context context) {
    return null;
  }

  @Override
  public Expression visitGreaterThanExpression(
      GreaterThanExpression greaterThanExpression, Context context) {
    return null;
  }

  @Override
  public Expression visitGreaterEqualExpression(
      GreaterEqualExpression greaterEqualExpression, Context context) {
    return null;
  }

  @Override
  public Expression visitLessThanExpression(
      LessThanExpression lessThanExpression, Context context) {
    return null;
  }

  @Override
  public Expression visitLessEqualExpression(
      LessEqualExpression lessEqualExpression, Context context) {
    return null;
  }

  @Override
  public Expression visitBetweenExpression(BetweenExpression betweenExpression, Context context) {
    return null;
  }

  protected static class Context {

    boolean hasValueFilter = false;

    // determined by the father of current expression
    boolean canRewrite = false;

    // whether it is the first LogicOrExpression encountered
    boolean isFirstOr = true;
  }
}
