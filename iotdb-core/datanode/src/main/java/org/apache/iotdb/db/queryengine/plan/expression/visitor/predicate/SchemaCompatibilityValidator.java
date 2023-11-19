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

import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.queryengine.plan.expression.Expression;
import org.apache.iotdb.db.queryengine.plan.expression.binary.EqualToExpression;
import org.apache.iotdb.db.queryengine.plan.expression.binary.GreaterEqualExpression;
import org.apache.iotdb.db.queryengine.plan.expression.binary.GreaterThanExpression;
import org.apache.iotdb.db.queryengine.plan.expression.binary.LessEqualExpression;
import org.apache.iotdb.db.queryengine.plan.expression.binary.LessThanExpression;
import org.apache.iotdb.db.queryengine.plan.expression.binary.LogicAndExpression;
import org.apache.iotdb.db.queryengine.plan.expression.binary.LogicOrExpression;
import org.apache.iotdb.db.queryengine.plan.expression.binary.NonEqualExpression;
import org.apache.iotdb.db.queryengine.plan.expression.leaf.ConstantOperand;
import org.apache.iotdb.db.queryengine.plan.expression.leaf.TimeSeriesOperand;
import org.apache.iotdb.db.queryengine.plan.expression.leaf.TimestampOperand;
import org.apache.iotdb.db.queryengine.plan.expression.other.GroupByTimeExpression;
import org.apache.iotdb.db.queryengine.plan.expression.ternary.BetweenExpression;
import org.apache.iotdb.db.queryengine.plan.expression.unary.InExpression;
import org.apache.iotdb.db.queryengine.plan.expression.unary.IsNullExpression;
import org.apache.iotdb.db.queryengine.plan.expression.unary.LikeExpression;
import org.apache.iotdb.db.queryengine.plan.expression.unary.LogicNotExpression;
import org.apache.iotdb.db.queryengine.plan.expression.unary.RegularExpression;

public class SchemaCompatibilityValidator
    extends PredicateVisitor<Void, SchemaCompatibilityValidator.Context> {

  public static void validate(Expression predicate) {
    predicate.accept(new SchemaCompatibilityValidator(), new Context());
  }

  protected static class Context {

    private PartialPath checkedPath;
    private boolean isAligned;

    public Context() {
      // needn't init
    }

    public void check(MeasurementPath path) {
      if (checkedPath == null) {
        // init checkedPath
        isAligned = path.isUnderAlignedEntity();
        checkedPath = getComparePath(path);
      } else {
        if (isAligned != path.isUnderAlignedEntity() || !checkedPath.equals(getComparePath(path))) {
          throw new IllegalArgumentException(
              "The paths in the predicate are not compatible with each other.");
        }
      }
    }

    private PartialPath getComparePath(MeasurementPath path) {
      if (path.isUnderAlignedEntity()) return path.getDevicePath();
      return path;
    }
  }

  @Override
  public Void visitTimeSeriesOperand(TimeSeriesOperand timeSeriesOperand, Context context) {
    context.check((MeasurementPath) timeSeriesOperand.getPath());
    return null;
  }

  @Override
  public Void visitTimeStampOperand(TimestampOperand timestampOperand, Context context) {
    // needn't check
    return null;
  }

  @Override
  public Void visitConstantOperand(ConstantOperand constantOperand, Context context) {
    // needn't check
    return null;
  }

  @Override
  public Void visitGroupByTimeExpression(
      GroupByTimeExpression groupByTimeExpression, Context context) {
    // needn't check
    return null;
  }

  @Override
  public Void visitInExpression(InExpression inExpression, Context context) {
    return inExpression.getExpression().accept(this, context);
  }

  @Override
  public Void visitIsNullExpression(IsNullExpression isNullExpression, Context context) {
    return isNullExpression.getExpression().accept(this, context);
  }

  @Override
  public Void visitLikeExpression(LikeExpression likeExpression, Context context) {
    return likeExpression.getExpression().accept(this, context);
  }

  @Override
  public Void visitRegularExpression(RegularExpression regularExpression, Context context) {
    return regularExpression.getExpression().accept(this, context);
  }

  @Override
  public Void visitLogicNotExpression(LogicNotExpression logicNotExpression, Context context) {
    return logicNotExpression.getExpression().accept(this, context);
  }

  @Override
  public Void visitLogicAndExpression(LogicAndExpression logicAndExpression, Context context) {
    logicAndExpression.getLeftExpression().accept(this, context);
    logicAndExpression.getRightExpression().accept(this, context);
    return null;
  }

  @Override
  public Void visitLogicOrExpression(LogicOrExpression logicOrExpression, Context context) {
    logicOrExpression.getLeftExpression().accept(this, context);
    logicOrExpression.getRightExpression().accept(this, context);
    return null;
  }

  @Override
  public Void visitEqualToExpression(EqualToExpression equalToExpression, Context context) {
    equalToExpression.getLeftExpression().accept(this, context);
    equalToExpression.getRightExpression().accept(this, context);
    return null;
  }

  @Override
  public Void visitNonEqualExpression(NonEqualExpression nonEqualExpression, Context context) {
    nonEqualExpression.getLeftExpression().accept(this, context);
    nonEqualExpression.getRightExpression().accept(this, context);
    return null;
  }

  @Override
  public Void visitGreaterThanExpression(
      GreaterThanExpression greaterThanExpression, Context context) {
    greaterThanExpression.getLeftExpression().accept(this, context);
    greaterThanExpression.getRightExpression().accept(this, context);
    return null;
  }

  @Override
  public Void visitGreaterEqualExpression(
      GreaterEqualExpression greaterEqualExpression, Context context) {
    greaterEqualExpression.getLeftExpression().accept(this, context);
    greaterEqualExpression.getRightExpression().accept(this, context);
    return null;
  }

  @Override
  public Void visitLessThanExpression(LessThanExpression lessThanExpression, Context context) {
    lessThanExpression.getLeftExpression().accept(this, context);
    lessThanExpression.getRightExpression().accept(this, context);
    return null;
  }

  @Override
  public Void visitLessEqualExpression(LessEqualExpression lessEqualExpression, Context context) {
    lessEqualExpression.getLeftExpression().accept(this, context);
    lessEqualExpression.getRightExpression().accept(this, context);
    return null;
  }

  @Override
  public Void visitBetweenExpression(BetweenExpression betweenExpression, Context context) {
    betweenExpression.getFirstExpression().accept(this, context);
    betweenExpression.getSecondExpression().accept(this, context);
    betweenExpression.getThirdExpression().accept(this, context);
    return null;
  }
}
