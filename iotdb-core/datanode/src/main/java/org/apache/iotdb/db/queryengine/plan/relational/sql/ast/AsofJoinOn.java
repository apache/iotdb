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

package org.apache.iotdb.db.queryengine.plan.relational.sql.ast;

import org.apache.iotdb.db.exception.sql.SemanticException;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import org.apache.tsfile.utils.RamUsageEstimator;
import org.apache.tsfile.utils.TimeDuration;

import java.util.List;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.ir.IrUtils.and;
import static org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ComparisonExpression.Operator.LESS_THAN_OR_EQUAL;

public class AsofJoinOn extends JoinOn {
  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(AsofJoinOn.class);

  // record main expression of ASOF join
  // .e.g 'ASOF (tolerance 1) JOIN ON t1.device = t2.device and t1.time > t2.time' =>
  // asofExpression:'t1.time > t2.time', expression in super Class: 't1.device = t2.device and
  // t1.time <= t2.time +
  // 1'
  private final Expression asofExpression;

  private static final String joinErrMsg =
      "The join expression of ASOF should be single Comparison or Logical 'AND'";
  private static final String asofErrMsg =
      "The main join expression of ASOF should only be Comparison '>, >=, <, <=', actual is %s";

  public AsofJoinOn(Expression otherExpression, Expression asofExpression) {
    super(otherExpression);
    this.asofExpression = asofExpression;
  }

  public Expression getAsofExpression() {
    return asofExpression;
  }

  public static JoinCriteria constructAsofJoinOn(
      Expression joinExpression, TimeDuration timeDuration) {
    ImmutableList.Builder<Expression> newTerms = ImmutableList.builder();
    ComparisonExpression asofExpression;
    if (joinExpression instanceof ComparisonExpression) {
      asofExpression = (ComparisonExpression) joinExpression;
    } else if (joinExpression instanceof LogicalExpression) {
      LogicalExpression logicalExpression = (LogicalExpression) joinExpression;
      if (logicalExpression.getOperator() != LogicalExpression.Operator.AND) {
        throw new SemanticException(joinErrMsg);
      }

      List<Expression> terms = logicalExpression.getTerms();
      Expression lastExpression = Iterables.getLast(terms);
      if (!(lastExpression instanceof ComparisonExpression)) {
        throw new SemanticException(String.format(asofErrMsg, lastExpression));
      }
      asofExpression = (ComparisonExpression) lastExpression;

      int size = terms.size() - 1;
      for (int i = 0; i < size; i++) {
        newTerms.add(terms.get(i));
      }
    } else {
      throw new SemanticException(joinErrMsg);
    }

    // add tolerance condition to expression in super Class, it will be extracted to post-Join
    // filter in later process
    if (timeDuration != null) {
      Expression timeInterval = new LongLiteral(String.valueOf(timeDuration.nonMonthDuration));
      switch (asofExpression.getOperator()) {
        case GREATER_THAN:
        case GREATER_THAN_OR_EQUAL:
          newTerms.add(
              new ComparisonExpression(
                  LESS_THAN_OR_EQUAL,
                  asofExpression.getLeft(),
                  new ArithmeticBinaryExpression(
                      ArithmeticBinaryExpression.Operator.ADD,
                      asofExpression.getRight(),
                      timeInterval)));
          break;
        case LESS_THAN:
        case LESS_THAN_OR_EQUAL:
          newTerms.add(
              new ComparisonExpression(
                  LESS_THAN_OR_EQUAL,
                  asofExpression.getRight(),
                  new ArithmeticBinaryExpression(
                      ArithmeticBinaryExpression.Operator.ADD,
                      asofExpression.getLeft(),
                      timeInterval)));
          break;
        default:
          throw new SemanticException(String.format(asofErrMsg, asofExpression));
      }
    } else {
      // also check Comparison type
      switch (asofExpression.getOperator()) {
        case GREATER_THAN:
        case GREATER_THAN_OR_EQUAL:
        case LESS_THAN:
        case LESS_THAN_OR_EQUAL:
          break;
        default:
          throw new SemanticException(String.format(asofErrMsg, asofExpression));
      }
    }

    List<Expression> newTermList = newTerms.build();
    if (newTermList.isEmpty()) {
      return new AsofJoinOn(null, asofExpression);
    } else {
      return new AsofJoinOn(and(newTermList), asofExpression);
    }
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if ((obj == null) || (getClass() != obj.getClass())) {
      return false;
    }
    AsofJoinOn o = (AsofJoinOn) obj;
    return Objects.equals(expression, o.expression)
        && Objects.equals(asofExpression, o.asofExpression);
  }

  @Override
  public int hashCode() {
    return Objects.hash(expression, asofExpression);
  }

  @Override
  public String toString() {
    return toStringHelper(this).addValue(expression).addValue(asofExpression).toString();
  }

  @Override
  public List<Node> getNodes() {
    return ImmutableList.of(expression, asofExpression);
  }

  @Override
  public long ramBytesUsed() {
    long size = INSTANCE_SIZE;
    size += ramBytesUsedExcludingInstanceSize();
    size += AstMemoryEstimationHelper.getEstimatedSizeOfAccountableObject(asofExpression);
    return size;
  }
}
