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

package org.apache.iotdb.db.query.expression;

import org.apache.iotdb.db.exception.query.LogicalOptimizeException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.exception.sql.StatementAnalyzeException;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.mpp.common.schematree.PathPatternTree;
import org.apache.iotdb.db.mpp.sql.rewriter.WildcardsRemover;
import org.apache.iotdb.db.qp.physical.crud.UDTFPlan;
import org.apache.iotdb.db.query.expression.binary.AdditionExpression;
import org.apache.iotdb.db.query.expression.binary.DivisionExpression;
import org.apache.iotdb.db.query.expression.binary.EqualToExpression;
import org.apache.iotdb.db.query.expression.binary.GreaterEqualExpression;
import org.apache.iotdb.db.query.expression.binary.GreaterThanExpression;
import org.apache.iotdb.db.query.expression.binary.LessEqualExpression;
import org.apache.iotdb.db.query.expression.binary.LessThanExpression;
import org.apache.iotdb.db.query.expression.binary.LogicAndExpression;
import org.apache.iotdb.db.query.expression.binary.LogicOrExpression;
import org.apache.iotdb.db.query.expression.binary.ModuloExpression;
import org.apache.iotdb.db.query.expression.binary.MultiplicationExpression;
import org.apache.iotdb.db.query.expression.binary.NonEqualExpression;
import org.apache.iotdb.db.query.expression.binary.SubtractionExpression;
import org.apache.iotdb.db.query.expression.unary.ConstantOperand;
import org.apache.iotdb.db.query.expression.unary.FunctionExpression;
import org.apache.iotdb.db.query.expression.unary.LogicNotExpression;
import org.apache.iotdb.db.query.expression.unary.NegationExpression;
import org.apache.iotdb.db.query.expression.unary.RegularExpression;
import org.apache.iotdb.db.query.expression.unary.TimeSeriesOperand;
import org.apache.iotdb.db.query.udf.core.executor.UDTFContext;
import org.apache.iotdb.db.query.udf.core.executor.UDTFExecutor;
import org.apache.iotdb.db.query.udf.core.layer.IntermediateLayer;
import org.apache.iotdb.db.query.udf.core.layer.LayerMemoryAssigner;
import org.apache.iotdb.db.query.udf.core.layer.RawQueryInputLayer;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.ZoneId;
import java.util.Deque;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** A skeleton class for expression */
public abstract class Expression {

  private String expressionStringCache;

  protected Boolean isConstantOperandCache = null;

  protected Integer inputColumnIndex = null;

  public boolean isBuiltInAggregationFunctionExpression() {
    return false;
  }

  public boolean isUserDefinedAggregationFunctionExpression() {
    return false;
  }

  public boolean isTimeSeriesGeneratingFunctionExpression() {
    return false;
  }

  public abstract void concat(
      List<PartialPath> prefixPaths,
      List<Expression> resultExpressions,
      PathPatternTree patternTree);

  // TODO: remove after MPP finish
  public abstract void concat(List<PartialPath> prefixPaths, List<Expression> resultExpressions);

  public abstract void removeWildcards(
      WildcardsRemover wildcardsRemover, List<Expression> resultExpressions)
      throws StatementAnalyzeException;

  // TODO: remove after MPP finish
  public abstract void removeWildcards(
      org.apache.iotdb.db.qp.utils.WildcardsRemover wildcardsRemover,
      List<Expression> resultExpressions)
      throws LogicalOptimizeException;

  public abstract void collectPaths(Set<PartialPath> pathSet);

  public abstract void constructUdfExecutors(
      Map<String, UDTFExecutor> expressionName2Executor, ZoneId zoneId);

  public abstract void bindInputLayerColumnIndexWithExpression(UDTFPlan udtfPlan);

  public abstract void updateStatisticsForMemoryAssigner(LayerMemoryAssigner memoryAssigner);

  public abstract IntermediateLayer constructIntermediateLayer(
      long queryId,
      UDTFContext udtfContext,
      RawQueryInputLayer rawTimeSeriesInputLayer,
      Map<Expression, IntermediateLayer> expressionIntermediateLayerMap,
      Map<Expression, TSDataType> expressionDataTypeMap,
      LayerMemoryAssigner memoryAssigner)
      throws QueryProcessException, IOException;

  /** Sub-classes should override this method indicating if the expression is a constant operand */
  protected abstract boolean isConstantOperandInternal();

  /**
   * returns the DIRECT children expressions if it has any, otherwise an EMPTY list will be returned
   */
  public abstract List<Expression> getExpressions();

  /** If this expression and all of its sub-expressions are {@link ConstantOperand}. */
  public final boolean isConstantOperand() {
    if (isConstantOperandCache == null) {
      isConstantOperandCache = isConstantOperandInternal();
    }
    return isConstantOperandCache;
  }

  /**
   * Sub-classes should override this method to provide valid string representation of this object.
   * See {@link #getExpressionString()}
   */
  protected abstract String getExpressionStringInternal();

  /**
   * Get the representation of the expression in string. The hash code of the returned value will be
   * the hash code of this object. See {@link #hashCode()} and {@link #equals(Object)}. In other
   * words, same expressions should have exactly the same string representation, and different
   * expressions must have different string representations.
   */
  public final String getExpressionString() {
    if (expressionStringCache == null) {
      expressionStringCache = getExpressionStringInternal();
    }
    return expressionStringCache;
  }

  /** Sub-classes must not override this method. */
  @Override
  public final int hashCode() {
    return getExpressionString().hashCode();
  }

  /** Sub-classes must not override this method. */
  @Override
  public final boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (!(o instanceof Expression)) {
      return false;
    }

    return getExpressionString().equals(((Expression) o).getExpressionString());
  }

  /** Sub-classes must not override this method. */
  @Override
  public final String toString() {
    return getExpressionString();
  }

  /** returns an iterator to traverse all the successor expressions in a level-order */
  public final Iterator<Expression> iterator() {
    return new ExpressionIterator(this);
  }

  /** the iterator of an Expression tree with level-order traversal */
  private static class ExpressionIterator implements Iterator<Expression> {

    private final Deque<Expression> queue = new LinkedList<>();

    public ExpressionIterator(Expression expression) {
      queue.add(expression);
    }

    @Override
    public boolean hasNext() {
      return !queue.isEmpty();
    }

    @Override
    public Expression next() {
      if (!hasNext()) {
        return null;
      }
      Expression current = queue.pop();
      if (current != null) {
        for (Expression subExp : current.getExpressions()) {
          queue.push(subExp);
        }
      }
      return current;
    }
  }

  protected abstract short getExpressionType();

  public static void serialize(Expression expression, ByteBuffer byteBuffer) {
    ReadWriteIOUtils.write(expression.getExpressionType(), byteBuffer);
    expression.serialize(byteBuffer);
  }

  public abstract void serialize(ByteBuffer byteBuffer);

  public static Expression deserialize(ByteBuffer byteBuffer) {
    short type = ReadWriteIOUtils.readShort(byteBuffer);
    switch (type) {
      case 0:
        return new AdditionExpression(byteBuffer);
      case 1:
        return new DivisionExpression(byteBuffer);
      case 2:
        return new EqualToExpression(byteBuffer);
      case 3:
        return new GreaterEqualExpression(byteBuffer);
      case 4:
        return new GreaterThanExpression(byteBuffer);
      case 5:
        return new LessEqualExpression(byteBuffer);
      case 6:
        return new LessThanExpression(byteBuffer);
      case 7:
        return new LogicAndExpression(byteBuffer);
      case 8:
        return new LogicOrExpression(byteBuffer);
      case 9:
        return new ModuloExpression(byteBuffer);
      case 10:
        return new MultiplicationExpression(byteBuffer);
      case 11:
        return new NonEqualExpression(byteBuffer);
      case 12:
        return new SubtractionExpression(byteBuffer);
      case 13:
        return new FunctionExpression(byteBuffer);
      case 14:
        return new LogicNotExpression(byteBuffer);
      case 15:
        return new NegationExpression(byteBuffer);
      case 16:
        return new TimeSeriesOperand(byteBuffer);
      case 17:
        return new ConstantOperand(byteBuffer);
      case 18:
        throw new IllegalArgumentException("Invalid expression type: " + type);
      case 19:
        return new RegularExpression(byteBuffer);
      default:
        throw new IllegalArgumentException("Invalid expression type: " + type);
    }
  }
}
