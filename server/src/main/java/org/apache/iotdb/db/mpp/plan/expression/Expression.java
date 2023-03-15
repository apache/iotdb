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

package org.apache.iotdb.db.mpp.plan.expression;

import org.apache.iotdb.db.mpp.common.NodeRef;
import org.apache.iotdb.db.mpp.plan.expression.binary.AdditionExpression;
import org.apache.iotdb.db.mpp.plan.expression.binary.DivisionExpression;
import org.apache.iotdb.db.mpp.plan.expression.binary.EqualToExpression;
import org.apache.iotdb.db.mpp.plan.expression.binary.GreaterEqualExpression;
import org.apache.iotdb.db.mpp.plan.expression.binary.GreaterThanExpression;
import org.apache.iotdb.db.mpp.plan.expression.binary.LessEqualExpression;
import org.apache.iotdb.db.mpp.plan.expression.binary.LessThanExpression;
import org.apache.iotdb.db.mpp.plan.expression.binary.LogicAndExpression;
import org.apache.iotdb.db.mpp.plan.expression.binary.LogicOrExpression;
import org.apache.iotdb.db.mpp.plan.expression.binary.ModuloExpression;
import org.apache.iotdb.db.mpp.plan.expression.binary.MultiplicationExpression;
import org.apache.iotdb.db.mpp.plan.expression.binary.NonEqualExpression;
import org.apache.iotdb.db.mpp.plan.expression.binary.SubtractionExpression;
import org.apache.iotdb.db.mpp.plan.expression.leaf.ConstantOperand;
import org.apache.iotdb.db.mpp.plan.expression.leaf.NullOperand;
import org.apache.iotdb.db.mpp.plan.expression.leaf.TimeSeriesOperand;
import org.apache.iotdb.db.mpp.plan.expression.leaf.TimestampOperand;
import org.apache.iotdb.db.mpp.plan.expression.multi.FunctionExpression;
import org.apache.iotdb.db.mpp.plan.expression.ternary.BetweenExpression;
import org.apache.iotdb.db.mpp.plan.expression.unary.InExpression;
import org.apache.iotdb.db.mpp.plan.expression.unary.IsNullExpression;
import org.apache.iotdb.db.mpp.plan.expression.unary.LikeExpression;
import org.apache.iotdb.db.mpp.plan.expression.unary.LogicNotExpression;
import org.apache.iotdb.db.mpp.plan.expression.unary.NegationExpression;
import org.apache.iotdb.db.mpp.plan.expression.unary.RegularExpression;
import org.apache.iotdb.db.mpp.plan.expression.visitor.ExpressionVisitor;
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.InputLocation;
import org.apache.iotdb.db.mpp.plan.statement.StatementNode;
import org.apache.iotdb.db.mpp.transformation.dag.memory.LayerMemoryAssigner;
import org.apache.iotdb.db.mpp.transformation.dag.udf.UDTFExecutor;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.ZoneId;
import java.util.Deque;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/** A skeleton class for expression */
public abstract class Expression extends StatementNode {
  /////////////////////////////////////////////////////////////////////////////////////////////////
  // Operations that Class Expression is not responsible for should be done through a visitor
  /////////////////////////////////////////////////////////////////////////////////////////////////

  /** Accessible for {@link ExpressionVisitor}, use {@link ExpressionVisitor#process} instead. */
  public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
    return visitor.visitExpression(this, context);
  }

  /////////////////////////////////////////////////////////////////////////////////////////////////
  // Expression type inferring for execution plan generation
  /////////////////////////////////////////////////////////////////////////////////////////////////

  public abstract ExpressionType getExpressionType();

  public boolean isBuiltInAggregationFunctionExpression() {
    return false;
  }

  public boolean isCompareBinaryExpression() {
    return false;
  }

  public abstract boolean isMappable(Map<NodeRef<Expression>, TSDataType> expressionTypes);

  /////////////////////////////////////////////////////////////////////////////////////////////////
  // isConstantOperand
  /////////////////////////////////////////////////////////////////////////////////////////////////

  protected Boolean isConstantOperandCache = null;

  /** If this expression and all of its sub-expressions are {@link ConstantOperand}. */
  public final boolean isConstantOperand() {
    if (isConstantOperandCache == null) {
      isConstantOperandCache = isConstantOperandInternal();
    }
    return isConstantOperandCache;
  }

  /** Sub-classes should override this method indicating if the expression is a constant operand */
  protected abstract boolean isConstantOperandInternal();

  /////////////////////////////////////////////////////////////////////////////////////////////////
  // For UDF instances initialization
  /////////////////////////////////////////////////////////////////////////////////////////////////

  public abstract void constructUdfExecutors(
      Map<String, UDTFExecutor> expressionName2Executor, ZoneId zoneId);

  /////////////////////////////////////////////////////////////////////////////////////////////////
  // For expression evaluation DAG building
  /////////////////////////////////////////////////////////////////////////////////////////////////

  protected Integer inputColumnIndex = null;

  public abstract void bindInputLayerColumnIndexWithExpression(
      Map<String, List<InputLocation>> inputLocations);

  public Integer getInputColumnIndex() {
    return inputColumnIndex;
  }

  public abstract void updateStatisticsForMemoryAssigner(LayerMemoryAssigner memoryAssigner);

  /////////////////////////////////////////////////////////////////////////////////////////////////
  // toString
  /////////////////////////////////////////////////////////////////////////////////////////////////

  private String expressionStringCache;

  /** Sub-classes must not override this method. */
  @Override
  public final String toString() {
    return getExpressionString();
  }

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

  /**
   * Sub-classes should override this method to provide valid string representation of this object.
   * See {@link #getExpressionString()}
   */
  protected abstract String getExpressionStringInternal();

  /////////////////////////////////////////////////////////////////////////////////////////////////
  // hashCode & equals
  /////////////////////////////////////////////////////////////////////////////////////////////////

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

  /////////////////////////////////////////////////////////////////////////////////////////////////
  // serialize & deserialize
  /////////////////////////////////////////////////////////////////////////////////////////////////

  public static void serialize(Expression expression, ByteBuffer byteBuffer) {
    ReadWriteIOUtils.write(
        expression.getExpressionType().getExpressionTypeInShortEnum(), byteBuffer);

    expression.serialize(byteBuffer);

    ReadWriteIOUtils.write(expression.inputColumnIndex != null, byteBuffer);
    if (expression.inputColumnIndex != null) {
      ReadWriteIOUtils.write(expression.inputColumnIndex, byteBuffer);
    }
  }

  public static void serialize(Expression expression, DataOutputStream stream) throws IOException {
    ReadWriteIOUtils.write(expression.getExpressionType().getExpressionTypeInShortEnum(), stream);

    expression.serialize(stream);

    ReadWriteIOUtils.write(expression.inputColumnIndex != null, stream);
    if (expression.inputColumnIndex != null) {
      ReadWriteIOUtils.write(expression.inputColumnIndex, stream);
    }
  }

  public static Expression deserialize(ByteBuffer byteBuffer) {
    short type = ReadWriteIOUtils.readShort(byteBuffer);

    Expression expression;
    switch (type) {
      case -4:
        expression = new ConstantOperand(byteBuffer);
        break;
      case -3:
        expression = new TimestampOperand(byteBuffer);
        break;
      case -2:
        expression = new TimeSeriesOperand(byteBuffer);
        break;
      case -1:
        expression = new FunctionExpression(byteBuffer);
        break;

      case 0:
        expression = new NegationExpression(byteBuffer);
        break;
      case 1:
        expression = new LogicNotExpression(byteBuffer);
        break;

      case 2:
        expression = new MultiplicationExpression(byteBuffer);
        break;
      case 3:
        expression = new DivisionExpression(byteBuffer);
        break;
      case 4:
        expression = new ModuloExpression(byteBuffer);
        break;

      case 5:
        expression = new AdditionExpression(byteBuffer);
        break;
      case 6:
        expression = new SubtractionExpression(byteBuffer);
        break;

      case 7:
        expression = new EqualToExpression(byteBuffer);
        break;
      case 8:
        expression = new NonEqualExpression(byteBuffer);
        break;
      case 9:
        expression = new GreaterEqualExpression(byteBuffer);
        break;
      case 10:
        expression = new GreaterThanExpression(byteBuffer);
        break;
      case 11:
        expression = new LessEqualExpression(byteBuffer);
        break;
      case 12:
        expression = new LessThanExpression(byteBuffer);
        break;

      case 13:
        expression = new LikeExpression(byteBuffer);
        break;
      case 14:
        expression = new RegularExpression(byteBuffer);
        break;

      case 15:
        expression = new IsNullExpression(byteBuffer);
        break;

      case 16:
        expression = new BetweenExpression(byteBuffer);
        break;

      case 17:
        expression = new InExpression(byteBuffer);
        break;

      case 18:
        expression = new LogicAndExpression(byteBuffer);
        break;

      case 19:
        expression = new LogicOrExpression(byteBuffer);
        break;

      case 20:
        expression = new NullOperand();
        break;

      default:
        throw new IllegalArgumentException("Invalid expression type: " + type);
    }

    boolean hasInputColumnIndex = ReadWriteIOUtils.readBool(byteBuffer);
    if (hasInputColumnIndex) {
      expression.inputColumnIndex = ReadWriteIOUtils.readInt(byteBuffer);
    }

    return expression;
  }

  protected abstract void serialize(ByteBuffer byteBuffer);

  protected abstract void serialize(DataOutputStream stream) throws IOException;

  /////////////////////////////////////////////////////////////////////////////////////////////////
  // iterator: level-order traversal iterator
  /////////////////////////////////////////////////////////////////////////////////////////////////

  /**
   * returns the DIRECT children expressions if it has any, otherwise an EMPTY list will be returned
   */
  public abstract List<Expression> getExpressions();

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
}
