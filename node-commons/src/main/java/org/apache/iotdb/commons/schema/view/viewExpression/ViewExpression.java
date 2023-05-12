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

package org.apache.iotdb.commons.schema.view.viewExpression;

import org.apache.iotdb.commons.schema.view.viewExpression.binary.arithmetic.AdditionViewExpression;
import org.apache.iotdb.commons.schema.view.viewExpression.binary.arithmetic.DivisionViewExpression;
import org.apache.iotdb.commons.schema.view.viewExpression.binary.arithmetic.ModuloViewExpression;
import org.apache.iotdb.commons.schema.view.viewExpression.binary.arithmetic.MultiplicationViewExpression;
import org.apache.iotdb.commons.schema.view.viewExpression.binary.arithmetic.SubtractionViewExpression;
import org.apache.iotdb.commons.schema.view.viewExpression.binary.compare.EqualToViewExpression;
import org.apache.iotdb.commons.schema.view.viewExpression.binary.compare.GreaterEqualViewExpression;
import org.apache.iotdb.commons.schema.view.viewExpression.binary.compare.GreaterThanViewExpression;
import org.apache.iotdb.commons.schema.view.viewExpression.binary.compare.LessEqualViewExpression;
import org.apache.iotdb.commons.schema.view.viewExpression.binary.compare.LessThanViewExpression;
import org.apache.iotdb.commons.schema.view.viewExpression.binary.compare.NonEqualViewExpression;
import org.apache.iotdb.commons.schema.view.viewExpression.binary.logic.LogicAndViewExpression;
import org.apache.iotdb.commons.schema.view.viewExpression.binary.logic.LogicOrViewExpression;
import org.apache.iotdb.commons.schema.view.viewExpression.leaf.ConstantViewOperand;
import org.apache.iotdb.commons.schema.view.viewExpression.leaf.NullViewOperand;
import org.apache.iotdb.commons.schema.view.viewExpression.leaf.TimeSeriesViewOperand;
import org.apache.iotdb.commons.schema.view.viewExpression.leaf.TimestampViewOperand;
import org.apache.iotdb.commons.schema.view.viewExpression.multi.FunctionViewExpression;
import org.apache.iotdb.commons.schema.view.viewExpression.ternary.BetweenViewExpression;
import org.apache.iotdb.commons.schema.view.viewExpression.unary.InViewExpression;
import org.apache.iotdb.commons.schema.view.viewExpression.unary.IsNullViewExpression;
import org.apache.iotdb.commons.schema.view.viewExpression.unary.LikeViewExpression;
import org.apache.iotdb.commons.schema.view.viewExpression.unary.LogicNotViewExpression;
import org.apache.iotdb.commons.schema.view.viewExpression.unary.NegationViewExpression;
import org.apache.iotdb.commons.schema.view.viewExpression.unary.RegularViewExpression;
import org.apache.iotdb.commons.schema.view.viewExpression.visitor.ViewExpressionVisitor;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.List;

/**
 * This is a class designed for views, such as logical views. The view expression is a simple sketch
 * of an expression. A view expression stores less information, that make it easier to store and
 * serialize.
 *
 * <p>A view expression has to be transformed into expression in same type before analyzing. Also,
 * an expression should be transformed into view expression before storage in views.
 */
public abstract class ViewExpression {

  public <R, C> R accept(ViewExpressionVisitor<R, C> visitor, C context) {
    return visitor.visitExpression(this, context);
  }

  public abstract ViewExpressionType getExpressionType();

  /** @return if this view expression is a leaf node, return true; else return false. */
  public final boolean isLeafOperand() {
    return isLeafOperandInternal();
  }

  protected abstract boolean isLeafOperandInternal();

  /**
   * @return return the DIRECT children view expressions if it has any, otherwise an EMPTY list will
   *     be returned
   */
  public abstract List<ViewExpression> getChildViewExpressions();

  /**
   * @return if this view expression could be a source for an ALIAS series (one kind of view),
   *     return true; else return false. Foe example, a SINGLE TimeSeriesOperand could be a source
   *     for an alias view.
   */
  public final boolean isSourceForAliasSeries() {
    if (this.getExpressionType().getExpressionTypeInShortEnum()
        == ViewExpressionType.TIMESERIES.getExpressionTypeInShortEnum()) {
      return true;
    }
    return false;
  }

  public String toString() {
    return this.toString(true);
  }

  public abstract String toString(boolean isRoot);

  // region methods for serializing and deserializing
  protected abstract void serialize(ByteBuffer byteBuffer);

  protected abstract void serialize(OutputStream stream) throws IOException;

  public static void serialize(ViewExpression expression, ByteBuffer byteBuffer) {
    ReadWriteIOUtils.write(
        expression.getExpressionType().getExpressionTypeInShortEnum(), byteBuffer);

    expression.serialize(byteBuffer);
  }

  public static void serialize(ViewExpression expression, OutputStream stream) throws IOException {
    ReadWriteIOUtils.write(expression.getExpressionType().getExpressionTypeInShortEnum(), stream);

    expression.serialize(stream);
  }

  public static ViewExpression deserialize(ByteBuffer byteBuffer) {
    short type = ReadWriteIOUtils.readShort(byteBuffer);

    ViewExpression expression;
    switch (type) {
      case -4:
        expression = new ConstantViewOperand(byteBuffer);
      case -3:
        expression = new TimestampViewOperand(byteBuffer);
        break;
      case -2:
        expression = new TimeSeriesViewOperand(byteBuffer);
        break;
      case -1:
        expression = new FunctionViewExpression(byteBuffer);
        break;

      case 0:
        expression = new NegationViewExpression(byteBuffer);
        break;
      case 1:
        expression = new LogicNotViewExpression(byteBuffer);
        break;

      case 2:
        expression = new MultiplicationViewExpression(byteBuffer);
        break;
      case 3:
        expression = new DivisionViewExpression(byteBuffer);
        break;
      case 4:
        expression = new ModuloViewExpression(byteBuffer);
        break;

      case 5:
        expression = new AdditionViewExpression(byteBuffer);
        break;
      case 6:
        expression = new SubtractionViewExpression(byteBuffer);
        break;

      case 7:
        expression = new EqualToViewExpression(byteBuffer);
        break;
      case 8:
        expression = new NonEqualViewExpression(byteBuffer);
        break;
      case 9:
        expression = new GreaterEqualViewExpression(byteBuffer);
        break;
      case 10:
        expression = new GreaterThanViewExpression(byteBuffer);
        break;
      case 11:
        expression = new LessEqualViewExpression(byteBuffer);
        break;
      case 12:
        expression = new LessThanViewExpression(byteBuffer);
        break;

      case 13:
        expression = new LikeViewExpression(byteBuffer);
        break;
      case 14:
        expression = new RegularViewExpression(byteBuffer);
        break;

      case 15:
        expression = new IsNullViewExpression(byteBuffer);
        break;

      case 16:
        expression = new BetweenViewExpression(byteBuffer);
        break;

      case 17:
        expression = new InViewExpression(byteBuffer);
        break;

      case 18:
        expression = new LogicAndViewExpression(byteBuffer);
        break;

      case 19:
        expression = new LogicOrViewExpression(byteBuffer);
        break;

      case 20:
        expression = new NullViewOperand();
        break;
      default:
        throw new IllegalArgumentException("Invalid viewExpression type: " + type);
    }
    return expression;
  }

  public static ViewExpression deserialize(InputStream inputStream) {
    try {
      short type = ReadWriteIOUtils.readShort(inputStream);

      ViewExpression expression;
      switch (type) {
        case -4:
          expression = new ConstantViewOperand(inputStream);
          break;
        case -3:
          expression = new TimestampViewOperand(inputStream);
          break;
        case -2:
          expression = new TimeSeriesViewOperand(inputStream);
          break;
        case -1:
          expression = new FunctionViewExpression(inputStream);
          break;

        case 0:
          expression = new NegationViewExpression(inputStream);
          break;
        case 1:
          expression = new LogicNotViewExpression(inputStream);
          break;

        case 2:
          expression = new MultiplicationViewExpression(inputStream);
          break;
        case 3:
          expression = new DivisionViewExpression(inputStream);
          break;
        case 4:
          expression = new ModuloViewExpression(inputStream);
          break;

        case 5:
          expression = new AdditionViewExpression(inputStream);
          break;
        case 6:
          expression = new SubtractionViewExpression(inputStream);
          break;

        case 7:
          expression = new EqualToViewExpression(inputStream);
          break;
        case 8:
          expression = new NonEqualViewExpression(inputStream);
          break;
        case 9:
          expression = new GreaterEqualViewExpression(inputStream);
          break;
        case 10:
          expression = new GreaterThanViewExpression(inputStream);
          break;
        case 11:
          expression = new LessEqualViewExpression(inputStream);
          break;
        case 12:
          expression = new LessThanViewExpression(inputStream);
          break;

        case 13:
          expression = new LikeViewExpression(inputStream);
          break;
        case 14:
          expression = new RegularViewExpression(inputStream);
          break;

        case 15:
          expression = new IsNullViewExpression(inputStream);
          break;

        case 16:
          expression = new BetweenViewExpression(inputStream);
          break;

        case 17:
          expression = new InViewExpression(inputStream);
          break;

        case 18:
          expression = new LogicAndViewExpression(inputStream);
          break;

        case 19:
          expression = new LogicOrViewExpression(inputStream);
          break;

        case 20:
          expression = new NullViewOperand();
          break;
        default:
          throw new IllegalArgumentException("Invalid viewExpression type: " + type);
      }
      return expression;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
  // end region
}
