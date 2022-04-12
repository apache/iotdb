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
import org.apache.iotdb.db.query.expression.unary.TimeSeriesOperand;

import java.nio.ByteBuffer;

public enum ExpressionType {
  Addition((short) 0),
  Division((short) 1),
  EqualTo((short) 2),
  Greater_Equal((short) 3),
  Greater_Than((short) 4),
  Less_Equal((short) 5),
  Less_Than((short) 6),
  Logic_And((short) 7),
  Logic_Or((short) 8),
  Modulo((short) 9),
  Multiplication((short) 10),
  Non_Equal((short) 11),
  Subtraction((short) 12),
  Function((short) 13),
  Logic_Not((short) 14),
  Negation((short) 15),
  TimeSeries((short) 16),
  Constant((short) 17);

  private final short expressionType;

  ExpressionType(short expressionType) {
    this.expressionType = expressionType;
  }

  public void serialize(ByteBuffer buffer) {
    buffer.putShort(expressionType);
  }

  public static Expression deserialize(ByteBuffer byteBuffer) {
    short type = byteBuffer.getShort();
    switch (type) {
      case 0:
        return AdditionExpression.deserialize(byteBuffer);
      case 1:
        return DivisionExpression.deserialize(byteBuffer);
      case 2:
        return EqualToExpression.deserialize(byteBuffer);
      case 3:
        return GreaterEqualExpression.deserialize(byteBuffer);
      case 4:
        return GreaterThanExpression.deserialize(byteBuffer);
      case 5:
        return LessEqualExpression.deserialize(byteBuffer);
      case 6:
        return LessThanExpression.deserialize(byteBuffer);
      case 7:
        return LogicAndExpression.deserialize(byteBuffer);
      case 8:
        return LogicOrExpression.deserialize(byteBuffer);
      case 9:
        return ModuloExpression.deserialize(byteBuffer);
      case 10:
        return MultiplicationExpression.deserialize(byteBuffer);
      case 11:
        return NonEqualExpression.deserialize(byteBuffer);
      case 12:
        return SubtractionExpression.deserialize(byteBuffer);
      case 13:
        return FunctionExpression.deserialize(byteBuffer);
      case 14:
        return LogicNotExpression.deserialize(byteBuffer);
      case 15:
        return NegationExpression.deserialize(byteBuffer);
      case 16:
        return TimeSeriesOperand.deserialize(byteBuffer);
      case 17:
        return ConstantOperand.deserialize(byteBuffer);
      default:
        throw new IllegalArgumentException("Invalid expression type: " + type);
    }
  }
}
