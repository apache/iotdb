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

package org.apache.iotdb.calc.transformation.dag.column.binary;

import org.apache.iotdb.calc.i18n.CalcMessages;
import org.apache.iotdb.calc.transformation.dag.column.ColumnTransformer;
import org.apache.iotdb.calc.transformation.dag.util.CommonTransformUtils;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.read.common.block.column.RunLengthEncodedColumn;
import org.apache.tsfile.read.common.type.Type;
import org.apache.tsfile.read.common.type.TypeEnum;

import static org.apache.iotdb.calc.plan.relational.metadata.CommonMetadataUtils.isBlobType;
import static org.apache.iotdb.calc.plan.relational.metadata.CommonMetadataUtils.isBool;
import static org.apache.iotdb.calc.plan.relational.metadata.CommonMetadataUtils.isCharType;

public abstract class CompareBinaryColumnTransformer extends BinaryColumnTransformer {

  protected CompareBinaryColumnTransformer(
      Type returnType, ColumnTransformer leftTransformer, ColumnTransformer rightTransformer) {
    super(returnType, leftTransformer, rightTransformer);
  }

  @Override
  protected void doTransform(
      Column leftColumn, Column rightColumn, ColumnBuilder builder, int positionCount) {
    // if either column is all null, append nullCount. For now, a RunLengthEncodeColumn with
    // mayHaveNull == true is all null
    if ((leftColumn.mayHaveNull() && leftColumn instanceof RunLengthEncodedColumn)
        || (rightColumn.mayHaveNull() && rightColumn instanceof RunLengthEncodedColumn)) {
      builder.appendNull(positionCount);
      return;
    }
    for (int i = 0; i < positionCount; i++) {
      if (!leftColumn.isNull(i) && !rightColumn.isNull(i)) {
        evaluateAndTransform(leftColumn, rightColumn, i, builder);
      } else {
        builder.appendNull();
      }
    }
  }

  @Override
  protected void doTransform(
      Column leftColumn,
      Column rightColumn,
      ColumnBuilder builder,
      int positionCount,
      boolean[] selection) {
    // if either column is all null, append nullCount. For now, a RunLengthEncodeColumn with
    // mayHaveNull == true is all null
    if ((leftColumn.mayHaveNull() && leftColumn instanceof RunLengthEncodedColumn)
        || (rightColumn.mayHaveNull() && rightColumn instanceof RunLengthEncodedColumn)) {
      builder.appendNull(positionCount);
      return;
    }
    for (int i = 0; i < positionCount; i++) {
      if (selection[i] && !leftColumn.isNull(i) && !rightColumn.isNull(i)) {
        evaluateAndTransform(leftColumn, rightColumn, i, builder);
      } else {
        builder.appendNull();
      }
    }
  }

  private void evaluateAndTransform(
      Column leftColumn, Column rightColumn, int index, ColumnBuilder builder) {
    boolean flag = false;
    // compare binary type
    if (isCharType(leftTransformer.getType()) || isBlobType(leftTransformer.getType())) {
      flag =
          transform(
              CommonTransformUtils.compare(
                  leftTransformer.getType().getBinary(leftColumn, index),
                  rightTransformer.getType().getBinary(rightColumn, index)));
    } else if (isBool(leftTransformer.getType())) {
      flag =
          transform(
              Boolean.compare(
                  leftTransformer.getType().getBoolean(leftColumn, index),
                  rightTransformer.getType().getBoolean(rightColumn, index)));
    } else if (needsExactComparison(leftTransformer.getType(), rightTransformer.getType())) {
      // A double cannot hold every INT64/TIMESTAMP value (Long.MAX_VALUE rounds up to 2^63), so
      // these are compared without converting the integer side to double.
      final Type leftType = leftTransformer.getType();
      final Type rightType = rightTransformer.getType();
      if (isFloatingPoint(rightType)) {
        final double right = rightType.getDouble(rightColumn, index);
        if (!Double.isNaN(right)) {
          flag = transform(compareLongToDouble(getLong(leftType, leftColumn, index), right));
        }
      } else if (isFloatingPoint(leftType)) {
        final double left = leftType.getDouble(leftColumn, index);
        if (!Double.isNaN(left)) {
          flag = transform(-compareLongToDouble(getLong(rightType, rightColumn, index), left));
        }
      } else {
        flag =
            transform(
                Long.compare(
                    getLong(leftType, leftColumn, index), getLong(rightType, rightColumn, index)));
      }
    } else {
      final double left = leftTransformer.getType().getDouble(leftColumn, index);
      final double right = rightTransformer.getType().getDouble(rightColumn, index);
      if (!Double.isNaN(left) && !Double.isNaN(right)) {
        flag = transform(Double.compare(left, right));
      }
    }
    returnType.writeBoolean(builder, flag);
  }

  /** True when one side is INT64/TIMESTAMP and the other is an integer or floating-point type. */
  private static boolean needsExactComparison(Type left, Type right) {
    return (isWideInteger(left) && (isInteger(right) || isFloatingPoint(right)))
        || (isWideInteger(right) && (isInteger(left) || isFloatingPoint(left)));
  }

  private static boolean isWideInteger(Type type) {
    return type.getTypeEnum() == TypeEnum.INT64 || type.getTypeEnum() == TypeEnum.TIMESTAMP;
  }

  private static boolean isInteger(Type type) {
    return type.getTypeEnum() == TypeEnum.INT32 || isWideInteger(type);
  }

  private static boolean isFloatingPoint(Type type) {
    return type.getTypeEnum() == TypeEnum.FLOAT || type.getTypeEnum() == TypeEnum.DOUBLE;
  }

  private static long getLong(Type type, Column column, int index) {
    return type.getTypeEnum() == TypeEnum.INT32
        ? type.getInt(column, index)
        : type.getLong(column, index);
  }

  /** Compares a long with a non-NaN double exactly. */
  private static int compareLongToDouble(long value, double other) {
    if (other >= 0x1p63) {
      return -1;
    }
    if (other < -0x1p63) {
      return 1;
    }
    // |other| < 2^63, so the cast is exact truncation toward zero
    final long truncated = (long) other;
    if (value != truncated) {
      return Long.compare(value, truncated);
    }
    // value equals the integral part of other, so the fractional part decides
    final double fraction = other - truncated;
    return fraction > 0 ? -1 : (fraction < 0 ? 1 : 0);
  }

  @Override
  protected void checkType() {
    // Boolean type can only be compared by == or !=
    if (leftTransformer.typeNotEquals(TypeEnum.BOOLEAN)
        && rightTransformer.typeNotEquals(TypeEnum.BOOLEAN)) {
      return;
    }
    throw new UnsupportedOperationException(CalcMessages.UNSUPPORTED_TYPE_BINARY);
  }

  /**
   * Transform int value of flag to corresponding boolean value.
   *
   * @param flag input int value
   * @return result boolean value
   */
  protected abstract boolean transform(int flag);
}
