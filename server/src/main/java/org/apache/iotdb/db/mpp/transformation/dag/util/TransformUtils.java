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

package org.apache.iotdb.db.mpp.transformation.dag.util;

import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.mpp.plan.expression.leaf.ConstantOperand;
import org.apache.iotdb.db.utils.CommonUtils;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.read.common.block.column.BinaryColumn;
import org.apache.iotdb.tsfile.read.common.block.column.BooleanColumn;
import org.apache.iotdb.tsfile.read.common.block.column.Column;
import org.apache.iotdb.tsfile.read.common.block.column.DoubleColumn;
import org.apache.iotdb.tsfile.read.common.block.column.FloatColumn;
import org.apache.iotdb.tsfile.read.common.block.column.IntColumn;
import org.apache.iotdb.tsfile.read.common.block.column.LongColumn;
import org.apache.iotdb.tsfile.utils.Binary;

import org.apache.commons.lang3.Validate;

import java.util.Objects;
import java.util.Optional;

public class TransformUtils {

  public static int compare(CharSequence cs1, CharSequence cs2) {
    if (Objects.requireNonNull(cs1) == Objects.requireNonNull(cs2)) {
      return 0;
    }

    if (cs1.getClass() == cs2.getClass() && cs1 instanceof Comparable) {
      return ((Comparable<Object>) cs1).compareTo(cs2);
    }

    for (int i = 0, len = Math.min(cs1.length(), cs2.length()); i < len; i++) {
      char a = cs1.charAt(i);
      char b = cs2.charAt(i);
      if (a != b) {
        return a - b;
      }
    }

    return cs1.length() - cs2.length();
  }

  public static Column transformConstantOperandToColumn(ConstantOperand constantOperand) {
    Validate.notNull(constantOperand);

    try {
      Object value =
          CommonUtils.parseValue(
              constantOperand.getDataType(), constantOperand.getExpressionString());
      if (value == null) {
        throw new UnsupportedOperationException(
            "Invalid constant operand: " + constantOperand.getExpressionString());
      }

      switch (constantOperand.getDataType()) {
        case INT32:
          return new IntColumn(1, Optional.of(new boolean[] {false}), new int[] {(int) value});
        case INT64:
          return new LongColumn(1, Optional.of(new boolean[] {false}), new long[] {(long) value});
        case FLOAT:
          return new FloatColumn(
              1, Optional.of(new boolean[] {false}), new float[] {(float) value});
        case DOUBLE:
          return new DoubleColumn(
              1, Optional.of(new boolean[] {false}), new double[] {(double) value});
        case TEXT:
          return new BinaryColumn(
              1, Optional.of(new boolean[] {false}), new Binary[] {(Binary) value});
        case BOOLEAN:
          return new BooleanColumn(
              1, Optional.of(new boolean[] {false}), new boolean[] {(boolean) value});
        default:
          throw new UnSupportedDataTypeException(
              "Unsupported type: " + constantOperand.getDataType());
      }
    } catch (QueryProcessException e) {
      throw new UnsupportedOperationException(e);
    }
  }
}
