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

package org.apache.iotdb.db.queryengine.transformation.dag.util;

import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.queryengine.plan.expression.leaf.ConstantOperand;
import org.apache.iotdb.db.queryengine.transformation.datastructure.util.ValueRecorder;
import org.apache.iotdb.db.utils.CommonUtils;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.column.BinaryColumn;
import org.apache.tsfile.read.common.block.column.BooleanColumn;
import org.apache.tsfile.read.common.block.column.DoubleColumn;
import org.apache.tsfile.read.common.block.column.FloatColumn;
import org.apache.tsfile.read.common.block.column.IntColumn;
import org.apache.tsfile.read.common.block.column.LongColumn;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.write.UnSupportedDataTypeException;

import java.util.Objects;
import java.util.Optional;

public class TransformUtils {

  private TransformUtils() {
    throw new IllegalStateException("TransformUtils should not be instantiated.");
  }

  public static int compare(Binary first, Binary second) {
    if (Objects.requireNonNull(first) == Objects.requireNonNull(second)) {
      return 0;
    }

    return first.compareTo(second);
  }

  public static Column transformConstantOperandToColumn(ConstantOperand constantOperand) {
    Objects.requireNonNull(constantOperand);

    try {
      Object value =
          CommonUtils.parseValue(constantOperand.getDataType(), constantOperand.getValueString());
      if (value == null) {
        throw new UnsupportedOperationException(
            "Invalid constant operand: " + constantOperand.getExpressionString());
      }

      switch (constantOperand.getDataType()) {
        case INT32:
          return new IntColumn(1, Optional.empty(), new int[] {(int) value});
        case INT64:
          return new LongColumn(1, Optional.empty(), new long[] {(long) value});
        case FLOAT:
          return new FloatColumn(1, Optional.empty(), new float[] {(float) value});
        case DOUBLE:
          return new DoubleColumn(1, Optional.empty(), new double[] {(double) value});
        case TEXT:
          return new BinaryColumn(1, Optional.empty(), new Binary[] {(Binary) value});
        case BOOLEAN:
          return new BooleanColumn(1, Optional.empty(), new boolean[] {(boolean) value});
        case STRING:
        case BLOB:
        case DATE:
        case TIMESTAMP:
        default:
          throw new UnSupportedDataTypeException(
              "Unsupported type: " + constantOperand.getDataType());
      }
    } catch (QueryProcessException e) {
      throw new UnsupportedOperationException(e);
    }
  }

  public static boolean splitWindowForStateWindow(
      TSDataType dataType, ValueRecorder valueRecorder, double delta, Column values, int index) {
    boolean res;
    switch (dataType) {
      case INT32:
        if (!valueRecorder.hasRecorded()) {
          valueRecorder.recordInt(values.getInt(index - 1));
          valueRecorder.setRecorded(true);
        }
        res = Math.abs(values.getInt(index) - valueRecorder.getInt()) > delta;
        if (res) {
          valueRecorder.recordInt(values.getInt(index));
        }
        break;
      case INT64:
        if (!valueRecorder.hasRecorded()) {
          valueRecorder.recordLong(values.getLong(index - 1));
          valueRecorder.setRecorded(true);
        }
        res = Math.abs(values.getLong(index) - valueRecorder.getLong()) > delta;
        if (res) {
          valueRecorder.recordLong(values.getLong(index));
        }
        break;
      case FLOAT:
        if (!valueRecorder.hasRecorded()) {
          valueRecorder.recordFloat(values.getFloat(index - 1));
          valueRecorder.setRecorded(true);
        }
        res = Math.abs(values.getFloat(index) - valueRecorder.getFloat()) > delta;
        if (res) {
          valueRecorder.recordFloat(values.getFloat(index));
        }
        break;
      case DOUBLE:
        if (!valueRecorder.hasRecorded()) {
          valueRecorder.recordDouble(values.getDouble(index - 1));
          valueRecorder.setRecorded(true);
        }
        res = Math.abs(values.getDouble(index) - valueRecorder.getDouble()) > delta;
        if (res) {
          valueRecorder.recordDouble(values.getDouble(index));
        }
        break;
      case BOOLEAN:
        if (!valueRecorder.hasRecorded()) {
          valueRecorder.recordBoolean(values.getBoolean(index - 1));
          valueRecorder.setRecorded(true);
        }
        res = values.getBoolean(index) != valueRecorder.getBoolean();
        if (res) {
          valueRecorder.recordBoolean(values.getBoolean(index));
        }
        break;
      case TEXT:
        if (!valueRecorder.hasRecorded()) {
          Binary binary = values.getBinary(index - 1);
          valueRecorder.recordString(binary.toString());
          valueRecorder.setRecorded(true);
        }
        String str = values.getBinary(index).toString();
        res = !str.equals(valueRecorder.getString());
        if (res) {
          valueRecorder.recordString(str);
        }
        break;
      case TIMESTAMP:
      case DATE:
      case BLOB:
      case STRING:
      default:
        throw new UnsupportedOperationException(
            "The data type of the state window strategy is not valid.");
    }
    return res;
  }
}
