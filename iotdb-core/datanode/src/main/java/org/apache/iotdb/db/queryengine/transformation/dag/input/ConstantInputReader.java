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

package org.apache.iotdb.db.queryengine.transformation.dag.input;

import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.queryengine.plan.expression.leaf.ConstantOperand;
import org.apache.iotdb.db.queryengine.transformation.api.LayerReader;
import org.apache.iotdb.db.queryengine.transformation.api.YieldableState;
import org.apache.iotdb.db.utils.CommonUtils;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.column.BinaryColumn;
import org.apache.tsfile.read.common.block.column.BooleanColumn;
import org.apache.tsfile.read.common.block.column.DoubleColumn;
import org.apache.tsfile.read.common.block.column.FloatColumn;
import org.apache.tsfile.read.common.block.column.IntColumn;
import org.apache.tsfile.read.common.block.column.LongColumn;
import org.apache.tsfile.read.common.block.column.RunLengthEncodedColumn;
import org.apache.tsfile.utils.Binary;

import java.io.IOException;
import java.util.Optional;

public class ConstantInputReader implements LayerReader {
  private final TSDataType dataType;

  private final Column[] cachedColumns;

  public ConstantInputReader(ConstantOperand expression) throws QueryProcessException {
    if (expression == null) {
      throw new QueryProcessException("The expression cannot be null");
    }

    Object value = CommonUtils.parseValue(expression.getDataType(), expression.getValueString());
    if (value == null) {
      throw new QueryProcessException(
          "Invalid constant operand: " + expression.getExpressionString());
    }

    // Use RLEColumn to mimic column filled with same values
    dataType = expression.getDataType();
    cachedColumns = new Column[1];
    int count = TSFileDescriptor.getInstance().getConfig().getMaxTsBlockLineNumber();
    switch (dataType) {
      case INT32:
        int[] intArray = {(int) value};
        Column intColumn = new IntColumn(1, Optional.empty(), intArray);
        cachedColumns[0] = new RunLengthEncodedColumn(intColumn, count);
        break;
      case INT64:
        long[] longArray = {(long) value};
        Column longColumn = new LongColumn(1, Optional.empty(), longArray);
        cachedColumns[0] = new RunLengthEncodedColumn(longColumn, count);
        break;
      case FLOAT:
        float[] floatArray = {(float) value};
        Column floatColumn = new FloatColumn(1, Optional.empty(), floatArray);
        cachedColumns[0] = new RunLengthEncodedColumn(floatColumn, count);
        break;
      case DOUBLE:
        double[] doubleArray = {(double) value};
        Column doubleColumn = new DoubleColumn(1, Optional.empty(), doubleArray);
        cachedColumns[0] = new RunLengthEncodedColumn(doubleColumn, count);
        break;
      case TEXT:
        Binary[] binaryArray = {(Binary) value};
        Column binaryColumn = new BinaryColumn(1, Optional.empty(), binaryArray);
        cachedColumns[0] = new RunLengthEncodedColumn(binaryColumn, count);
        break;
      case BOOLEAN:
        boolean[] booleanArray = {(boolean) value};
        Column booleanColumn = new BooleanColumn(1, Optional.empty(), booleanArray);
        cachedColumns[0] = new RunLengthEncodedColumn(booleanColumn, count);
        break;
      case BLOB:
      case STRING:
      case TIMESTAMP:
      case DATE:
      default:
        throw new QueryProcessException("Unsupported type: " + expression.getDataType());
    }
  }

  @Override
  public boolean isConstantPointReader() {
    return true;
  }

  @Override
  public void consumedAll() {
    // Do nothing
  }

  @Override
  public Column[] current() throws IOException {
    return cachedColumns;
  }

  @Override
  public YieldableState yield() {
    return YieldableState.YIELDABLE;
  }

  @Override
  public TSDataType[] getDataTypes() {
    return new TSDataType[] {dataType};
  }
}
