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
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.block.column.*;
import org.apache.iotdb.tsfile.utils.Binary;

import org.apache.commons.lang3.Validate;

import java.io.IOException;
import java.util.Arrays;
import java.util.Optional;

public class ConstantInputReader implements LayerReader {
  private int count = TSFileDescriptor.getInstance().getConfig().getMaxTsBlockLineNumber();
  private final ConstantOperand expression;
  private TSDataType dataType;

  private Column cachedColumn;

  public ConstantInputReader(ConstantOperand expression) throws QueryProcessException {
    this.expression = Validate.notNull(expression);

    Object value = CommonUtils.parseValue(expression.getDataType(), expression.getValueString());
    if (value == null) {
      throw new QueryProcessException(
          "Invalid constant operand: " + expression.getExpressionString());
    }

    dataType = expression.getDataType();
    switch (dataType) {
      case INT32:
        int[] intArray = new int[count];
        Arrays.fill(intArray, (int) value);
        cachedColumn = new IntColumn(count, Optional.empty(), intArray);
        break;
      case INT64:
        long[] longArray = new long[count];
        Arrays.fill(longArray, (long) value);
        cachedColumn = new LongColumn(count, Optional.empty(), longArray);
        break;
      case FLOAT:
        float[] floatArray = new float[count];
        Arrays.fill(floatArray, (float) value);
        cachedColumn = new FloatColumn(count, Optional.empty(), floatArray);
        break;
      case DOUBLE:
        double[] doubleArray = new double[count];
        Arrays.fill(doubleArray, (double) value);
        cachedColumn = new DoubleColumn(count, Optional.empty(), doubleArray);
        break;
      case TEXT:
        Binary[] binaryArray = new Binary[count];
        Arrays.fill(binaryArray, value);
        cachedColumn = new BinaryColumn(count, Optional.empty(), binaryArray);
        break;
      case BOOLEAN:
        boolean[] booleanArray = new boolean[count];
        Arrays.fill(booleanArray, (boolean) value);
        cachedColumn = new BooleanColumn(count, Optional.empty(), booleanArray);
        break;
      default:
        throw new QueryProcessException("Unsupported type: " + expression.getDataType());
    }
  }

  @Override
  public boolean isConstantPointReader() {
    return true;
  }

  @Override
  public void consumed(int consumed) {
    // Do nothing
  }

  @Override
  public void consumedAll() {
    // Do nothing
  }

  @Override
  public Column[] current() throws IOException {
    return new Column[] {cachedColumn};
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
