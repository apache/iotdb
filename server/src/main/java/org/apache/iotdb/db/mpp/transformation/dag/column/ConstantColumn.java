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

package org.apache.iotdb.db.mpp.transformation.dag.column;

import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.mpp.plan.expression.Expression;
import org.apache.iotdb.db.mpp.plan.expression.leaf.ConstantOperand;
import org.apache.iotdb.db.utils.CommonUtils;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.block.column.Column;
import org.apache.iotdb.tsfile.read.common.block.column.ColumnEncoding;
import org.apache.iotdb.tsfile.utils.Binary;

import org.apache.commons.lang3.Validate;

/** used for calculation in constant column transformer */
public class ConstantColumn implements Column {
  private TSDataType tsDataType;

  private Expression expression;

  protected int cachedInt;
  protected long cachedLong;
  protected float cachedFloat;
  protected double cachedDouble;
  protected boolean cachedBoolean;
  protected Binary cachedBinary;

  public ConstantColumn(ConstantOperand expression) {
    this.expression = Validate.notNull(expression);

    try {
      Object value =
          CommonUtils.parseValue(expression.getDataType(), expression.getExpressionString());
      if (value == null) {
        throw new QueryProcessException(
            "Invalid constant operand: " + expression.getExpressionString());
      }

      switch (expression.getDataType()) {
        case INT32:
          cachedInt = (int) value;
          break;
        case INT64:
          cachedLong = (long) value;
          break;
        case FLOAT:
          cachedFloat = (float) value;
          break;
        case DOUBLE:
          cachedDouble = (double) value;
          break;
        case TEXT:
          cachedBinary = (Binary) value;
          break;
        case BOOLEAN:
          cachedBoolean = (boolean) value;
          break;
        default:
          throw new QueryProcessException("Unsupported type: " + expression.getDataType());
      }
    } catch (QueryProcessException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public boolean getBoolean(int position) {
    return cachedBoolean;
  }

  @Override
  public int getInt(int position) {
    return cachedInt;
  }

  @Override
  public long getLong(int position) {
    return cachedLong;
  }

  @Override
  public float getFloat(int position) {
    return cachedFloat;
  }

  @Override
  public double getDouble(int position) {
    return cachedDouble;
  }

  @Override
  public Binary getBinary(int position) {
    return cachedBinary;
  }

  @Override
  public TSDataType getDataType() {
    return tsDataType;
  }

  @Override
  public ColumnEncoding getEncoding() {
    throw new UnsupportedOperationException(getClass().getName());
  }

  @Override
  public boolean mayHaveNull() {
    throw new UnsupportedOperationException(getClass().getName());
  }

  @Override
  public boolean isNull(int position) {
    return false;
  }

  // we return -1 here to represent that it's ConstantColumn
  @Override
  public int getPositionCount() {
    return -1;
  }

  @Override
  public long getRetainedSizeInBytes() {
    throw new UnsupportedOperationException(getClass().getName());
  }

  @Override
  public Column getRegion(int positionOffset, int length) {
    throw new UnsupportedOperationException(getClass().getName());
  }

  @Override
  public Column subColumn(int fromIndex) {
    throw new UnsupportedOperationException(getClass().getName());
  }

  @Override
  public void reverse() {
    throw new UnsupportedOperationException(getClass().getName());
  }
}
