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

package org.apache.iotdb.db.mpp.transformation.dag.input;

import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.mpp.plan.expression.leaf.ConstantOperand;
import org.apache.iotdb.db.mpp.transformation.api.LayerPointReader;
import org.apache.iotdb.db.mpp.transformation.api.YieldableState;
import org.apache.iotdb.db.utils.CommonUtils;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.Binary;

import org.apache.commons.lang3.Validate;

import java.io.IOException;

/** LayerPointReader for constants. */
public class ConstantInputReader implements LayerPointReader {

  private final ConstantOperand expression;

  protected int cachedInt;
  protected long cachedLong;
  protected float cachedFloat;
  protected double cachedDouble;
  protected boolean cachedBoolean;
  protected Binary cachedBinary;

  public ConstantInputReader(ConstantOperand expression) throws QueryProcessException {
    this.expression = Validate.notNull(expression);

    Object value = CommonUtils.parseValue(expression.getDataType(), expression.getValueString());
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
  }

  @Override
  public boolean isConstantPointReader() {
    return true;
  }

  @Override
  public YieldableState yield() {
    return YieldableState.YIELDABLE;
  }

  @Override
  public boolean next() {
    return true;
  }

  @Override
  public void readyForNext() {
    // Do nothing
  }

  @Override
  public TSDataType getDataType() {
    return expression.getDataType();
  }

  @Override
  public long currentTime() throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public int currentInt() throws IOException {
    return cachedInt;
  }

  @Override
  public long currentLong() throws IOException {
    return cachedLong;
  }

  @Override
  public float currentFloat() throws IOException {
    return cachedFloat;
  }

  @Override
  public double currentDouble() throws IOException {
    return cachedDouble;
  }

  @Override
  public boolean currentBoolean() throws IOException {
    return cachedBoolean;
  }

  @Override
  public Binary currentBinary() throws IOException {
    return cachedBinary;
  }

  @Override
  public boolean isCurrentNull() {
    return false;
  }
}
