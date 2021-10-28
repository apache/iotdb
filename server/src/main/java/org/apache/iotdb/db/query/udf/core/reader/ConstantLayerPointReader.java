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

package org.apache.iotdb.db.query.udf.core.reader;

import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.query.expression.unary.ConstantExpression;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.Binary;

import org.apache.commons.lang3.Validate;

import java.io.IOException;

public class ConstantLayerPointReader implements LayerPointReader {

  private final ConstantExpression expression;

  public ConstantLayerPointReader(ConstantExpression expression) {
    this.expression = Validate.notNull(expression);
  }

  @Override
  public boolean isConstantPointReader() {
    return true;
  }

  @Override
  public boolean next() throws QueryProcessException, IOException {
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
    if (TSDataType.INT32.equals(expression.getDataType())) {
      return (int) expression.getValue();
    }
    throw new ClassCastException(
        "Cannot cast " + expression.getDataType() + " to " + TSDataType.INT32);
  }

  @Override
  public long currentLong() throws IOException {
    if (TSDataType.INT64.equals(expression.getDataType())) {
      return (long) expression.getValue();
    }
    throw new ClassCastException(
        "Cannot cast " + expression.getDataType() + " to " + TSDataType.INT64);
  }

  @Override
  public float currentFloat() throws IOException {
    if (TSDataType.FLOAT.equals(expression.getDataType())) {
      return (float) expression.getValue();
    }
    throw new ClassCastException(
        "Cannot cast " + expression.getDataType() + " to " + TSDataType.FLOAT);
  }

  @Override
  public double currentDouble() throws IOException {
    if (TSDataType.DOUBLE.equals(expression.getDataType())) {
      return (double) expression.getValue();
    }
    throw new ClassCastException(
        "Cannot cast " + expression.getDataType() + " to " + TSDataType.DOUBLE);
  }

  @Override
  public boolean currentBoolean() throws IOException {
    if (TSDataType.BOOLEAN.equals(expression.getDataType())) {
      return (boolean) expression.getValue();
    }
    throw new ClassCastException(
        "Cannot cast " + expression.getDataType() + " to " + TSDataType.BOOLEAN);
  }

  @Override
  public Binary currentBinary() throws IOException {
    if (TSDataType.TEXT.equals(expression.getDataType())) {
      return new Binary((String) expression.getValue());
    }
    throw new ClassCastException(
        "Cannot cast " + expression.getDataType() + " to " + TSDataType.TEXT);
  }
}
