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

package org.apache.iotdb.db.pipe.processor.aggregate.operator.intermediateresult.sametype.numeric;

import org.apache.iotdb.db.i18n.DataNodePipeMessages;
import org.apache.iotdb.db.pipe.processor.aggregate.operator.intermediateresult.IntermediateResultOperator;
import org.apache.iotdb.db.utils.TypeServices.Pipe.SameTypeNumericOperatorStrategy;
import org.apache.iotdb.pipe.api.type.Binary;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.type.Type;
import org.apache.tsfile.utils.Pair;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.util.Map;

import static org.apache.iotdb.db.utils.TypeServices.Pipe.SAME_TYPE_NUMERIC_OPERATOR_STRATEGY_SERVICE;

/**
 * {@link AbstractSameTypeNumericOperator} is the parent class of all the operators where there
 * value type is the same as the input data type, the input data type is a numeric type, and the
 * initial value equals to the first input value.
 */
public abstract class AbstractSameTypeNumericOperator implements IntermediateResultOperator {
  protected TSDataType outPutDataType;
  protected int intValue;
  protected long longValue;
  protected float floatValue;
  protected double doubleValue;

  @Override
  public void configureSystemParameters(final Map<String, String> systemParams) {
    // Do nothing
  }

  @Override
  public boolean initAndGetIsSupport(final boolean initialInput, final long initialTimestamp) {
    return false;
  }

  @Override
  public boolean initAndGetIsSupport(final int initialInput, final long initialTimestamp) {
    outPutDataType = TSDataType.INT32;
    return true;
  }

  @Override
  public boolean initAndGetIsSupport(final LocalDate initialInput, final long initialTimestamp) {
    return false;
  }

  @Override
  public boolean initAndGetIsSupport(final long initialInput, final long initialTimestamp) {
    outPutDataType = TSDataType.INT64;
    return true;
  }

  @Override
  public boolean initAndGetIsSupport(final float initialInput, final long initialTimestamp) {
    outPutDataType = TSDataType.FLOAT;
    return true;
  }

  @Override
  public boolean initAndGetIsSupport(final double initialInput, final long initialTimestamp) {
    outPutDataType = TSDataType.DOUBLE;
    return true;
  }

  @Override
  public boolean initAndGetIsSupport(final String initialInput, final long initialTimestamp) {
    return false;
  }

  @Override
  public boolean initAndGetIsSupport(final Binary initialInput, final long initialTimestamp) {
    return false;
  }

  @Override
  public void updateValue(final boolean input, final long timestamp) {
    throw new UnsupportedOperationException(
        DataNodePipeMessages.ABSTRACTSAMETYPENUMERICOPERATOR_DOES_NOT_SUPPORT_BOOLEAN_INPUT);
  }

  @Override
  public void updateValue(final LocalDate input, final long timestamp) {
    throw new UnsupportedOperationException(
        DataNodePipeMessages.ABSTRACTSAMETYPENUMERICOPERATOR_DOES_NOT_SUPPORT_DATE_INPUT);
  }

  @Override
  public void updateValue(final String input, final long timestamp) {
    throw new UnsupportedOperationException(
        DataNodePipeMessages.ABSTRACTSAMETYPENUMERICOPERATOR_DOES_NOT_SUPPORT_STRING_INPUT);
  }

  @Override
  public void updateValue(final Binary input, final long timestamp) {
    throw new UnsupportedOperationException(
        DataNodePipeMessages.ABSTRACTSAMETYPENUMERICOPERATOR_DOES_NOT_SUPPORT_BINARY_INPUT);
  }

  @Override
  public Pair<TSDataType, Object> getResult() {
    try {
      return getStrategy().getResult(this);
    } catch (final UnsupportedOperationException ignored) {
      return null;
    }
  }

  @Override
  public void serialize(final DataOutputStream outputStream) throws IOException {
    outPutDataType.serializeTo(outputStream);
    try {
      getStrategy().serialize(this, outputStream);
    } catch (final UnsupportedOperationException ignored) {
      throw unsupportedOutputDataTypeException();
    }
  }

  @Override
  public void deserialize(final ByteBuffer byteBuffer) throws IOException {
    outPutDataType = TSDataType.deserializeFrom(byteBuffer);
    try {
      getStrategy().deserialize(this, byteBuffer);
    } catch (final UnsupportedOperationException ignored) {
      throw unsupportedOutputDataTypeException();
    }
  }

  private SameTypeNumericOperatorStrategy getStrategy() {
    return SAME_TYPE_NUMERIC_OPERATOR_STRATEGY_SERVICE.call(Type.fromTsDataType(outPutDataType));
  }

  private IOException unsupportedOutputDataTypeException() {
    return new IOException(
        String.format(DataNodePipeMessages.UNSUPPORTED_OUTPUT_DATATYPE_FMT, outPutDataType));
  }

  public int getIntValue() {
    return intValue;
  }

  public void setIntValue(final int intValue) {
    this.intValue = intValue;
  }

  public long getLongValue() {
    return longValue;
  }

  public void setLongValue(final long longValue) {
    this.longValue = longValue;
  }

  public float getFloatValue() {
    return floatValue;
  }

  public void setFloatValue(final float floatValue) {
    this.floatValue = floatValue;
  }

  public double getDoubleValue() {
    return doubleValue;
  }

  public void setDoubleValue(final double doubleValue) {
    this.doubleValue = doubleValue;
  }
}
