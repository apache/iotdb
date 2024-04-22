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

import org.apache.iotdb.db.pipe.processor.aggregate.operator.intermediateresult.IntermediateResultOperator;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;

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
  public void configureSystemParameters(Map<String, String> systemParams) {
    // Do nothing
  }

  @Override
  public boolean initAndGetIsSupport(boolean initialInput, long initialTimestamp) {
    return false;
  }

  @Override
  public boolean initAndGetIsSupport(int initialInput, long initialTimestamp) {
    outPutDataType = TSDataType.INT32;
    return true;
  }

  @Override
  public boolean initAndGetIsSupport(long initialInput, long initialTimestamp) {
    outPutDataType = TSDataType.INT64;
    return true;
  }

  @Override
  public boolean initAndGetIsSupport(float initialInput, long initialTimestamp) {
    outPutDataType = TSDataType.FLOAT;
    return true;
  }

  @Override
  public boolean initAndGetIsSupport(double initialInput, long initialTimestamp) {
    outPutDataType = TSDataType.DOUBLE;
    return true;
  }

  @Override
  public boolean initAndGetIsSupport(String initialInput, long initialTimestamp) {
    return false;
  }

  @Override
  public void updateValue(boolean input, long timestamp) {
    throw new UnsupportedOperationException(
        "AbstractSameTypeNumericOperator does not support boolean input");
  }

  @Override
  public void updateValue(String input, long timestamp) {
    throw new UnsupportedOperationException(
        "AbstractSameTypeNumericOperator does not support string input");
  }

  @Override
  public Pair<TSDataType, Object> getResult() {
    switch (outPutDataType) {
      case INT32:
        return new Pair<>(TSDataType.INT32, intValue);
      case INT64:
        return new Pair<>(TSDataType.INT64, longValue);
      case FLOAT:
        return new Pair<>(TSDataType.FLOAT, floatValue);
      case DOUBLE:
        return new Pair<>(TSDataType.DOUBLE, doubleValue);
      default:
        return null;
    }
  }

  @Override
  public void serialize(DataOutputStream outputStream) throws IOException {
    outPutDataType.serializeTo(outputStream);
    switch (outPutDataType) {
      case INT32:
        ReadWriteIOUtils.write(intValue, outputStream);
        break;
      case INT64:
        ReadWriteIOUtils.write(longValue, outputStream);
        break;
      case FLOAT:
        ReadWriteIOUtils.write(floatValue, outputStream);
        break;
      case DOUBLE:
        ReadWriteIOUtils.write(doubleValue, outputStream);
        break;
      default:
        throw new IOException(String.format("Unsupported output datatype %s", outPutDataType));
    }
  }

  @Override
  public void deserialize(ByteBuffer byteBuffer) throws IOException {
    outPutDataType = TSDataType.deserializeFrom(byteBuffer);
    switch (outPutDataType) {
      case INT32:
        intValue = ReadWriteIOUtils.readInt(byteBuffer);
        break;
      case INT64:
        longValue = ReadWriteIOUtils.readLong(byteBuffer);
        break;
      case FLOAT:
        floatValue = ReadWriteIOUtils.readFloat(byteBuffer);
        break;
      case DOUBLE:
        doubleValue = ReadWriteIOUtils.readDouble(byteBuffer);
        break;
      default:
        throw new IOException(String.format("Unsupported output datatype %s", outPutDataType));
    }
  }
}
