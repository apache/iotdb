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
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

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
  public Class<?> initAndGetReturnValueType(boolean initialInput, long initialTimestamp) {
    return null;
  }

  @Override
  public Class<?> initAndGetReturnValueType(int initialInput, long initialTimestamp) {
    intValue = initialInput;
    outPutDataType = TSDataType.INT32;
    return int.class;
  }

  @Override
  public Class<?> initAndGetReturnValueType(long initialInput, long initialTimestamp) {
    longValue = initialInput;
    outPutDataType = TSDataType.INT64;
    return long.class;
  }

  @Override
  public Class<?> initAndGetReturnValueType(float initialInput, long initialTimestamp) {
    floatValue = initialInput;
    outPutDataType = TSDataType.FLOAT;
    return float.class;
  }

  @Override
  public Class<?> initAndGetReturnValueType(double initialInput, long initialTimestamp) {
    doubleValue = initialInput;
    outPutDataType = TSDataType.DOUBLE;
    return double.class;
  }

  @Override
  public Class<?> initAndGetReturnValueType(String initialInput, long initialTimestamp) {
    return null;
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
  public Object getResult() {
    switch (outPutDataType) {
      case INT32:
        return intValue;
      case INT64:
        return longValue;
      case FLOAT:
        return floatValue;
      case DOUBLE:
        return doubleValue;
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
