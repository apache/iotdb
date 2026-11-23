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

import org.apache.tsfile.enums.TSDataType;

import java.io.IOException;
import java.nio.ByteBuffer;

public class AbsoluteMaxOperator extends AbstractSameTypeNumericOperator {
  @Override
  public String getName() {
    return "abs_max";
  }

  @Override
  public boolean initAndGetIsSupport(final int initialInput, final long initialTimestamp) {
    if (initialInput == Integer.MIN_VALUE) {
      doubleValue = -(double) initialInput;
      outPutDataType = TSDataType.DOUBLE;
    } else {
      intValue = Math.abs(initialInput);
      outPutDataType = TSDataType.INT32;
    }
    return true;
  }

  @Override
  public boolean initAndGetIsSupport(final long initialInput, final long initialTimestamp) {
    if (initialInput == Long.MIN_VALUE) {
      doubleValue = -(double) initialInput;
      outPutDataType = TSDataType.DOUBLE;
    } else {
      longValue = Math.abs(initialInput);
      outPutDataType = TSDataType.INT64;
    }
    return true;
  }

  @Override
  public boolean initAndGetIsSupport(final float initialInput, final long initialTimestamp) {
    floatValue = Math.abs(initialInput);
    outPutDataType = TSDataType.FLOAT;
    return true;
  }

  @Override
  public boolean initAndGetIsSupport(final double initialInput, final long initialTimestamp) {
    doubleValue = Math.abs(initialInput);
    outPutDataType = TSDataType.DOUBLE;
    return true;
  }

  @Override
  public void updateValue(final int input, final long timestamp) {
    if (outPutDataType == TSDataType.DOUBLE) {
      doubleValue = Math.max(doubleValue, Math.abs((double) input));
    } else if (input == Integer.MIN_VALUE) {
      doubleValue = -(double) input;
      outPutDataType = TSDataType.DOUBLE;
    } else {
      intValue = Math.max(intValue, Math.abs(input));
    }
  }

  @Override
  public void updateValue(final long input, final long timestamp) {
    if (outPutDataType == TSDataType.DOUBLE) {
      doubleValue = Math.max(doubleValue, Math.abs((double) input));
    } else if (input == Long.MIN_VALUE) {
      doubleValue = -(double) input;
      outPutDataType = TSDataType.DOUBLE;
    } else {
      longValue = Math.max(longValue, Math.abs(input));
    }
  }

  @Override
  public void updateValue(final float input, final long timestamp) {
    floatValue = Math.max(floatValue, Math.abs(input));
  }

  @Override
  public void updateValue(final double input, final long timestamp) {
    doubleValue = Math.max(doubleValue, Math.abs(input));
  }

  @Override
  public void deserialize(final ByteBuffer byteBuffer) throws IOException {
    super.deserialize(byteBuffer);
    switch (outPutDataType) {
      case INT32:
        if (intValue == Integer.MIN_VALUE) {
          doubleValue = -(double) intValue;
          outPutDataType = TSDataType.DOUBLE;
        } else {
          intValue = Math.abs(intValue);
        }
        break;
      case INT64:
        if (longValue == Long.MIN_VALUE) {
          doubleValue = -(double) longValue;
          outPutDataType = TSDataType.DOUBLE;
        } else {
          longValue = Math.abs(longValue);
        }
        break;
      case FLOAT:
        floatValue = Math.abs(floatValue);
        break;
      case DOUBLE:
        doubleValue = Math.abs(doubleValue);
        break;
      default:
        throw new IOException();
    }
  }
}
