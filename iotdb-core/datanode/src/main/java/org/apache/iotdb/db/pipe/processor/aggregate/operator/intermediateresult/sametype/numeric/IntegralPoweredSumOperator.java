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
import org.apache.tsfile.read.common.type.Type;
import org.apache.tsfile.read.common.type.service.TypeService;

import java.io.IOException;
import java.nio.ByteBuffer;

public class IntegralPoweredSumOperator extends AbstractSameTypeNumericOperator {
  private final transient int power;

  private static final TypeService<Deserializer> DESERIALIZER_SERVICE =
      type ->
          switch (type.getTypeEnum()) {
            case INT32 -> operator -> operator.doubleValue = operator.intValue;
            case INT64 -> operator -> operator.doubleValue = operator.longValue;
            case FLOAT -> operator -> operator.doubleValue = operator.floatValue;
            case DOUBLE -> operator -> {};
            default ->
                operator -> {
                  throw new IOException();
                };
          };

  @FunctionalInterface
  private interface Deserializer {
    void deserialize(IntegralPoweredSumOperator operator) throws IOException;
  }

  public IntegralPoweredSumOperator(int power) {
    this.power = power;
  }

  @Override
  public String getName() {
    return "sum_x" + power;
  }

  @Override
  public boolean initAndGetIsSupport(int initialInput, long initialTimestamp) {
    doubleValue = Math.pow(initialInput, power);
    outPutDataType = TSDataType.DOUBLE;
    return true;
  }

  @Override
  public boolean initAndGetIsSupport(long initialInput, long initialTimestamp) {
    doubleValue = Math.pow(initialInput, power);
    outPutDataType = TSDataType.DOUBLE;
    return true;
  }

  @Override
  public boolean initAndGetIsSupport(float initialInput, long initialTimestamp) {
    doubleValue = Math.pow(initialInput, power);
    outPutDataType = TSDataType.DOUBLE;
    return true;
  }

  @Override
  public boolean initAndGetIsSupport(double initialInput, long initialTimestamp) {
    doubleValue = Math.pow(initialInput, power);
    outPutDataType = TSDataType.DOUBLE;
    return true;
  }

  @Override
  public void updateValue(int input, long timestamp) {
    doubleValue += Math.pow(input, power);
  }

  @Override
  public void updateValue(long input, long timestamp) {
    doubleValue += Math.pow(input, power);
  }

  @Override
  public void updateValue(float input, long timestamp) {
    doubleValue += Math.pow(input, power);
  }

  @Override
  public void updateValue(double input, long timestamp) {
    doubleValue += Math.pow(input, power);
  }

  @Override
  public void deserialize(final ByteBuffer byteBuffer) throws IOException {
    super.deserialize(byteBuffer);
    DESERIALIZER_SERVICE.call(Type.fromTsDataType(outPutDataType)).deserialize(this);
    outPutDataType = TSDataType.DOUBLE;
  }
}
