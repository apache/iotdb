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

package org.apache.iotdb.db.pipe.processor.aggregate.operator.intermediateresult.specifictype.doubletype;

import org.apache.iotdb.db.pipe.processor.aggregate.operator.intermediateresult.IntermediateResultOperator;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;

public class FractionPoweredSumOperator implements IntermediateResultOperator {
  private double sum;
  private final transient double power;

  /**
   * Warning: Do not use fraction values like "1/3" to initiate the operator, unless you know what
   * the output is when printing the double power. For instance, use "0.3333" instead, and you can
   * identify the operator's name as "sum_x0.3333".
   *
   * @param power the power to use
   */
  public FractionPoweredSumOperator(double power) {
    this.power = power;
  }

  @Override
  public String getName() {
    return "sum_x" + power;
  }

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
    sum = Math.pow(initialInput, power);
    return true;
  }

  @Override
  public boolean initAndGetIsSupport(long initialInput, long initialTimestamp) {
    sum = Math.pow(initialInput, power);
    return true;
  }

  @Override
  public boolean initAndGetIsSupport(float initialInput, long initialTimestamp) {
    sum = Math.pow(initialInput, power);
    return true;
  }

  @Override
  public boolean initAndGetIsSupport(double initialInput, long initialTimestamp) {
    sum = Math.pow(initialInput, power);
    return true;
  }

  @Override
  public boolean initAndGetIsSupport(String initialInput, long initialTimestamp) {
    return false;
  }

  @Override
  public void updateValue(boolean input, long timestamp) {
    throw new UnsupportedOperationException(
        "FractionPoweredSumOperator does not support boolean input");
  }

  @Override
  public void updateValue(int input, long timestamp) {
    sum += Math.pow(input, power);
  }

  @Override
  public void updateValue(long input, long timestamp) {
    sum += Math.pow(input, power);
  }

  @Override
  public void updateValue(float input, long timestamp) {
    sum += Math.pow(input, power);
  }

  @Override
  public void updateValue(double input, long timestamp) {
    sum += Math.pow(input, power);
  }

  @Override
  public void updateValue(String input, long timestamp) {
    throw new UnsupportedOperationException(
        "FractionPoweredSumOperator does not support string input");
  }

  @Override
  public Pair<TSDataType, Object> getResult() {
    return new Pair<>(TSDataType.DOUBLE, sum);
  }

  @Override
  public void serialize(DataOutputStream outputStream) throws IOException {
    ReadWriteIOUtils.write(sum, outputStream);
  }

  @Override
  public void deserialize(ByteBuffer byteBuffer) throws IOException {
    sum = ReadWriteIOUtils.readDouble(byteBuffer);
  }
}
