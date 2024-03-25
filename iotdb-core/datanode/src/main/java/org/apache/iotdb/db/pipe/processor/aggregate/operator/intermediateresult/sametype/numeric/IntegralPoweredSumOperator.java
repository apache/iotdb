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

public class IntegralPoweredSumOperator extends AbstractSameTypeNumericOperator {
  private final transient int power;

  public IntegralPoweredSumOperator(int power) {
    this.power = power;
  }

  @Override
  public String getName() {
    return "sum_x" + power;
  }

  @Override
  public boolean initAndGetIsSupport(int initialInput, long initialTimestamp) {
    intValue = (int) Math.pow(initialInput, power);
    return super.initAndGetIsSupport(initialInput, initialTimestamp);
  }

  @Override
  public boolean initAndGetIsSupport(long initialInput, long initialTimestamp) {
    longValue = (long) Math.pow(initialInput, power);
    return super.initAndGetIsSupport(initialInput, initialTimestamp);
  }

  @Override
  public boolean initAndGetIsSupport(float initialInput, long initialTimestamp) {
    floatValue = (float) Math.pow(initialInput, power);
    return super.initAndGetIsSupport(initialInput, initialTimestamp);
  }

  @Override
  public boolean initAndGetIsSupport(double initialInput, long initialTimestamp) {
    doubleValue = Math.pow(initialInput, power);
    return super.initAndGetIsSupport(initialInput, initialTimestamp);
  }

  @Override
  public void updateValue(int input, long timestamp) {
    intValue += (int) Math.pow(input, power);
  }

  @Override
  public void updateValue(long input, long timestamp) {
    longValue += (long) Math.pow(input, power);
  }

  @Override
  public void updateValue(float input, long timestamp) {
    floatValue += (float) Math.pow(input, power);
  }

  @Override
  public void updateValue(double input, long timestamp) {
    doubleValue += Math.pow(input, power);
  }
}
