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

public class MaxOperator extends AbstractSameTypeNumericOperator {
  @Override
  public String getName() {
    return "max";
  }

  @Override
  public boolean initAndGetIsSupport(int initialInput, long initialTimestamp) {
    intValue = initialInput;
    return super.initAndGetIsSupport(initialInput, initialTimestamp);
  }

  @Override
  public boolean initAndGetIsSupport(long initialInput, long initialTimestamp) {
    longValue = initialInput;
    return super.initAndGetIsSupport(initialInput, initialTimestamp);
  }

  @Override
  public boolean initAndGetIsSupport(final float initialInput, final long initialTimestamp) {
    floatValue = initialInput;
    return super.initAndGetIsSupport(initialInput, initialTimestamp);
  }

  @Override
  public boolean initAndGetIsSupport(final double initialInput, final long initialTimestamp) {
    doubleValue = initialInput;
    return super.initAndGetIsSupport(initialInput, initialTimestamp);
  }

  @Override
  public void updateValue(final int input, final long timestamp) {
    intValue = Math.max(intValue, input);
  }

  @Override
  public void updateValue(final long input, final long timestamp) {
    longValue = Math.max(longValue, input);
  }

  @Override
  public void updateValue(final float input, final long timestamp) {
    floatValue = Math.max(floatValue, input);
  }

  @Override
  public void updateValue(final double input, final long timestamp) {
    doubleValue = Math.max(doubleValue, input);
  }
}
