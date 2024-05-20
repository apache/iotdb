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

package org.apache.iotdb.db.pipe.processor.aggregate.operator.intermediateresult.specifictype.integertype;

import org.apache.iotdb.db.pipe.processor.aggregate.operator.intermediateresult.IntermediateResultOperator;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;

public class CountOperator implements IntermediateResultOperator {
  private int count;

  @Override
  public String getName() {
    return "count";
  }

  @Override
  public void configureSystemParameters(Map<String, String> systemParams) {
    // Do nothing
  }

  @Override
  public boolean initAndGetIsSupport(boolean initialInput, long initialTimestamp) {
    count = 1;
    return true;
  }

  @Override
  public boolean initAndGetIsSupport(int initialInput, long initialTimestamp) {
    count = 1;
    return true;
  }

  @Override
  public boolean initAndGetIsSupport(long initialInput, long initialTimestamp) {
    count = 1;
    return true;
  }

  @Override
  public boolean initAndGetIsSupport(float initialInput, long initialTimestamp) {
    count = 1;
    return true;
  }

  @Override
  public boolean initAndGetIsSupport(double initialInput, long initialTimestamp) {
    count = 1;
    return true;
  }

  @Override
  public boolean initAndGetIsSupport(String initialInput, long initialTimestamp) {
    count = 1;
    return true;
  }

  @Override
  public void updateValue(boolean input, long timestamp) {
    ++count;
  }

  @Override
  public void updateValue(int input, long timestamp) {
    ++count;
  }

  @Override
  public void updateValue(long input, long timestamp) {
    ++count;
  }

  @Override
  public void updateValue(float input, long timestamp) {
    ++count;
  }

  @Override
  public void updateValue(double input, long timestamp) {
    ++count;
  }

  @Override
  public void updateValue(String input, long timestamp) {
    ++count;
  }

  @Override
  public Pair<TSDataType, Object> getResult() {
    return new Pair<>(TSDataType.INT32, count);
  }

  @Override
  public void serialize(DataOutputStream outputStream) throws IOException {
    ReadWriteIOUtils.write(count, outputStream);
  }

  @Override
  public void deserialize(ByteBuffer byteBuffer) throws IOException {
    count = ReadWriteIOUtils.readInt(byteBuffer);
  }
}
