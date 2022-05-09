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
package org.apache.iotdb.db.mpp.plan.planner.plan.parameter;

import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.nio.ByteBuffer;
import java.util.Objects;

public class InputLocation {
  // which input tsBlock
  private final int tsBlockIndex;
  // which value column of that tsBlock
  private final int valueColumnIndex;

  public InputLocation(int tsBlockIndex, int valueColumnIndex) {
    this.tsBlockIndex = tsBlockIndex;
    this.valueColumnIndex = valueColumnIndex;
  }

  public int getTsBlockIndex() {
    return tsBlockIndex;
  }

  public int getValueColumnIndex() {
    return valueColumnIndex;
  }

  public void serialize(ByteBuffer byteBuffer) {
    ReadWriteIOUtils.write(tsBlockIndex, byteBuffer);
    ReadWriteIOUtils.write(valueColumnIndex, byteBuffer);
  }

  public static InputLocation deserialize(ByteBuffer byteBuffer) {
    int tsBlockIndex = ReadWriteIOUtils.readInt(byteBuffer);
    int valueColumnIndex = ReadWriteIOUtils.readInt(byteBuffer);
    return new InputLocation(tsBlockIndex, valueColumnIndex);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    InputLocation that = (InputLocation) o;
    return tsBlockIndex == that.tsBlockIndex && valueColumnIndex == that.valueColumnIndex;
  }

  @Override
  public int hashCode() {
    return Objects.hash(tsBlockIndex, valueColumnIndex);
  }
}
