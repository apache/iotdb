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

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * This attribute indicates the input and output type of the {@code Aggregator}.
 *
 * <p>There are three types of input/output:
 *
 * <ul>
 *   <li>Raw: raw data, as input only
 *   <li>Partial: intermediate aggregation result
 *   <li>Final: final aggregation result
 * </ul>
 */
public enum AggregationStep {

  // input Raw, output Partial
  PARTIAL(InputType.RAW, true, (byte) 0),
  // input Partial, output Final
  FINAL(InputType.PARTIAL, false, (byte) 1),
  // input Partial, output Partial
  INTERMEDIATE(InputType.PARTIAL, true, (byte) 2),
  // input Raw, output Final
  SINGLE(InputType.RAW, false, (byte) 3),
  // input final, output final
  STATIC(InputType.FINAL, false, (byte) 4);

  private enum InputType {
    RAW,
    PARTIAL,
    FINAL
  }

  private final InputType inputType;
  private final boolean outputPartial;
  private final byte ordinal;

  AggregationStep(InputType inputType, boolean outputPartial, byte ordinal) {
    this.inputType = inputType;
    this.outputPartial = outputPartial;
    this.ordinal = ordinal;
  }

  public boolean isInputRaw() {
    return inputType == InputType.RAW;
  }

  public boolean isInputPartial() {
    return inputType == InputType.PARTIAL;
  }

  public boolean isInputFinal() {
    return inputType == InputType.FINAL;
  }

  public boolean isOutputPartial() {
    return outputPartial;
  }

  public void serialize(ByteBuffer byteBuffer) {
    ReadWriteIOUtils.write(ordinal, byteBuffer);
  }

  public void serialize(DataOutputStream stream) throws IOException {
    ReadWriteIOUtils.write(ordinal, stream);
  }

  public static AggregationStep deserialize(ByteBuffer byteBuffer) {
    byte type = ReadWriteIOUtils.readByte(byteBuffer);
    switch (type) {
      case 0:
        return AggregationStep.PARTIAL;
      case 1:
        return AggregationStep.FINAL;
      case 2:
        return AggregationStep.INTERMEDIATE;
      case 3:
        return AggregationStep.SINGLE;
      case 4:
        return AggregationStep.STATIC;
      default:
        throw new IllegalArgumentException("Invalid AggregationStep type: " + type);
    }
  }
}
