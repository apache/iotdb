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

package org.apache.iotdb.db.mpp.sql.planner.plan.parameter;

import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.nio.ByteBuffer;

/**
 * This attribute indicates the input and output type of the {@code Aggregator}.
 *
 * <p>There are three types of input/output:
 *
 * <ul>
 *   <li>Raw: raw data, as input only
 *   <li>Partial: intermediate aggregation result
 *   <li>Final: final aggregation result, as output only
 * </ul>
 */
public enum AggregationStep {

  // input Raw, output Partial
  PARTIAL(true, true),
  // input Partial, output Final
  FINAL(false, false),
  // input Partial, output Partial
  INTERMEDIATE(false, true),
  // input Raw, output Final
  SINGLE(true, false);

  private final boolean inputRaw;
  private final boolean outputPartial;

  AggregationStep(boolean inputRaw, boolean outputPartial) {
    this.inputRaw = inputRaw;
    this.outputPartial = outputPartial;
  }

  public boolean isInputRaw() {
    return inputRaw;
  }

  public boolean isOutputPartial() {
    return outputPartial;
  }

  public static AggregationStep partialOutput(AggregationStep step) {
    if (step.isInputRaw()) {
      return AggregationStep.PARTIAL;
    }
    return AggregationStep.INTERMEDIATE;
  }

  public static AggregationStep partialInput(AggregationStep step) {
    if (step.isOutputPartial()) {
      return AggregationStep.INTERMEDIATE;
    }
    return AggregationStep.FINAL;
  }

  public void serialize(ByteBuffer byteBuffer) {
    ReadWriteIOUtils.write(isInputRaw(), byteBuffer);
    ReadWriteIOUtils.write(isOutputPartial(), byteBuffer);
  }

  public static AggregationStep deserialize(ByteBuffer byteBuffer) {
    boolean isInputRaw = ReadWriteIOUtils.readBool(byteBuffer);
    boolean isOutputPartial = ReadWriteIOUtils.readBool(byteBuffer);
    if (isInputRaw && isOutputPartial) {
      return AggregationStep.PARTIAL;
    }
    if (!isInputRaw && isOutputPartial) {
      return AggregationStep.INTERMEDIATE;
    }
    if (isInputRaw) {
      return AggregationStep.SINGLE;
    }
    return AggregationStep.FINAL;
  }
}
