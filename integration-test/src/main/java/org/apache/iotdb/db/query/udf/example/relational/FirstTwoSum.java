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

package org.apache.iotdb.db.query.udf.example.relational;

import org.apache.iotdb.udf.api.State;
import org.apache.iotdb.udf.api.customizer.analysis.AggregateFunctionAnalysis;
import org.apache.iotdb.udf.api.customizer.parameter.FunctionArguments;
import org.apache.iotdb.udf.api.exception.UDFArgumentNotValidException;
import org.apache.iotdb.udf.api.exception.UDFException;
import org.apache.iotdb.udf.api.relational.AggregateFunction;
import org.apache.iotdb.udf.api.relational.access.Record;
import org.apache.iotdb.udf.api.type.Type;
import org.apache.iotdb.udf.api.utils.ResultValue;

import java.nio.ByteBuffer;

public class FirstTwoSum implements AggregateFunction {

  static class FirstTwoSumState implements State {
    long firstTime = Long.MAX_VALUE;
    long secondTime = Long.MAX_VALUE;
    double firstValue;
    double secondValue;

    @Override
    public void reset() {
      firstTime = Long.MAX_VALUE;
      secondTime = Long.MAX_VALUE;
      firstValue = 0;
      secondValue = 0;
    }

    @Override
    public byte[] serialize() {
      ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES * 2 + Double.BYTES * 2);
      buffer.putLong(firstTime);
      buffer.putLong(secondTime);
      buffer.putDouble(firstValue);
      buffer.putDouble(secondValue);
      return buffer.array();
    }

    @Override
    public void deserialize(byte[] bytes) {
      ByteBuffer buffer = ByteBuffer.wrap(bytes);
      firstTime = buffer.getLong();
      secondTime = buffer.getLong();
      firstValue = buffer.getDouble();
      secondValue = buffer.getDouble();
    }
  }

  @Override
  public AggregateFunctionAnalysis analyze(FunctionArguments arguments)
      throws UDFArgumentNotValidException {
    if (arguments.getArgumentsSize() != 3) {
      throw new UDFArgumentNotValidException("FirstTwoSum should accept three column as input");
    }
    for (int i = 0; i < 2; i++) {
      if (arguments.getDataType(i) != Type.INT32
          && arguments.getDataType(i) != Type.INT64
          && arguments.getDataType(i) != Type.FLOAT
          && arguments.getDataType(i) != Type.DOUBLE) {
        throw new UDFArgumentNotValidException(
            "FirstTwoSum should accept INT32, INT64, FLOAT, DOUBLE as the first two inputs");
      }
    }
    if (arguments.getDataType(2) != Type.TIMESTAMP) {
      throw new UDFArgumentNotValidException(
          "FirstTwoSum should accept TIMESTAMP as the third input");
    }
    return new AggregateFunctionAnalysis.Builder().outputDataType(Type.DOUBLE).build();
  }

  @Override
  public State createState() {
    return new FirstTwoSumState();
  }

  @Override
  public void addInput(State state, Record input) {
    FirstTwoSumState firstTwoSumState = (FirstTwoSumState) state;
    long time = input.getLong(2);
    if (!input.isNull(0) && time < firstTwoSumState.firstTime) {
      firstTwoSumState.firstTime = time;
      switch (input.getDataType(0)) {
        case INT32:
          firstTwoSumState.firstValue = input.getInt(0);
          break;
        case INT64:
          firstTwoSumState.firstValue = input.getLong(0);
          break;
        case FLOAT:
          firstTwoSumState.firstValue = input.getFloat(0);
          break;
        case DOUBLE:
          firstTwoSumState.firstValue = input.getDouble(0);
          break;
        default:
          throw new UDFException(
              "FirstTwoSum should accept INT32, INT64, FLOAT, DOUBLE as the first two inputs");
      }
    }
    if (!input.isNull(1) && time < firstTwoSumState.secondTime) {
      firstTwoSumState.secondTime = time;
      switch (input.getDataType(1)) {
        case INT32:
          firstTwoSumState.secondValue = input.getInt(1);
          break;
        case INT64:
          firstTwoSumState.secondValue = input.getLong(1);
          break;
        case FLOAT:
          firstTwoSumState.secondValue = input.getFloat(1);
          break;
        case DOUBLE:
          firstTwoSumState.secondValue = input.getDouble(1);
          break;
        default:
          throw new UDFException(
              "FirstTwoSum should accept INT32, INT64, FLOAT, DOUBLE as the first two inputs");
      }
    }
  }

  @Override
  public void combineState(State state, State rhs) {
    FirstTwoSumState firstTwoSumState = (FirstTwoSumState) state;
    FirstTwoSumState rhsState = (FirstTwoSumState) rhs;
    if (rhsState.firstTime < firstTwoSumState.firstTime) {
      firstTwoSumState.firstTime = rhsState.firstTime;
      firstTwoSumState.firstValue = rhsState.firstValue;
    }
    if (rhsState.secondTime < firstTwoSumState.secondTime) {
      firstTwoSumState.secondTime = rhsState.secondTime;
      firstTwoSumState.secondValue = rhsState.secondValue;
    }
  }

  @Override
  public void outputFinal(State state, ResultValue resultValue) {
    FirstTwoSumState firstTwoSumState = (FirstTwoSumState) state;
    if (firstTwoSumState.firstTime == Long.MAX_VALUE
        && firstTwoSumState.secondTime == Long.MAX_VALUE) {
      resultValue.setNull();
    } else {
      resultValue.setDouble(firstTwoSumState.firstValue + firstTwoSumState.secondValue);
    }
  }
}
