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

public class MyAvg implements AggregateFunction {

  static class AvgState implements State {
    double sum;
    long count;

    @Override
    public void reset() {
      sum = 0;
      count = 0;
    }

    @Override
    public byte[] serialize() {
      ByteBuffer buffer = ByteBuffer.allocate(Double.BYTES + Long.BYTES);
      buffer.putDouble(sum);
      buffer.putLong(count);

      return buffer.array();
    }

    @Override
    public void deserialize(byte[] bytes) {
      ByteBuffer buffer = ByteBuffer.wrap(bytes);
      sum = buffer.getDouble();
      count = buffer.getLong();
    }
  }

  @Override
  public AggregateFunctionAnalysis analyze(FunctionArguments arguments)
      throws UDFArgumentNotValidException {
    if (arguments.getArgumentsSize() != 1) {
      throw new UDFArgumentNotValidException("MyAvg only accepts one column as input");
    }
    if (arguments.getDataType(0) != Type.INT32
        && arguments.getDataType(0) != Type.INT64
        && arguments.getDataType(0) != Type.FLOAT
        && arguments.getDataType(0) != Type.DOUBLE) {
      throw new UDFArgumentNotValidException(
          "MyAvg only accepts INT32, INT64, FLOAT, DOUBLE as input");
    }
    return new AggregateFunctionAnalysis.Builder()
        .outputDataType(Type.DOUBLE)
        .removable(true)
        .build();
  }

  @Override
  public State createState() {
    return new AvgState();
  }

  @Override
  public void addInput(State state, Record input) {
    if (!input.isNull(0)) {
      AvgState avgState = (AvgState) state;
      switch (input.getDataType(0)) {
        case INT32:
          avgState.sum += input.getInt(0);
          break;
        case INT64:
          avgState.sum += input.getLong(0);
          break;
        case FLOAT:
          avgState.sum += input.getFloat(0);
          break;
        case DOUBLE:
          avgState.sum += input.getDouble(0);
          break;
        default:
          throw new UDFException("MyAvg only accepts INT32, INT64, FLOAT, DOUBLE as input");
      }
      avgState.count++;
    }
  }

  @Override
  public void combineState(State state, State rhs) {
    AvgState avgState = (AvgState) state;
    AvgState avgRhs = (AvgState) rhs;
    avgState.sum += avgRhs.sum;
    avgState.count += avgRhs.count;
  }

  @Override
  public void outputFinal(State state, ResultValue resultValue) {
    AvgState avgState = (AvgState) state;
    if (avgState.count != 0) {
      resultValue.setDouble(avgState.sum / avgState.count);
    } else {
      resultValue.setNull();
    }
  }
}
