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

package org.apache.iotdb.db.query.udf.example.relational.iotdblocal;

import org.apache.iotdb.udf.api.IoTDBLocal;
import org.apache.iotdb.udf.api.State;
import org.apache.iotdb.udf.api.UDFResultSet;
import org.apache.iotdb.udf.api.customizer.analysis.AggregateFunctionAnalysis;
import org.apache.iotdb.udf.api.customizer.parameter.FunctionArguments;
import org.apache.iotdb.udf.api.exception.UDFArgumentNotValidException;
import org.apache.iotdb.udf.api.exception.UDFException;
import org.apache.iotdb.udf.api.relational.AggregateFunction;
import org.apache.iotdb.udf.api.relational.access.Record;
import org.apache.iotdb.udf.api.type.Type;
import org.apache.iotdb.udf.api.utils.ResultValue;

import java.nio.ByteBuffer;

/** UDAF that calls {@link IoTDBLocal#query(String)} only in {@link #beforeStart}. */
public class LocalQueryUdafAtBeforeStartAggregateFunction implements AggregateFunction {

  private long extraCount;

  private static class CountState implements State {
    long count;
    long extraCount;

    @Override
    public void reset() {
      count = 0;
    }

    @Override
    public byte[] serialize() {
      ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES * 2);
      buffer.putLong(count);
      buffer.putLong(extraCount);
      return buffer.array();
    }

    @Override
    public void deserialize(byte[] bytes) {
      ByteBuffer buffer = ByteBuffer.wrap(bytes);
      count = buffer.getLong();
      extraCount = buffer.getLong();
    }
  }

  @Override
  public AggregateFunctionAnalysis analyze(FunctionArguments arguments)
      throws UDFArgumentNotValidException {
    if (arguments.getArgumentsSize() != 1) {
      throw new UDFArgumentNotValidException("expects one column");
    }
    return new AggregateFunctionAnalysis.Builder().outputDataType(Type.INT64).build();
  }

  @Override
  public void beforeStart(FunctionArguments arguments, IoTDBLocal local) throws UDFException {
    try (UDFResultSet rs = local.query("SELECT COUNT(*) FROM device_info")) {
      if (rs.hasNext()) {
        extraCount = rs.next().getLong(0);
      }
    }
  }

  @Override
  public State createState() {
    CountState state = new CountState();
    state.extraCount = extraCount;
    return state;
  }

  @Override
  public void addInput(State state, Record input) {
    if (!input.isNull(0)) {
      ((CountState) state).count++;
    }
  }

  @Override
  public void combineState(State state, State rhs) {
    CountState left = (CountState) state;
    CountState right = (CountState) rhs;
    left.count += right.count;
    left.extraCount = Math.max(left.extraCount, right.extraCount);
  }

  @Override
  public void outputFinal(State state, ResultValue resultValue) {
    CountState countState = (CountState) state;
    resultValue.setLong(countState.count + countState.extraCount);
  }
}
