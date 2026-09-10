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

/**
 * UDAF that calls {@link IoTDBLocal#query(String)} only in {@link #combineState(State, State,
 * IoTDBLocal)}.
 */
public class LocalQueryUdafInCombineStateAggregateFunction implements AggregateFunction {

  private static class CountState implements State {
    long count;
    long extraCount;
    boolean combineQueried;

    @Override
    public void reset() {
      count = 0;
      extraCount = 0;
      combineQueried = false;
    }

    @Override
    public byte[] serialize() {
      ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES * 2 + 1);
      buffer.putLong(count);
      buffer.putLong(extraCount);
      buffer.put((byte) (combineQueried ? 1 : 0));
      return buffer.array();
    }

    @Override
    public void deserialize(byte[] bytes) {
      ByteBuffer buffer = ByteBuffer.wrap(bytes);
      count = buffer.getLong();
      extraCount = buffer.getLong();
      combineQueried = buffer.get() != 0;
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
  public State createState() {
    return new CountState();
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
    left.combineQueried = left.combineQueried || right.combineQueried;
  }

  @Override
  public void combineState(State state, State rhs, IoTDBLocal local) {
    CountState left = (CountState) state;
    if (!left.combineQueried) {
      try (UDFResultSet rs = local.query("SELECT COUNT(*) FROM device_limits")) {
        if (rs.hasNext()) {
          left.extraCount = rs.next().getLong(0);
        }
      } catch (UDFException e) {
        throw new IllegalStateException(e);
      }
      left.combineQueried = true;
    }
    combineState(state, rhs);
  }

  @Override
  public void outputFinal(State state, ResultValue resultValue) {
    CountState countState = (CountState) state;
    resultValue.setLong(countState.count + countState.extraCount);
  }
}
