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

package org.apache.iotdb.udf;

import org.apache.iotdb.udf.api.State;
import org.apache.iotdb.udf.api.customizer.config.AggregateFunctionConfig;
import org.apache.iotdb.udf.api.customizer.parameter.FunctionParameters;
import org.apache.iotdb.udf.api.exception.UDFException;
import org.apache.iotdb.udf.api.relational.AggregateFunction;
import org.apache.iotdb.udf.api.relational.access.Record;
import org.apache.iotdb.udf.api.type.Type;
import org.apache.iotdb.udf.api.utils.ResultValue;

import java.nio.ByteBuffer;

public class MyFirst implements AggregateFunction {

  static class MyFirstState implements State {
    long firstTime = Long.MAX_VALUE;
    double firstValue;

    @Override
    public void reset() {
      firstTime = Long.MAX_VALUE;
      firstValue = 0;
    }

    @Override
    public byte[] serialize() {
      ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES * 2 + Double.BYTES * 2);
      buffer.putLong(firstTime);
      buffer.putDouble(firstValue);
      return buffer.array();
    }

    @Override
    public void deserialize(byte[] bytes) {
      ByteBuffer buffer = ByteBuffer.wrap(bytes);
      firstTime = buffer.getLong();
      firstValue = buffer.getDouble();
    }
  }

  @Override
  public void validate(FunctionParameters parameters) throws UDFException {
    if (parameters.getChildExpressionsSize() != 2) {
      throw new UDFException("MyFirst should accept two parameters");
    }
    if (parameters.getDataType(0) != Type.DOUBLE) {
      throw new UDFException("The first parameter of MyFirst should be DOUBLE");
    }
    if (parameters.getDataType(1) != Type.TIMESTAMP) {
      throw new UDFException("The second parameter of MyFirst should be TIMESTAMP");
    }
  }

  @Override
  public void beforeStart(FunctionParameters parameters, AggregateFunctionConfig configurations) {
    configurations.setOutputDataType(Type.DOUBLE);
  }

  @Override
  public State createState() {
    return new MyFirstState();
  }

  @Override
  public void addInput(State state, Record input) {
    MyFirstState myFirstState = (MyFirstState) state;
    if (!input.isNull(0) && input.getLong(1) < myFirstState.firstTime) {
      myFirstState.firstTime = input.getLong(1);
      myFirstState.firstValue = input.getDouble(0);
    }
  }

  @Override
  public void combineState(State state, State rhs) {
    MyFirstState myFirstState = (MyFirstState) state;
    MyFirstState rhsState = (MyFirstState) rhs;
    if (rhsState.firstTime < myFirstState.firstTime) {
      myFirstState.firstTime = rhsState.firstTime;
      myFirstState.firstValue = rhsState.firstValue;
    }
  }

  @Override
  public void outputFinal(State state, ResultValue resultValue) {
    resultValue.setDouble(((MyFirstState) state).firstValue);
  }
}
