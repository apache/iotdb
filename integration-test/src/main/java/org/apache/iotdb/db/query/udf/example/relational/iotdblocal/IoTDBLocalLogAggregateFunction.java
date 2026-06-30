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
import org.apache.iotdb.udf.api.customizer.analysis.AggregateFunctionAnalysis;
import org.apache.iotdb.udf.api.customizer.parameter.FunctionArguments;
import org.apache.iotdb.udf.api.exception.UDFArgumentNotValidException;
import org.apache.iotdb.udf.api.relational.AggregateFunction;
import org.apache.iotdb.udf.api.relational.access.Record;
import org.apache.iotdb.udf.api.type.Type;
import org.apache.iotdb.udf.api.utils.ResultValue;

import java.nio.ByteBuffer;

/** Exercises IoTDBLocal log APIs at each aggregate-function lifecycle hook. */
public class IoTDBLocalLogAggregateFunction implements AggregateFunction {

  private static class CountState implements State {
    long count;
    boolean addInputLogged;
    boolean combineStateLogged;
    boolean outputFinalLogged;
    boolean destroyLogged;

    @Override
    public void reset() {
      count = 0;
    }

    @Override
    public byte[] serialize() {
      ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
      buffer.putLong(count);
      return buffer.array();
    }

    @Override
    public void deserialize(byte[] bytes) {
      count = ByteBuffer.wrap(bytes).getLong();
    }
  }

  @Override
  public AggregateFunctionAnalysis analyze(FunctionArguments arguments)
      throws UDFArgumentNotValidException {
    if (arguments.getArgumentsSize() != 1) {
      throw new UDFArgumentNotValidException("IoTDBLocalLogAggregateFunction accepts one column");
    }
    return new AggregateFunctionAnalysis.Builder().outputDataType(Type.INT64).build();
  }

  @Override
  public void beforeStart(FunctionArguments arguments, IoTDBLocal local) {
    IoTDBLocalLogHelper.logAllApis(local, IoTDBLocalLogHelper.UDAF_BEFORE_START);
  }

  @Override
  public State createState() {
    return new CountState();
  }

  @Override
  public void addInput(State state, Record input) {
    CountState countState = (CountState) state;
    countState.count++;
  }

  @Override
  public void addInput(State state, Record input, IoTDBLocal local) {
    CountState countState = (CountState) state;
    if (!countState.addInputLogged) {
      countState.addInputLogged = true;
      IoTDBLocalLogHelper.logAllApis(local, IoTDBLocalLogHelper.UDAF_ADD_INPUT);
    }
    addInput(state, input);
  }

  @Override
  public void combineState(State state, State rhs) {
    CountState left = (CountState) state;
    CountState right = (CountState) rhs;
    left.count += right.count;
  }

  @Override
  public void combineState(State state, State rhs, IoTDBLocal local) {
    CountState countState = (CountState) state;
    if (!countState.combineStateLogged) {
      countState.combineStateLogged = true;
      IoTDBLocalLogHelper.logAllApis(local, IoTDBLocalLogHelper.UDAF_COMBINE_STATE);
    }
    combineState(state, rhs);
  }

  @Override
  public void outputFinal(State state, ResultValue resultValue) {
    resultValue.setLong(((CountState) state).count);
  }

  @Override
  public void outputFinal(State state, ResultValue resultValue, IoTDBLocal local) {
    CountState countState = (CountState) state;
    if (!countState.outputFinalLogged) {
      countState.outputFinalLogged = true;
      IoTDBLocalLogHelper.logAllApis(local, IoTDBLocalLogHelper.UDAF_OUTPUT_FINAL);
    }
    outputFinal(state, resultValue);
  }

  @Override
  public void beforeDestroy(IoTDBLocal local) {
    IoTDBLocalLogHelper.logAllApis(local, IoTDBLocalLogHelper.UDAF_BEFORE_DESTROY);
  }
}
