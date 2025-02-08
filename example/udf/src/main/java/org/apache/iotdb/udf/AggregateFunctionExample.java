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
import org.apache.iotdb.udf.api.customizer.analysis.AggregateFunctionAnalysis;
import org.apache.iotdb.udf.api.customizer.parameter.FunctionArguments;
import org.apache.iotdb.udf.api.exception.UDFArgumentNotValidException;
import org.apache.iotdb.udf.api.relational.AggregateFunction;
import org.apache.iotdb.udf.api.relational.access.Record;
import org.apache.iotdb.udf.api.type.Type;
import org.apache.iotdb.udf.api.utils.ResultValue;

import java.nio.ByteBuffer;

/**
 * This is an internal example of the AggregateFunction implementation.
 *
 * <p>CREATE DATABASE test;
 *
 * <p>USE test;
 *
 * <p>CREATE TABLE t1(device_id STRING TAG, s1 TEXT FIELD, s2 INT32 FIELD);
 *
 * <p>INSERT INTO t1(time, device_id, s1, s2) VALUES (1, 'd1', 'a', 1), (2, 'd1', null, 2), (3,
 * 'd2', 'c', null);
 *
 * <p>CREATE FUNCTION my_count AS 'org.apache.iotdb.udf.AggregateFunctionExample';
 *
 * <p>SHOW FUNCTIONS;
 *
 * <p>SELECT device_id, my_count(s1) as s1_count, my_count(s2) as s2_count FROM t1 group by
 * device_id;
 *
 * <p>SELECT my_count(s1) as s1_count, my_count(s2) as s2_count FROM t1;
 */
public class AggregateFunctionExample implements AggregateFunction {

  static class CountState implements State {

    long count;

    @Override
    public void reset() {
      count = 0;
    }

    @Override
    public byte[] serialize() {
      ByteBuffer buffer = ByteBuffer.allocate(Double.BYTES);
      buffer.putLong(count);
      return buffer.array();
    }

    @Override
    public void deserialize(byte[] bytes) {
      ByteBuffer buffer = ByteBuffer.wrap(bytes);
      count = buffer.getLong();
    }
  }

  @Override
  public AggregateFunctionAnalysis analyze(FunctionArguments arguments)
      throws UDFArgumentNotValidException {
    if (arguments.getArgumentsSize() != 1) {
      throw new UDFArgumentNotValidException("Only one parameter is required.");
    }
    return new AggregateFunctionAnalysis.Builder()
        .outputDataType(Type.INT64)
        .removable(true)
        .build();
  }

  @Override
  public State createState() {
    return new CountState();
  }

  @Override
  public void addInput(State state, Record input) {
    CountState countState = (CountState) state;
    if (!input.isNull(0)) {
      countState.count++;
    }
  }

  @Override
  public void combineState(State state, State rhs) {
    CountState countState = (CountState) state;
    CountState rhsCountState = (CountState) rhs;
    countState.count += rhsCountState.count;
  }

  @Override
  public void outputFinal(State state, ResultValue resultValue) {
    CountState countState = (CountState) state;
    resultValue.setLong(countState.count);
  }

  @Override
  public void remove(State state, Record input) {
    CountState countState = (CountState) state;
    if (!input.isNull(0)) {
      countState.count--;
    }
  }
}
