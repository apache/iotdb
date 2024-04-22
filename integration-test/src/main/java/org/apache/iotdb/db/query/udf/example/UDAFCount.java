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

package org.apache.iotdb.db.query.udf.example;

import org.apache.iotdb.udf.api.State;
import org.apache.iotdb.udf.api.UDAF;
import org.apache.iotdb.udf.api.customizer.config.UDAFConfigurations;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameterValidator;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameters;
import org.apache.iotdb.udf.api.type.Type;
import org.apache.iotdb.udf.api.utils.ResultValue;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.utils.BitMap;

import java.nio.ByteBuffer;

public class UDAFCount implements UDAF {
  static class CountState implements State {
    long count;

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
      ByteBuffer buffer = ByteBuffer.wrap(bytes);
      count = buffer.getLong();
    }
  }

  @Override
  public void validate(UDFParameterValidator validator) throws Exception {
    validator
        .validateInputSeriesNumber(1)
        .validateInputSeriesDataType(
            0, Type.INT32, Type.INT64, Type.FLOAT, Type.DOUBLE, Type.BOOLEAN, Type.TEXT);
  }

  @Override
  public void beforeStart(UDFParameters parameters, UDAFConfigurations configurations) {
    configurations.setOutputDataType(Type.INT64);
  }

  @Override
  public State createState() {
    return new CountState();
  }

  @Override
  public void addInput(State state, Column[] column, BitMap bitMap) {
    CountState countState = (CountState) state;

    int count = column[0].getPositionCount();
    for (int i = 0; i < count; i++) {
      if (bitMap != null && !bitMap.isMarked(i)) {
        continue;
      }
      if (!column[0].isNull(i)) {
        countState.count++;
      }
    }
  }

  @Override
  public void combineState(State state, State rhs) {
    CountState countState = (CountState) state;
    CountState countRhs = (CountState) rhs;

    countState.count += countRhs.count;
  }

  @Override
  public void outputFinal(State state, ResultValue resultValue) {
    CountState countState = (CountState) state;
    resultValue.setLong(countState.count);
  }

  @Override
  public void removeState(State state, State removed) {
    CountState countState = (CountState) state;
    CountState countRhs = (CountState) removed;

    countState.count -= countRhs.count;
  }
}
