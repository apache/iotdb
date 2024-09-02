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
import org.apache.iotdb.udf.api.exception.UDFException;
import org.apache.iotdb.udf.api.type.Type;
import org.apache.iotdb.udf.api.utils.ResultValue;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.utils.BitMap;
import org.apache.tsfile.write.UnSupportedDataTypeException;

import java.nio.ByteBuffer;

public class UDAFSum implements UDAF {
  static class SumState implements State {
    double sum = 0;

    boolean initResult = false;

    @Override
    public void reset() {
      sum = 0;
      initResult = false;
    }

    @Override
    public byte[] serialize() {
      ByteBuffer buffer = ByteBuffer.allocate(Double.BYTES + 1);
      buffer.putDouble(sum);
      buffer.put(initResult ? (byte) 1 : (byte) 0);

      return buffer.array();
    }

    @Override
    public void deserialize(byte[] bytes) {
      ByteBuffer buffer = ByteBuffer.wrap(bytes);
      sum = buffer.getDouble();
      initResult = (buffer.get() == (byte) 1);
    }
  }

  private Type dataType;

  @Override
  public void validate(UDFParameterValidator validator) throws UDFException {
    validator
        .validateInputSeriesNumber(1)
        .validateInputSeriesDataType(0, Type.INT32, Type.INT64, Type.FLOAT, Type.DOUBLE);
  }

  @Override
  public void beforeStart(UDFParameters parameters, UDAFConfigurations configurations) {
    dataType = parameters.getDataType(0);
    configurations.setOutputDataType(Type.DOUBLE);
  }

  @Override
  public State createState() {
    return new SumState();
  }

  @Override
  public void addInput(State state, Column[] columns, BitMap bitMap) {
    SumState sumState = (SumState) state;

    switch (dataType) {
      case INT32:
        addIntInput(sumState, columns, bitMap);
        return;
      case INT64:
        addLongInput(sumState, columns, bitMap);
        return;
      case FLOAT:
        addFloatInput(sumState, columns, bitMap);
        return;
      case DOUBLE:
        addDoubleInput(sumState, columns, bitMap);
        return;
      case TEXT:
      case STRING:
      case BLOB:
      case BOOLEAN:
      case TIMESTAMP:
      case DATE:
      default:
        throw new UnSupportedDataTypeException(
            String.format("Unsupported data type in aggregation AVG : %s", dataType));
    }
  }

  @Override
  public void combineState(State state, State rhs) {
    SumState sumState = (SumState) state;
    SumState sumRhs = (SumState) rhs;

    sumState.initResult |= sumRhs.initResult;
    sumState.sum += sumRhs.sum;
  }

  @Override
  public void outputFinal(State state, ResultValue resultValue) {
    SumState sumState = (SumState) state;

    if (sumState.initResult) {
      resultValue.setDouble(sumState.sum);
    } else {
      resultValue.setNull();
    }
  }

  @Override
  public void removeState(State state, State removed) {
    SumState sumState = (SumState) state;
    SumState sumRhs = (SumState) removed;

    sumState.sum -= sumRhs.sum;
  }

  private void addIntInput(SumState state, Column[] columns, BitMap bitMap) {
    int count = columns[0].getPositionCount();
    for (int i = 0; i < count; i++) {
      if (bitMap != null && !bitMap.isMarked(i)) {
        continue;
      }
      if (!columns[0].isNull(i)) {
        state.initResult = true;
        state.sum += columns[0].getInt(i);
      }
    }
  }

  private void addLongInput(SumState state, Column[] columns, BitMap bitMap) {
    int count = columns[0].getPositionCount();
    for (int i = 0; i < count; i++) {
      if (bitMap != null && !bitMap.isMarked(i)) {
        continue;
      }
      if (!columns[0].isNull(i)) {
        state.initResult = true;
        state.sum += columns[0].getLong(i);
      }
    }
  }

  private void addFloatInput(SumState state, Column[] columns, BitMap bitMap) {
    int count = columns[0].getPositionCount();
    for (int i = 0; i < count; i++) {
      if (bitMap != null && !bitMap.isMarked(i)) {
        continue;
      }
      if (!columns[0].isNull(i)) {
        state.initResult = true;
        state.sum += columns[0].getFloat(i);
      }
    }
  }

  private void addDoubleInput(SumState state, Column[] columns, BitMap bitMap) {
    int count = columns[0].getPositionCount();
    for (int i = 0; i < count; i++) {
      if (bitMap != null && !bitMap.isMarked(i)) {
        continue;
      }
      if (!columns[0].isNull(i)) {
        state.initResult = true;
        state.sum += columns[0].getDouble(i);
      }
    }
  }
}
