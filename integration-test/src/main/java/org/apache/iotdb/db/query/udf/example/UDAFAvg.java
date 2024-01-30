package org.apache.iotdb.db.query.udf.example;

import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.read.common.block.column.Column;
import org.apache.iotdb.tsfile.utils.BitMap;
import org.apache.iotdb.udf.api.State;
import org.apache.iotdb.udf.api.UDAF;
import org.apache.iotdb.udf.api.customizer.config.UDAFConfigurations;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameterValidator;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameters;
import org.apache.iotdb.udf.api.exception.UDFException;
import org.apache.iotdb.udf.api.type.Type;
import org.apache.iotdb.udf.api.utils.ResultValue;

import java.nio.ByteBuffer;

public class UDAFAvg implements UDAF {
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
    return new AvgState();
  }

  @Override
  public void addInput(State state, Column[] columns, BitMap bitMap) {
    AvgState avgState = (AvgState) state;

    switch (dataType) {
      case INT32:
        addIntInput(avgState, columns, bitMap);
        return;
      case INT64:
        addLongInput(avgState, columns, bitMap);
        return;
      case FLOAT:
        addFloatInput(avgState, columns, bitMap);
        return;
      case DOUBLE:
        addDoubleInput(avgState, columns, bitMap);
        return;
      case TEXT:
      case BOOLEAN:
      default:
        throw new UnSupportedDataTypeException(
            String.format("Unsupported data type in aggregation AVG : %s", dataType));
    }
  }

  @Override
  public void combineState(State state, State rhs) {
    AvgState avgState = (AvgState) state;
    AvgState avgRhs = (AvgState) rhs;

    avgState.count += avgRhs.count;
    avgState.sum += avgRhs.sum;
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

  @Override
  public void removeState(State state, State removed) {
    AvgState avgState = (AvgState) state;
    AvgState avgRhs = (AvgState) removed;

    avgState.count -= avgRhs.count;
    avgState.sum -= avgRhs.sum;
  }

  private void addIntInput(AvgState state, Column[] columns, BitMap bitMap) {
    int count = columns[0].getPositionCount();
    for (int i = 0; i < count; i++) {
      if (bitMap != null && !bitMap.isMarked(i)) {
        continue;
      }
      if (!columns[1].isNull(i)) {
        state.count++;
        state.sum += columns[1].getInt(i);
      }
    }
  }

  private void addLongInput(AvgState avgState, Column[] columns, BitMap bitMap) {
    int count = columns[0].getPositionCount();
    for (int i = 0; i < count; i++) {
      if (bitMap != null && !bitMap.isMarked(i)) {
        continue;
      }
      if (!columns[1].isNull(i)) {
        avgState.count++;
        avgState.sum += columns[1].getLong(i);
      }
    }
  }

  private void addFloatInput(AvgState avgState, Column[] columns, BitMap bitMap) {
    int count = columns[0].getPositionCount();
    for (int i = 0; i < count; i++) {
      if (bitMap != null && !bitMap.isMarked(i)) {
        continue;
      }
      if (!columns[1].isNull(i)) {
        avgState.count++;
        avgState.sum += columns[1].getFloat(i);
      }
    }
  }

  private void addDoubleInput(AvgState avgState, Column[] columns, BitMap bitMap) {
    int count = columns[0].getPositionCount();
    for (int i = 0; i < count; i++) {
      if (bitMap != null && !bitMap.isMarked(i)) {
        continue;
      }
      if (!columns[1].isNull(i)) {
        avgState.count++;
        avgState.sum += columns[1].getDouble(i);
      }
    }
  }
}
