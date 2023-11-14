package org.apache.iotdb.db.queryengine.execution.aggregation;

import org.apache.iotdb.tsfile.access.Column;
import org.apache.iotdb.tsfile.access.ColumnBuilder;
import org.apache.iotdb.tsfile.enums.TSDataType;
import org.apache.iotdb.tsfile.exception.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.BitMap;
import org.apache.iotdb.tsfile.utils.BytesUtils;

import java.nio.ByteBuffer;
import java.util.Arrays;

import static com.google.common.base.Preconditions.checkArgument;

public class VarPopAccumulator implements Accumulator {
  private final TSDataType seriesDataType;

  // TODO: Add inner static class
  private long count;
  private double mean;
  private double m2;

  public VarPopAccumulator(TSDataType seriesDataType) {
    this.seriesDataType = seriesDataType;
  }

  @Override
  public void addInput(Column[] column, BitMap bitMap, int lastIndex) {
    switch (seriesDataType) {
      case INT32:
        addIntInput(column, bitMap, lastIndex);
        return;
      case FLOAT:
        addFloatInput(column, bitMap, lastIndex);
        return;
      case DOUBLE:
        addDoubleInput(column, bitMap, lastIndex);
        return;
      default:
        throw new UnSupportedDataTypeException(
            String.format("Unsupported data type in aggregation VAR_POP : %s", seriesDataType));
    }
  }

  @Override
  public void addIntermediate(Column[] partialResult) {
    checkArgument(partialResult.length == 1, "partialResult of VarPop should be 1");
    if (partialResult[0].isNull(0)) {
      return;
    }
    byte[] bytes = partialResult[0].getBinary(0).getValues();
    long intermediateCount = BytesUtils.bytesToLong(bytes, Long.BYTES);
    double intermediateMean = BytesUtils.bytesToDouble(bytes, Long.BYTES);
    double intermediateM2 = BytesUtils.bytesToDouble(bytes, (Long.BYTES + Double.BYTES));

    long newCount = count + intermediateCount;
    double newMean = ((intermediateCount * intermediateMean) + (count * mean)) / newCount;
    double delta = intermediateMean - mean;

    m2 = m2 + intermediateM2 + delta * delta * intermediateCount * count / newCount;
    count = newCount;
    mean = newMean;
  }

  @Override
  public void removeInput(Column[] input) {
    checkArgument(input.length == 1, "Input of VarPop should be 1");
    if (input[0].isNull(0)) {
      return;
    }
    // Deserialize
    byte[] bytes = input[0].getBinary(0).getValues();
    long intermediateCount = BytesUtils.bytesToLong(bytes, Long.BYTES);
    double intermediateMean = BytesUtils.bytesToDouble(bytes, Long.BYTES);
    double intermediateM2 = BytesUtils.bytesToDouble(bytes, (Long.BYTES + Double.BYTES));
    // Remove from state
    long newCount = count - intermediateCount;
    double newMean = ((count * mean) - (intermediateCount * intermediateMean)) / newCount;
    double delta = intermediateMean - mean;

    m2 = m2 - intermediateM2 - delta * delta * intermediateCount * count / newCount;
    count = newCount;
    mean = newMean;
  }

  @Override
  public void addStatistics(Statistics statistics) {
    throw new UnsupportedOperationException(getClass().getName());
  }

  @Override
  public void setFinal(Column finalResult) {
    reset();
    if (finalResult.isNull(0)) {
      return;
    }
    count = 1;
    double value = finalResult.getDouble(0);
    mean = value;
    m2 = value * value;
  }

  @Override
  public void outputIntermediate(ColumnBuilder[] columnBuilders) {
    checkArgument(columnBuilders.length == 1, "partialResult of VarPop should be 1");
    if (count == 0) {
      columnBuilders[0].appendNull();
    } else {
      byte[] bytes = serialize();
      columnBuilders[0].writeBinary(new Binary(bytes));
    }
  }

  private byte[] serialize() {
    byte[] countBytes = BytesUtils.longToBytes(count);
    byte[] meanBytes = BytesUtils.doubleToBytes(mean);
    byte[] m2Bytes = BytesUtils.doubleToBytes(m2);

    return BytesUtils.concatByteArrayList(Arrays.asList(countBytes, meanBytes, m2Bytes));
  }

  @Override
  public void outputFinal(ColumnBuilder columnBuilder) {
    if (count == 0) {
      columnBuilder.appendNull();
    } else {
      columnBuilder.writeDouble(m2 / count);
    }
  }

  @Override
  public void reset() {
    count = 0;
    mean = 0.0;
    m2 = 0.0;
  }

  @Override
  public boolean hasFinalResult() {
    return false;
  }

  @Override
  public TSDataType[] getIntermediateType() {
    return new TSDataType[] {TSDataType.TEXT};
  }

  @Override
  public TSDataType getFinalType() {
    return TSDataType.DOUBLE;
  }

  private void addIntInput(Column[] columns, BitMap bitmap, int lastIndex) {
    for (int i = 0; i <= lastIndex; i++) {
      if (bitmap != null && !bitmap.isMarked(i)) {
        continue;
      }
      if (!columns[1].isNull(i)) {
        int value = columns[1].getInt(i);
        count++;
        double delta = value - mean;
        mean += delta / count;
        m2 += delta * (value - mean);
      }
    }
  }

  private void addFloatInput(Column[] columns, BitMap bitmap, int lastIndex) {
    for (int i = 0; i <= lastIndex; i++) {
      if (bitmap != null && !bitmap.isMarked(i)) {
        continue;
      }
      if (!columns[1].isNull(i)) {
        float value = columns[1].getFloat(i);
        count++;
        double delta = value - mean;
        mean += delta / count;
        m2 += delta * (value - mean);
      }
    }
  }

  private void addDoubleInput(Column[] columns, BitMap bitmap, int lastIndex) {
    for (int i = 0; i <= lastIndex; i++) {
      if (bitmap != null && !bitmap.isMarked(i)) {
        continue;
      }
      if (!columns[1].isNull(i)) {
        double value = columns[1].getDouble(i);
        count++;
        double delta = value - mean;
        mean += delta / count;
        m2 += delta * (value - mean);
      }
    }
  }
}
