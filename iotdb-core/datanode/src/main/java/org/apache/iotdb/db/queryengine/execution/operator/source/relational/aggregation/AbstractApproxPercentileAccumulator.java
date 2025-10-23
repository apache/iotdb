/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation;

import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.approximate.TDigest;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.statistics.Statistics;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.RamUsageEstimator;
import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.apache.tsfile.write.UnSupportedDataTypeException;

import java.nio.ByteBuffer;

public abstract class AbstractApproxPercentileAccumulator implements TableAccumulator {
  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(ApproxPercentileAccumulator.class);

  protected final TDigest tDigest = new TDigest();
  protected final TSDataType seriesDataType;
  protected double percentage;

  AbstractApproxPercentileAccumulator(TSDataType seriesDataType) {
    this.seriesDataType = seriesDataType;
  }

  @Override
  public long getEstimatedSize() {
    return INSTANCE_SIZE + tDigest.getEstimatedSize();
  }

  @Override
  public TableAccumulator copy() {
    return new ApproxPercentileAccumulator(seriesDataType);
  }

  @Override
  public void addInput(Column[] arguments, AggregationMask mask) {
    if (arguments.length == 2) {
      percentage = arguments[1].getDouble(0);
    } else if (arguments.length == 3) {
      percentage = arguments[2].getDouble(0);
    } else {
      throw new SemanticException(
          String.format(
              "APPROX_PERCENTILE requires 2 or 3 arguments, but got %d", arguments.length));
    }
    switch (seriesDataType) {
      case INT32:
        addIntInput(arguments, mask);
        return;
      case INT64:
      case TIMESTAMP:
        addLongInput(arguments, mask);
        return;
      case FLOAT:
        addFloatInput(arguments, mask);
        return;
      case DOUBLE:
        addDoubleInput(arguments, mask);
        return;
      default:
        throw new UnSupportedDataTypeException(
            String.format(
                "Unsupported data type in APPROX_PERCENTILE Aggregation: %s", seriesDataType));
    }
  }

  @Override
  public void addIntermediate(Column argument) {
    for (int i = 0; i < argument.getPositionCount(); i++) {
      if (!argument.isNull(i)) {
        byte[] data = argument.getBinary(i).getValues();
        // Read percentage from the first 8 bytes and TDigest from the rest
        ByteBuffer buffer = ByteBuffer.wrap(data);
        this.percentage = ReadWriteIOUtils.readDouble(buffer);
        TDigest other = TDigest.fromByteBuffer(buffer);
        tDigest.add(other);
      }
    }
  }

  @Override
  public void evaluateIntermediate(ColumnBuilder columnBuilder) {
    int tDigestDataLength = tDigest.byteSize();
    // Create a buffer with space for percentage (8 bytes) + TDigest data
    ByteBuffer buffer = ByteBuffer.allocate(8 + tDigestDataLength);
    ReadWriteIOUtils.write(percentage, buffer);
    tDigest.toByteArray(buffer);
    columnBuilder.writeBinary(new Binary(buffer.array()));
  }

  @Override
  public void evaluateFinal(ColumnBuilder columnBuilder) {
    double result = tDigest.quantile(percentage);
    if (Double.isNaN(result)) {
      columnBuilder.appendNull();
      return;
    }
    switch (seriesDataType) {
      case INT32:
        columnBuilder.writeInt((int) result);
        break;
      case INT64:
      case TIMESTAMP:
        columnBuilder.writeLong((long) result);
        break;
      case FLOAT:
        columnBuilder.writeFloat((float) result);
        break;
      case DOUBLE:
        columnBuilder.writeDouble(result);
        break;
      default:
        throw new UnSupportedDataTypeException(
            String.format(
                "Unsupported data type in APPROX_PERCENTILE Aggregation: %s", seriesDataType));
    }
  }

  @Override
  public boolean hasFinalResult() {
    return false;
  }

  @Override
  public void addStatistics(Statistics[] statistics) {
    throw new UnsupportedOperationException(
        "ApproxPercentileAccumulator does not support statistics");
  }

  @Override
  public void reset() {
    tDigest.reset();
  }

  public abstract void addIntInput(Column[] arguments, AggregationMask mask);

  public abstract void addLongInput(Column[] arguments, AggregationMask mask);

  public abstract void addFloatInput(Column[] arguments, AggregationMask mask);

  public abstract void addDoubleInput(Column[] arguments, AggregationMask mask);

  public static double toDoubleExact(long value) {
    double doubleValue = (double) value;
    if ((long) doubleValue != value) {
      throw new SemanticException(
          String.format("no exact double representation for long: %s", value));
    }
    return value;
  }
}
