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

package org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.grouped;

import org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.AggregationMask;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.approximate.TDigest;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.grouped.array.TDigestBigArray;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.RamUsageEstimator;
import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.apache.tsfile.write.UnSupportedDataTypeException;

import java.nio.ByteBuffer;

public abstract class AbstractGroupedApproxPercentileAccumulator implements GroupedAccumulator {

  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(GroupedApproxPercentileAccumulator.class);
  protected final TSDataType seriesDataType;
  protected double percentage;
  protected final TDigestBigArray array = new TDigestBigArray();

  AbstractGroupedApproxPercentileAccumulator(TSDataType seriesDataType) {
    this.seriesDataType = seriesDataType;
  }

  @Override
  public long getEstimatedSize() {
    return INSTANCE_SIZE + array.sizeOf();
  }

  @Override
  public void setGroupCount(long groupCount) {
    array.ensureCapacity(groupCount);
  }

  @Override
  public void addInput(int[] groupIds, Column[] arguments, AggregationMask mask) {
    if (arguments.length == 2) {
      percentage = arguments[1].getDouble(0);
    } else if (arguments.length == 3) {
      percentage = arguments[2].getDouble(0);
    } else {
      throw new IllegalArgumentException(
          String.format(
              "APPROX_PERCENTILE requires 2 or 3 arguments, but got %d", arguments.length));
    }

    switch (seriesDataType) {
      case INT32:
        addIntInput(groupIds, arguments, mask);
        break;
      case INT64:
      case TIMESTAMP:
        addLongInput(groupIds, arguments, mask);
        break;
      case FLOAT:
        addFloatInput(groupIds, arguments, mask);
        break;
      case DOUBLE:
        addDoubleInput(groupIds, arguments, mask);
        break;
      default:
        throw new UnSupportedDataTypeException(
            String.format(
                "Unsupported data type in APPROX_PERCENTILE Aggregation: %s", seriesDataType));
    }
  }

  @Override
  public void addIntermediate(int[] groupIds, Column argument) {
    for (int i = 0; i < groupIds.length; i++) {
      int groupId = groupIds[i];
      if (!argument.isNull(i)) {
        byte[] data = argument.getBinary(i).getValues();
        ByteBuffer buffer = ByteBuffer.wrap(data);
        this.percentage = ReadWriteIOUtils.readDouble(buffer);
        TDigest other = TDigest.fromByteBuffer(buffer);
        array.get(groupId).add(other);
      }
    }
  }

  @Override
  public void evaluateIntermediate(int groupId, ColumnBuilder columnBuilder) {
    TDigest tDigest = array.get(groupId);
    int tDigestDataLength = tDigest.byteSize();
    ByteBuffer buffer = ByteBuffer.allocate(8 + tDigestDataLength);
    ReadWriteIOUtils.write(percentage, buffer);
    tDigest.toByteArray(buffer);
    columnBuilder.writeBinary(new Binary(buffer.array()));
  }

  @Override
  public void evaluateFinal(int groupId, ColumnBuilder columnBuilder) {
    TDigest tDigest = array.get(groupId);
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
  public void prepareFinal() {}

  @Override
  public void reset() {
    array.reset();
  }

  public abstract void addIntInput(int[] groupIds, Column[] arguments, AggregationMask mask);

  public abstract void addLongInput(int[] groupIds, Column[] arguments, AggregationMask mask);

  public abstract void addFloatInput(int[] groupIds, Column[] arguments, AggregationMask mask);

  public abstract void addDoubleInput(int[] groupIds, Column[] arguments, AggregationMask mask);
}
