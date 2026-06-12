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

package org.apache.iotdb.calc.execution.operator.source.relational.aggregation;

import org.apache.iotdb.calc.execution.operator.source.relational.Percentile;
import org.apache.iotdb.calc.plan.planner.memory.MemoryReservationManager;
import org.apache.iotdb.commons.exception.SemanticException;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.statistics.Statistics;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.RamUsageEstimator;
import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.apache.tsfile.write.UnSupportedDataTypeException;

import java.nio.ByteBuffer;

public class PercentileAccumulator implements TableAccumulator {
  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(PercentileAccumulator.class);

  private final TSDataType seriesDataType;
  private Percentile percentile = new Percentile();
  // percentage is a query-level constant; it is read once from the first input/intermediate and
  // kept fixed afterwards, so it never gets reset to 0 by a later all-null batch.
  private double percentage;
  private boolean percentageInitialized;

  private final MemoryReservationManager memoryReservationManager;
  private long previousPercentileSize;

  public PercentileAccumulator(
      TSDataType seriesDataType, MemoryReservationManager memoryReservationManager) {
    this.seriesDataType = seriesDataType;
    this.memoryReservationManager = memoryReservationManager;
    updateMemoryReservation();
  }

  @Override
  public long getEstimatedSize() {
    return INSTANCE_SIZE + percentile.getEstimatedSize();
  }

  @Override
  public TableAccumulator copy() {
    return new PercentileAccumulator(seriesDataType, memoryReservationManager);
  }

  @Override
  public void addInput(Column[] arguments, AggregationMask mask) {
    if (arguments.length != 2) {
      throw new SemanticException(
          String.format("PERCENTILE requires 2 arguments, but got %d", arguments.length));
    }
    if (!percentageInitialized) {
      percentage = arguments[1].getDouble(0);
      percentageInitialized = true;
    }
    switch (seriesDataType) {
      case INT32:
        addIntInput(arguments[0], mask);
        break;
      case INT64:
      case TIMESTAMP:
        addLongInput(arguments[0], mask);
        break;
      case FLOAT:
        addFloatInput(arguments[0], mask);
        break;
      case DOUBLE:
        addDoubleInput(arguments[0], mask);
        break;
      default:
        throw new UnSupportedDataTypeException(
            String.format("Unsupported data type in Percentile Aggregation: %s", seriesDataType));
    }
    updateMemoryReservation();
  }

  @Override
  public void addIntermediate(Column argument) {
    for (int i = 0; i < argument.getPositionCount(); i++) {
      if (!argument.isNull(i)) {
        byte[] data = argument.getBinary(i).getValues();
        ByteBuffer buffer = ByteBuffer.wrap(data);
        // Always consume the leading 8 bytes so the buffer position is correct for deserialize,
        // but only keep the percentage once: every partial carries the same query-level constant.
        double serializedPercentage = ReadWriteIOUtils.readDouble(buffer);
        if (!percentageInitialized) {
          percentage = serializedPercentage;
          percentageInitialized = true;
        }
        percentile.merge(Percentile.deserialize(buffer));
      }
    }
    updateMemoryReservation();
  }

  @Override
  public void evaluateIntermediate(ColumnBuilder columnBuilder) {
    int percentileDataLength = percentile.getSerializedSize();
    // Use long arithmetic to avoid integer overflow
    ByteBuffer buffer = ByteBuffer.allocate(Math.toIntExact(8L + percentileDataLength));
    ReadWriteIOUtils.write(percentage, buffer);
    percentile.serialize(buffer);
    columnBuilder.writeBinary(new Binary(buffer.array()));
  }

  @Override
  public void evaluateFinal(ColumnBuilder columnBuilder) {
    double result = percentile.getPercentile(percentage);
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
            String.format("Unsupported data type in PERCENTILE Aggregation: %s", seriesDataType));
    }
  }

  @Override
  public boolean hasFinalResult() {
    return false;
  }

  @Override
  public void addStatistics(Statistics[] statistics) {
    throw new UnsupportedOperationException("PercentileAccumulator does not support statistics");
  }

  @Override
  public void reset() {
    percentile = new Percentile();
    percentageInitialized = false;
    updateMemoryReservation();
  }

  private void updateMemoryReservation() {
    long currentSize = percentile.getEstimatedSize();
    long delta = currentSize - previousPercentileSize;
    if (delta > 0) {
      memoryReservationManager.reserveMemoryCumulatively(delta);
    } else if (delta < 0) {
      memoryReservationManager.releaseMemoryCumulatively(-delta);
    }
    previousPercentileSize = currentSize;
  }

  private void addIntInput(Column column, AggregationMask mask) {
    int positionCount = mask.getSelectedPositionCount();

    if (mask.isSelectAll()) {
      for (int i = 0; i < positionCount; i++) {
        if (!column.isNull(i)) {
          percentile.addValue(column.getInt(i));
        }
      }
    } else {
      int[] selectedPositions = mask.getSelectedPositions();
      int position;
      for (int i = 0; i < positionCount; i++) {
        position = selectedPositions[i];
        if (!column.isNull(position)) {
          percentile.addValue(column.getInt(position));
        }
      }
    }
  }

  private void addLongInput(Column column, AggregationMask mask) {
    int positionCount = mask.getSelectedPositionCount();

    if (mask.isSelectAll()) {
      for (int i = 0; i < positionCount; i++) {
        if (!column.isNull(i)) {
          percentile.addValue(column.getLong(i));
        }
      }
    } else {
      int[] selectedPositions = mask.getSelectedPositions();
      int position;
      for (int i = 0; i < positionCount; i++) {
        position = selectedPositions[i];
        if (!column.isNull(position)) {
          percentile.addValue(column.getLong(position));
        }
      }
    }
  }

  private void addFloatInput(Column column, AggregationMask mask) {
    int positionCount = mask.getSelectedPositionCount();

    if (mask.isSelectAll()) {
      for (int i = 0; i < positionCount; i++) {
        if (!column.isNull(i)) {
          percentile.addValue(column.getFloat(i));
        }
      }
    } else {
      int[] selectedPositions = mask.getSelectedPositions();
      int position;
      for (int i = 0; i < positionCount; i++) {
        position = selectedPositions[i];
        if (!column.isNull(position)) {
          percentile.addValue(column.getFloat(position));
        }
      }
    }
  }

  private void addDoubleInput(Column column, AggregationMask mask) {
    int positionCount = mask.getSelectedPositionCount();

    if (mask.isSelectAll()) {
      for (int i = 0; i < positionCount; i++) {
        if (!column.isNull(i)) {
          percentile.addValue(column.getDouble(i));
        }
      }
    } else {
      int[] selectedPositions = mask.getSelectedPositions();
      int position;
      for (int i = 0; i < positionCount; i++) {
        position = selectedPositions[i];
        if (!column.isNull(position)) {
          percentile.addValue(column.getDouble(position));
        }
      }
    }
  }
}
