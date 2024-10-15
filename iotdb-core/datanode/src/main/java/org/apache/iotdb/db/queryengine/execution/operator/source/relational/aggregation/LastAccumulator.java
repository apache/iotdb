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

package org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.statistics.Statistics;
import org.apache.tsfile.utils.RamUsageEstimator;
import org.apache.tsfile.utils.TsPrimitiveType;
import org.apache.tsfile.write.UnSupportedDataTypeException;

public class LastAccumulator implements TableAccumulator {

  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(LastAccumulator.class);
  protected final TSDataType seriesDataType;
  protected TsPrimitiveType lastValue;
  protected long maxTime = Long.MIN_VALUE;
  protected boolean initResult = false;

  public LastAccumulator(TSDataType seriesDataType) {
    this.seriesDataType = seriesDataType;
    lastValue = TsPrimitiveType.getByType(seriesDataType);
  }

  @Override
  public long getEstimatedSize() {
    return INSTANCE_SIZE;
  }

  @Override
  public TableAccumulator copy() {
    return new LastAccumulator(seriesDataType);
  }

  @Override
  public void addInput(Column[] arguments) {
    switch (seriesDataType) {
      case INT32:
      case DATE:
        addIntInput(arguments);
        return;
      case INT64:
      case TIMESTAMP:
        addLongInput(arguments);
        return;
      case FLOAT:
        addFloatInput(arguments);
        return;
      case DOUBLE:
        addDoubleInput(arguments);
        return;
      case TEXT:
      case STRING:
      case BLOB:
        addBinaryInput(arguments);
        return;
      case BOOLEAN:
        addBooleanInput(arguments);
        return;
      default:
        throw new UnSupportedDataTypeException(
            String.format("Unsupported data type in LastValue: %s", seriesDataType));
    }
  }

  @Override
  public void addIntermediate(Column argument) {}

  @Override
  public void evaluateIntermediate(ColumnBuilder columnBuilder) {}

  @Override
  public void evaluateFinal(ColumnBuilder columnBuilder) {}

  @Override
  public boolean hasFinalResult() {
    return false;
  }

  @Override
  public void addStatistics(Statistics statistics) {}

  @Override
  public void reset() {
    initResult = false;
    this.maxTime = Long.MIN_VALUE;
    this.lastValue.reset();
  }
}
