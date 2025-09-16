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

package org.apache.iotdb.db.queryengine.execution.operator.process;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.InputLocation;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.type.Type;
import org.apache.tsfile.read.common.type.TypeFactory;
import org.apache.tsfile.utils.RamUsageEstimator;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class TreeInsertTabletStatementGenerator extends InsertTabletStatementGenerator {
  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(TreeInsertTabletStatementGenerator.class);

  private final Map<String, AtomicLong> writtenCounter;

  public TreeInsertTabletStatementGenerator(
      PartialPath targetDevice,
      Map<String, InputLocation> measurementToInputLocationMap,
      Map<String, TSDataType> measurementToDataTypeMap,
      List<TSDataType> inputColumnTypes,
      boolean isAligned,
      int rowLimit) {
    super(
        targetDevice,
        measurementToInputLocationMap.keySet().toArray(new String[0]),
        measurementToDataTypeMap.values().toArray(new TSDataType[0]),
        measurementToInputLocationMap.values().toArray(new InputLocation[0]),
        inputColumnTypes.stream().map(TypeFactory::getType).toArray(Type[]::new),
        isAligned,
        rowLimit);
    this.writtenCounter = new HashMap<>();
    for (String measurement : measurements) {
      writtenCounter.put(measurement, new AtomicLong(0));
    }
    this.reset();
  }

  public int processTsBlock(TsBlock tsBlock, int lastReadIndex) {
    while (lastReadIndex < tsBlock.getPositionCount()) {

      times[rowCount] = tsBlock.getTimeByIndex(lastReadIndex);

      for (int i = 0; i < measurements.length; ++i) {
        int valueColumnIndex = inputLocations[i].getValueColumnIndex();
        Column valueColumn = tsBlock.getValueColumns()[valueColumnIndex];

        // if the value is NULL
        if (valueColumn.isNull(lastReadIndex)) {
          // bit in bitMaps are marked as 1 (NULL) by default
          continue;
        }

        bitMaps[i].unmark(rowCount);
        writtenCounter.get(measurements[i]).getAndIncrement();
        processColumn(
            valueColumn, columns[i], dataTypes[i], typeConvertors[valueColumnIndex], lastReadIndex);
      }

      ++rowCount;
      ++lastReadIndex;
      if (rowCount == rowLimit) {
        break;
      }
    }
    return lastReadIndex;
  }

  @Override
  public long getWrittenCount() {
    throw new UnsupportedOperationException("getWrittenCount() is not supported");
  }

  @Override
  public long getWrittenCount(String measurement) {
    if (!writtenCounter.containsKey(measurement)) {
      return -1;
    }
    return writtenCounter.get(measurement).get();
  }

  @Override
  public long ramBytesUsed() {
    return INSTANCE_SIZE
        + ramBytesUsedByTimeAndColumns()
        + RamUsageEstimator.sizeOf(measurements)
        + sizeOf(dataTypes, TSDataType.class)
        + sizeOf(inputLocations, InputLocation.class)
        + sizeOf(typeConvertors, Type.class)
        + RamUsageEstimator.sizeOfMap(
            writtenCounter, RamUsageEstimator.shallowSizeOfInstance(AtomicLong.class));
  }
}
