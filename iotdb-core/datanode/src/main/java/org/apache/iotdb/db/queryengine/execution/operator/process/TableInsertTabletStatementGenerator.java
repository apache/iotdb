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
import org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.InputLocation;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertTabletStatement;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.type.Type;
import org.apache.tsfile.read.common.type.TypeFactory;
import org.apache.tsfile.utils.RamUsageEstimator;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.iotdb.commons.schema.table.TsTable.TIME_COLUMN_NAME;

public class TableInsertTabletStatementGenerator extends InsertTabletStatementGenerator {
  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(TableInsertTabletStatementGenerator.class);

  private final String databaseName;
  private final AtomicLong writtenCounter;
  private final int timeColumnIndex;
  private final TsTableColumnCategory[] columnCategories;

  public TableInsertTabletStatementGenerator(
      String databaseName,
      PartialPath targetTable,
      Map<String, InputLocation> measurementToInputLocationMap,
      Map<String, TSDataType> measurementToDataTypeMap,
      List<TSDataType> inputColumnTypes,
      List<TsTableColumnCategory> tsTableColumnCategories,
      boolean isAligned,
      int rowLimit) {
    super(
        targetTable,
        measurementToDataTypeMap.keySet().toArray(new String[0]),
        measurementToDataTypeMap.values().toArray(new TSDataType[0]),
        measurementToInputLocationMap.entrySet().stream()
            .filter(entry -> !entry.getKey().equalsIgnoreCase(TIME_COLUMN_NAME))
            .map(Map.Entry::getValue)
            .toArray(InputLocation[]::new),
        inputColumnTypes.stream().map(TypeFactory::getType).toArray(Type[]::new),
        isAligned,
        rowLimit);
    this.databaseName = databaseName;
    this.writtenCounter = new AtomicLong(0);
    this.columnCategories = tsTableColumnCategories.toArray(new TsTableColumnCategory[0]);
    this.timeColumnIndex =
        measurementToInputLocationMap.get(TIME_COLUMN_NAME).getValueColumnIndex();
    this.reset();
  }

  public int processTsBlock(TsBlock tsBlock, int lastReadIndex) {
    while (lastReadIndex < tsBlock.getPositionCount()) {
      times[rowCount] = tsBlock.getValueColumns()[timeColumnIndex].getLong(lastReadIndex);

      for (int i = 0; i < measurements.length; ++i) {
        int valueColumnIndex = inputLocations[i].getValueColumnIndex();
        Column valueColumn = tsBlock.getValueColumns()[valueColumnIndex];

        // if the value is NULL
        if (valueColumn.isNull(lastReadIndex)) {
          // bit in bitMaps are marked as 1 (NULL) by default
          continue;
        }

        bitMaps[i].unmark(rowCount);
        processColumn(valueColumn, columns[i], dataTypes[i], typeConvertors[i], lastReadIndex);
      }

      writtenCounter.getAndIncrement();
      ++rowCount;
      ++lastReadIndex;
      if (rowCount == rowLimit) {
        break;
      }
    }
    return lastReadIndex;
  }

  @Override
  public InsertTabletStatement constructInsertTabletStatement() {
    InsertTabletStatement insertTabletStatement = super.constructInsertTabletStatement();
    insertTabletStatement.setDatabaseName(databaseName);
    insertTabletStatement.setWriteToTable(true);
    return insertTabletStatement;
  }

  @Override
  public long getWrittenCount() {
    return writtenCounter.get();
  }

  @Override
  public long getWrittenCount(String measurement) {
    throw new UnsupportedOperationException("getWrittenCount(measurement) is not supported");
  }

  @Override
  public long ramBytesUsed() {
    return INSTANCE_SIZE
        + ramBytesUsedByTimeAndColumns()
        + RamUsageEstimator.sizeOf(measurements)
        + sizeOf(dataTypes, TSDataType.class)
        + sizeOf(inputLocations, InputLocation.class)
        + sizeOf(typeConvertors, Type.class)
        + sizeOf(columnCategories, TsTableColumnCategory.class)
        + RamUsageEstimator.shallowSizeOfInstance(AtomicLong.class);
  }
}
