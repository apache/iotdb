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
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.write.UnSupportedDataTypeException;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.iotdb.commons.schema.table.TsTable.TIME_COLUMN_NAME;

public class TableInsertTabletStatementGenerator extends InsertTabletStatementGenerator {

  private final String databaseName;
  private final AtomicLong writtenCounter;
  private final int timeColumnIndex;
  private final List<TsTableColumnCategory> tsTableColumnCategories;

  public TableInsertTabletStatementGenerator(
      String databaseName,
      PartialPath targetTable,
      Map<String, InputLocation> measurementToInputLocationMap,
      Map<String, TSDataType> measurementToDataTypeMap,
      List<Type> sourceTypeConvertors,
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
        sourceTypeConvertors,
        isAligned,
        rowLimit);
    this.databaseName = databaseName;
    this.writtenCounter = new AtomicLong(0);
    this.tsTableColumnCategories = tsTableColumnCategories;
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
        Type sourceTypeConvertor = sourceTypeConvertors.get(i);

        // if the value is NULL
        if (valueColumn.isNull(lastReadIndex)) {
          // bit in bitMaps are marked as 1 (NULL) by default
          continue;
        }

        bitMaps[i].unmark(rowCount);
        switch (dataTypes[i]) {
          case INT32:
          case DATE:
            ((int[]) columns[i])[rowCount] = sourceTypeConvertor.getInt(valueColumn, lastReadIndex);
            break;
          case INT64:
          case TIMESTAMP:
            ((long[]) columns[i])[rowCount] =
                sourceTypeConvertor.getLong(valueColumn, lastReadIndex);
            break;
          case FLOAT:
            ((float[]) columns[i])[rowCount] =
                sourceTypeConvertor.getFloat(valueColumn, lastReadIndex);
            break;
          case DOUBLE:
            ((double[]) columns[i])[rowCount] =
                sourceTypeConvertor.getDouble(valueColumn, lastReadIndex);
            break;
          case BOOLEAN:
            ((boolean[]) columns[i])[rowCount] =
                sourceTypeConvertor.getBoolean(valueColumn, lastReadIndex);
            break;
          case TEXT:
          case BLOB:
          case STRING:
            ((Binary[]) columns[i])[rowCount] =
                sourceTypeConvertor.getBinary(valueColumn, lastReadIndex);
            break;
          default:
            throw new UnSupportedDataTypeException(
                String.format(
                    "data type %s is not supported when convert data at client",
                    valueColumn.getDataType()));
        }
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

  public InsertTabletStatement constructInsertTabletStatement() {
    InsertTabletStatement insertTabletStatement = super.constructInsertTabletStatement();
    insertTabletStatement.setDatabaseName(databaseName);
    insertTabletStatement.setWriteToTable(true);
    insertTabletStatement.setColumnCategories(
        tsTableColumnCategories.toArray(new TsTableColumnCategory[0]));
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
}
