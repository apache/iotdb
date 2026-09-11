/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iotdb.rest.protocol.table.v1.handler;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.queryengine.utils.TimestampPrecisionUtils;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory;
import org.apache.iotdb.db.exception.WriteProcessRejectException;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertTabletStatement;
import org.apache.iotdb.rest.protocol.table.v1.model.InsertTabletRequest;
import org.apache.iotdb.rest.protocol.utils.TypeServices;

import org.apache.tsfile.enums.ColumnCategory;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.type.Type;
import org.apache.tsfile.utils.BitMap;

import java.util.List;
import java.util.Locale;

public class StatementConstructionHandler {

  private StatementConstructionHandler() {}

  public static InsertTabletStatement constructInsertTabletStatement(
      InsertTabletRequest insertTabletReq)
      throws IllegalPathException, WriteProcessRejectException {
    InsertTabletStatement insertStatement = new InsertTabletStatement();
    insertStatement.setDevicePath(new PartialPath(insertTabletReq.getTable(), false));
    insertStatement.setMeasurements(insertTabletReq.getColumnNames().toArray(new String[0]));
    long[] timestamps =
        insertTabletReq.getTimestamps().stream().mapToLong(Long::longValue).toArray();
    if (timestamps.length != 0) {
      TimestampPrecisionUtils.checkTimestampPrecision(timestamps[timestamps.length - 1]);
    }
    insertStatement.setTimes(timestamps);
    int columnSize = insertTabletReq.getColumnNames().size();
    int rowSize = insertTabletReq.getTimestamps().size();
    List<List<Object>> rawData = insertTabletReq.getValues();
    Object[] columns = new Object[columnSize];
    BitMap[] bitMaps = new BitMap[columnSize];
    List<String> rawDataType = insertTabletReq.getDataTypes();
    TSDataType[] dataTypes = new TSDataType[columnSize];

    for (int i = 0; i < columnSize; i++) {
      dataTypes[i] = TSDataType.valueOf(rawDataType.get(i).toUpperCase(Locale.ROOT));
    }

    for (int columnIndex = 0; columnIndex < columnSize; columnIndex++) {
      bitMaps[columnIndex] = new BitMap(rowSize);
      final int column = columnIndex;
      columns[columnIndex] =
          TypeServices.INSERT_TABLET_COLUMN_WRITER_SERVICE
              .call(Type.fromTsDataType(dataTypes[columnIndex]))
              .write(rowIndex -> rawData.get(rowIndex).get(column), rowSize, bitMaps[columnIndex]);
    }
    insertStatement.setColumns(columns);
    insertStatement.setBitMaps(bitMaps);
    insertStatement.setRowCount(rowSize);
    insertStatement.setDataTypes(dataTypes);
    insertStatement.setAligned(false);
    insertStatement.setWriteToTable(true);
    TsTableColumnCategory[] columnCategories =
        new TsTableColumnCategory[insertTabletReq.getColumnCategories().size()];
    for (int i = 0; i < columnCategories.length; i++) {
      columnCategories[i] =
          TsTableColumnCategory.fromTsFileColumnCategory(
              ColumnCategory.valueOf(insertTabletReq.getColumnCategories().get(i)));
    }
    insertStatement.setColumnCategories(columnCategories);

    return insertStatement;
  }
}
