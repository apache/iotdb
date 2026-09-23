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

package org.apache.iotdb.rest.protocol.v1.handler;

import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.queryengine.utils.TimestampPrecisionUtils;
import org.apache.iotdb.commons.utils.PathUtils;
import org.apache.iotdb.db.exception.WriteProcessRejectException;
import org.apache.iotdb.db.queryengine.plan.analyze.cache.schema.DataNodeDevicePathCache;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertTabletStatement;
import org.apache.iotdb.rest.protocol.utils.TypeServices;
import org.apache.iotdb.rest.protocol.v1.model.InsertTabletRequest;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.type.Type;
import org.apache.tsfile.utils.BitMap;

import java.util.List;
import java.util.Locale;

public class StatementConstructionHandler {
  private StatementConstructionHandler() {}

  public static InsertTabletStatement constructInsertTabletStatement(
      InsertTabletRequest insertTabletRequest)
      throws MetadataException, WriteProcessRejectException {
    TimestampPrecisionUtils.checkTimestampPrecision(
        insertTabletRequest.getTimestamps().get(insertTabletRequest.getTimestamps().size() - 1));
    // construct insert statement
    InsertTabletStatement insertStatement = new InsertTabletStatement();
    insertStatement.setDevicePath(
        DataNodeDevicePathCache.getInstance().getPartialPath(insertTabletRequest.getDeviceId()));
    // TODO: remove the check for table model
    insertStatement.setMeasurements(
        PathUtils.checkIsLegalSingleMeasurementsAndUpdate(insertTabletRequest.getMeasurements())
            .toArray(new String[0]));
    List<List<Object>> rawData = insertTabletRequest.getValues();
    List<String> rawDataType = insertTabletRequest.getDataTypes();

    int rowSize = insertTabletRequest.getTimestamps().size();
    int columnSize = rawDataType.size();

    Object[] columns = new Object[columnSize];
    BitMap[] bitMaps = new BitMap[columnSize];
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
              .write(rowIndex -> rawData.get(column).get(rowIndex), rowSize, bitMaps[columnIndex]);
    }

    insertStatement.setTimes(
        insertTabletRequest.getTimestamps().stream().mapToLong(Long::longValue).toArray());
    insertStatement.setColumns(columns);
    insertStatement.setBitMaps(bitMaps);
    insertStatement.setRowCount(insertTabletRequest.getTimestamps().size());
    insertStatement.setDataTypes(dataTypes);
    insertStatement.setAligned(insertTabletRequest.getIsAligned());
    return insertStatement;
  }
}
