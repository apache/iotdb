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

package org.apache.iotdb.db.queryengine.execution.operator.source.relational;

import org.apache.iotdb.db.queryengine.plan.relational.metadata.ColumnSchema;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.DeviceEntry;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.column.BinaryColumn;
import org.apache.tsfile.read.common.block.column.RunLengthEncodedColumn;
import org.apache.tsfile.utils.Binary;

import java.util.List;
import java.util.Optional;

import static org.apache.iotdb.db.queryengine.execution.operator.source.relational.AbstractTableScanOperator.TIME_COLUMN_TEMPLATE;

public class MeasurementToTableViewAdaptorUtils {
  private MeasurementToTableViewAdaptorUtils() {}

  public static TsBlock toTableBlock(
      TsBlock measurementDataBlock,
      int[] columnsIndexArray,
      List<ColumnSchema> columnSchemas,
      DeviceEntry deviceEntry,
      GetNthIdColumnValueFunc func) {
    if (measurementDataBlock == null) {
      return null;
    }
    int positionCount = measurementDataBlock.getPositionCount();
    Column[] valueColumns = new Column[columnsIndexArray.length];
    for (int i = 0; i < columnsIndexArray.length; i++) {
      switch (columnSchemas.get(i).getColumnCategory()) {
        case TAG:
          String idColumnValue = func.getNthIdColumnValue(columnsIndexArray[i]);

          valueColumns[i] =
              getIdOrAttributeValueColumn(
                  idColumnValue == null
                      ? null
                      : new Binary(idColumnValue, TSFileConfig.STRING_CHARSET),
                  positionCount);
          break;
        case ATTRIBUTE:
          Binary attributeColumnValue =
              deviceEntry.getAttributeColumnValues()[columnsIndexArray[i]];
          valueColumns[i] = getIdOrAttributeValueColumn(attributeColumnValue, positionCount);
          break;
        case FIELD:
          valueColumns[i] = measurementDataBlock.getColumn(columnsIndexArray[i]);
          break;
        case TIME:
          valueColumns[i] = measurementDataBlock.getTimeColumn();
          break;
        default:
          throw new IllegalArgumentException(
              "Unexpected column category: " + columnSchemas.get(i).getColumnCategory());
      }
    }
    return new TsBlock(
        positionCount,
        new RunLengthEncodedColumn(TIME_COLUMN_TEMPLATE, positionCount),
        valueColumns);
  }

  private static RunLengthEncodedColumn getIdOrAttributeValueColumn(
      Binary value, int positionCount) {
    if (value == null) {
      return new RunLengthEncodedColumn(
          new BinaryColumn(
              1, Optional.of(new boolean[] {true}), new org.apache.tsfile.utils.Binary[] {null}),
          positionCount);
    } else {
      return new RunLengthEncodedColumn(
          new BinaryColumn(1, Optional.empty(), new org.apache.tsfile.utils.Binary[] {value}),
          positionCount);
    }
  }

  @FunctionalInterface
  public interface GetNthIdColumnValueFunc {
    String getNthIdColumnValue(int idColumnIndex);
  }
}
