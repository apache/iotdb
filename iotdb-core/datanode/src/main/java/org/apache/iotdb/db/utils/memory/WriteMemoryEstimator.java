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

package org.apache.iotdb.db.utils.memory;

import org.apache.iotdb.db.pipe.resource.memory.InsertNodeMemoryEstimator;
import org.apache.iotdb.db.queryengine.plan.statement.Statement;
import org.apache.iotdb.db.queryengine.plan.statement.StatementType;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertMultiTabletsStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertRowStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertRowsOfOneDeviceStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertRowsStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertTabletStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.LoadTsFileStatement;
import org.apache.iotdb.db.queryengine.plan.statement.pipe.PipeEnrichedStatement;
import org.apache.iotdb.db.utils.TypeInferenceUtils;

import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.BitMap;
import org.apache.tsfile.write.schema.MeasurementSchema;

/** Estimates write_memory charge for insert / load statements before quota acquire. */
public final class WriteMemoryEstimator {

  private WriteMemoryEstimator() {}

  /** Unwrap PIPE_ENRICHED and dispatch by StatementType; returns 0 for non-write types. */
  public static long estimate(Statement s) {
    if (s.getType() == StatementType.PIPE_ENRICHED) {
      s = ((PipeEnrichedStatement) s).getInnerStatement();
    }
    switch (s.getType()) {
      case INSERT:
        if (s instanceof InsertStatement) {
          long size = 0;
          InsertStatement insertStatement = (InsertStatement) s;
          for (Object[] values : insertStatement.getValuesList()) {
            size += calculationWrite(values);
          }
          return size;
        }
        if (s instanceof InsertRowStatement) {
          return calculationWrite(((InsertRowStatement) s).getValues());
        }
        return 0;
      case BATCH_INSERT:
        return estimateTablet((InsertTabletStatement) s);
      case BATCH_INSERT_ONE_DEVICE:
        long oneDeviceSize = 0;
        for (InsertRowStatement row :
            ((InsertRowsOfOneDeviceStatement) s).getInsertRowStatementList()) {
          oneDeviceSize += calculationWrite(row.getValues());
        }
        return oneDeviceSize;
      case BATCH_INSERT_ROWS:
        long rowsSize = 0;
        for (InsertRowStatement row : ((InsertRowsStatement) s).getInsertRowStatementList()) {
          rowsSize += calculationWrite(row.getValues());
        }
        return rowsSize;
      case MULTI_BATCH_INSERT:
        if (s instanceof LoadTsFileStatement) {
          long loadSize = 0;
          LoadTsFileStatement load = (LoadTsFileStatement) s;
          for (int i = 0; i < load.getResources().size(); i++) {
            loadSize += load.getResources().get(i).getTsFileSize();
          }
          return loadSize;
        }
        if (s instanceof InsertMultiTabletsStatement) {
          long tabletSize = 0;
          InsertMultiTabletsStatement multi = (InsertMultiTabletsStatement) s;
          for (InsertTabletStatement tablet : multi.getInsertTabletStatementList()) {
            tabletSize += estimateTablet(tablet);
          }
          return tabletSize;
        }
        return 0;
      default:
        return 0;
    }
  }

  private static long calculationWrite(Object[] values) {
    long size = 0;
    for (Object value : values) {
      size += estimateValue(value);
    }
    return size;
  }

  /** Sum fixed-/variable-width columns and bitmaps for one tablet. */
  private static long estimateTablet(InsertTabletStatement tablet) {
    MeasurementSchema[] measurementSchemas = tablet.getMeasurementSchemas();
    Object[] columns = tablet.getColumns();
    if (measurementSchemas != null && columns != null) {
      long size = InsertNodeMemoryEstimator.sizeOfColumns(columns, measurementSchemas);
      BitMap[] bitMaps = tablet.getBitMaps();
      if (bitMaps != null) {
        for (BitMap bitMap : bitMaps) {
          if (bitMap != null) {
            size += bitMap.getSize();
          }
        }
      }
      return size;
    }
    long size = 0;
    int rowCount = tablet.getRowCount();
    TSDataType[] dataTypes = tablet.getDataTypes();
    if (dataTypes != null) {
      for (int i = 0; i < dataTypes.length; i++) {
        TSDataType dataType = dataTypes[i];
        if (dataType != null) {
          Object column = columns != null && i < columns.length ? columns[i] : null;
          if (isVariableWidth(dataType) && column != null) {
            size += estimateVariableColumn(column, rowCount);
          } else {
            size += (long) dataType.getDataTypeSize() * rowCount;
          }
        }
      }
    }
    BitMap[] bitMaps = tablet.getBitMaps();
    if (bitMaps != null) {
      for (BitMap bitMap : bitMaps) {
        if (bitMap != null) {
          size += bitMap.getSize();
        }
      }
    }
    return size;
  }

  private static long estimateValue(Object value) {
    if (value == null) {
      return 0;
    }
    if (value instanceof Binary) {
      return ((Binary) value).getLength();
    }
    if (value instanceof String) {
      return ((String) value).getBytes(TSFileConfig.STRING_CHARSET).length;
    }
    if (value instanceof byte[]) {
      return ((byte[]) value).length;
    }
    TSDataType dataType = TypeInferenceUtils.getPredictedDataType(value, true);
    return dataType == null ? 0 : dataType.getDataTypeSize();
  }

  private static boolean isVariableWidth(TSDataType dataType) {
    return dataType == TSDataType.TEXT
        || dataType == TSDataType.STRING
        || dataType == TSDataType.BLOB;
  }

  private static long estimateVariableColumn(Object column, int rowCount) {
    long size = 0;
    if (column instanceof Binary[]) {
      Binary[] values = (Binary[]) column;
      for (int i = 0; i < Math.min(values.length, rowCount); i++) {
        size += estimateValue(values[i]);
      }
    } else if (column instanceof String[]) {
      String[] values = (String[]) column;
      for (int i = 0; i < Math.min(values.length, rowCount); i++) {
        size += estimateValue(values[i]);
      }
    } else if (column instanceof byte[][]) {
      byte[][] values = (byte[][]) column;
      for (int i = 0; i < Math.min(values.length, rowCount); i++) {
        size += estimateValue(values[i]);
      }
    } else if (column instanceof Object[]) {
      Object[] values = (Object[]) column;
      for (int i = 0; i < Math.min(values.length, rowCount); i++) {
        size += estimateValue(values[i]);
      }
    }
    return size;
  }
}
