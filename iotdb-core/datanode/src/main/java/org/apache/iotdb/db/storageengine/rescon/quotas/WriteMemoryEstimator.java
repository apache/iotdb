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

package org.apache.iotdb.db.storageengine.rescon.quotas;

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

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.BitMap;

public final class WriteMemoryEstimator {

  private WriteMemoryEstimator() {}

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
      TSDataType dataType = TypeInferenceUtils.getPredictedDataType(value, true);
      if (dataType != null) {
        size += dataType.getDataTypeSize();
      }
    }
    return size;
  }

  private static long estimateTablet(InsertTabletStatement tablet) {
    long size = 0;
    int rowCount = tablet.getRowCount();
    TSDataType[] dataTypes = tablet.getDataTypes();
    if (dataTypes != null) {
      for (TSDataType dataType : dataTypes) {
        if (dataType != null) {
          size += (long) dataType.getDataTypeSize() * rowCount;
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
}
