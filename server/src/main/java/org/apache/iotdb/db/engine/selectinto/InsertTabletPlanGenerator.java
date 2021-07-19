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

package org.apache.iotdb.db.engine.selectinto;

import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.physical.crud.InsertTabletPlan;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Field;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.BitMap;

import java.util.ArrayList;
import java.util.List;

public class InsertTabletPlanGenerator {

  private final String intoDevice;
  private final List<Integer> intoMeasurementIndexes;
  private final List<String> intoMeasurementIds;

  private final int fetchSize;

  private InsertTabletPlan insertTabletPlan;
  private int rowCount;
  private long[] times;
  private Object[] columns;
  private BitMap[] bitMaps;

  public InsertTabletPlanGenerator(String intoDevice, int fetchSize) {
    this.intoDevice = intoDevice;
    // column index of insertTabletPlan -> column index of queryDataSet (column index of intoPaths)
    intoMeasurementIndexes = new ArrayList<>();
    intoMeasurementIds = new ArrayList<>();

    this.fetchSize = fetchSize;
  }

  public void addMeasurementIdIndex(List<PartialPath> intoPaths, int intoMeasurementIndex) {
    intoMeasurementIndexes.add(intoMeasurementIndex);
    intoMeasurementIds.add(intoPaths.get(intoMeasurementIndex).getMeasurement());
  }

  public void internallyConstructNewPlan() throws IllegalPathException {
    insertTabletPlan = new InsertTabletPlan(new PartialPath(intoDevice), intoMeasurementIds);
    insertTabletPlan.setAligned(false);

    rowCount = 0;
    insertTabletPlan.setRowCount(rowCount);

    times = new long[fetchSize];
    insertTabletPlan.setTimes(times);

    columns = new Object[intoMeasurementIds.size()];
    insertTabletPlan.setColumns(columns);

    bitMaps = new BitMap[intoMeasurementIds.size()];
    for (int i = 0; i < bitMaps.length; ++i) {
      bitMaps[i] = new BitMap(fetchSize);
      bitMaps[i].markAll();
    }
    insertTabletPlan.setBitMaps(bitMaps);
  }

  public void collectRowRecord(RowRecord rowRecord) {
    if (rowCount == 0) {
      setDataTypes(rowRecord);
      initColumns();
    }

    times[rowCount] = rowRecord.getTimestamp();

    for (int i = 0; i < columns.length; ++i) {
      Field field = rowRecord.getFields().get(intoMeasurementIndexes.get(i));

      // if the field is NULL
      if (field == null || field.getDataType() == null) {
        // bit in bitMaps are marked as 1 (NULL) by default
        continue;
      }

      bitMaps[i].unmark(rowCount);
      switch (field.getDataType()) {
        case INT32:
          ((int[]) columns[i])[rowCount] = field.getIntV();
          break;
        case INT64:
          ((long[]) columns[i])[rowCount] = field.getLongV();
          break;
        case FLOAT:
          ((float[]) columns[i])[rowCount] = field.getFloatV();
          break;
        case DOUBLE:
          ((double[]) columns[i])[rowCount] = field.getDoubleV();
          break;
        case BOOLEAN:
          ((boolean[]) columns[i])[rowCount] = field.getBoolV();
          break;
        case TEXT:
          ((Binary[]) columns[i])[rowCount] = field.getBinaryV();
          break;
        default:
          throw new UnSupportedDataTypeException(
              String.format(
                  "data type %s is not supported when convert data at client",
                  field.getDataType()));
      }
    }

    ++rowCount;
  }

  private void setDataTypes(RowRecord rowRecord) {
    List<TSDataType> dataTypes = new ArrayList<>();
    for (Field field : rowRecord.getFields()) {
      dataTypes.add(field.getDataType());
    }
    insertTabletPlan.setDataTypes(dataTypes.toArray(new TSDataType[0]));
  }

  private void initColumns() {
    final TSDataType[] dataTypes = insertTabletPlan.getDataTypes();
    for (int i = 0; i < dataTypes.length; ++i) {
      switch (dataTypes[i]) {
        case BOOLEAN:
          columns[i] = new boolean[fetchSize];
          break;
        case INT32:
          columns[i] = new int[fetchSize];
          break;
        case INT64:
          columns[i] = new long[fetchSize];
          break;
        case FLOAT:
          columns[i] = new float[fetchSize];
          break;
        case DOUBLE:
          columns[i] = new double[fetchSize];
          break;
        case TEXT:
          columns[i] = new Binary[fetchSize];
          break;
        default:
          throw new UnSupportedDataTypeException(
              String.format(
                  "data type %s is not supported when convert data at client", dataTypes[i]));
      }
    }
  }

  public InsertTabletPlan getInsertTabletPlan() {
    insertTabletPlan.setRowCount(rowCount);
    return insertTabletPlan;
  }
}
