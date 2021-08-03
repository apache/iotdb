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
import java.util.Arrays;
import java.util.List;

/** internallyConstructNewPlan -> collectRowRecord * N -> generateInsertTabletPlan */
public class InsertTabletPlanGenerator {

  private final String targetDevice;
  // the index of column in InsertTabletPlan -> the index of output column of query data set
  private final List<Integer> queryDataSetIndexes;
  // the index of column in InsertTabletPlan -> the measurement id of the column
  private final List<String> targetMeasurementIds;

  private final int tabletRowLimit;

  // the following fields are used to construct plan
  private int rowCount;
  private long[] times;
  private Object[] columns;
  private BitMap[] bitMaps;
  private TSDataType[] dataTypes;

  private int numberOfInitializedColumns;

  public InsertTabletPlanGenerator(String targetDevice, int tabletRowLimit) {
    this.targetDevice = targetDevice;
    queryDataSetIndexes = new ArrayList<>();
    targetMeasurementIds = new ArrayList<>();

    this.tabletRowLimit = tabletRowLimit;
  }

  public void collectTargetPathInformation(String targetMeasurementId, int queryDataSetIndex) {
    targetMeasurementIds.add(targetMeasurementId);
    queryDataSetIndexes.add(queryDataSetIndex);
  }

  public void internallyConstructNewPlan() {
    rowCount = 0;
    times = new long[tabletRowLimit];
    columns = new Object[targetMeasurementIds.size()];
    bitMaps = new BitMap[targetMeasurementIds.size()];
    for (int i = 0; i < bitMaps.length; ++i) {
      bitMaps[i] = new BitMap(tabletRowLimit);
      bitMaps[i].markAll();
    }
    dataTypes = new TSDataType[targetMeasurementIds.size()];

    numberOfInitializedColumns = 0;
  }

  public void collectRowRecord(RowRecord rowRecord) {
    if (numberOfInitializedColumns != columns.length) {
      List<Integer> initializedDataTypeIndexes = trySetDataTypes(rowRecord);
      tryInitColumns(initializedDataTypeIndexes);
      numberOfInitializedColumns += initializedDataTypeIndexes.size();
    }

    times[rowCount] = rowRecord.getTimestamp();

    for (int i = 0; i < columns.length; ++i) {
      Field field = rowRecord.getFields().get(queryDataSetIndexes.get(i));

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

  private List<Integer> trySetDataTypes(RowRecord rowRecord) {
    List<Integer> initializedDataTypeIndexes = new ArrayList<>();
    List<Field> fields = rowRecord.getFields();

    for (int i = 0; i < dataTypes.length; ++i) {
      // if the data type is already set
      if (dataTypes[i] != null) {
        continue;
      }

      // get the field index of the row record
      int queryDataSetIndex = queryDataSetIndexes.get(i);
      // if the field is not null
      if (fields.get(queryDataSetIndex) != null
          && fields.get(queryDataSetIndex).getDataType() != null) {
        // set the data type to the field type
        dataTypes[i] = fields.get(queryDataSetIndex).getDataType();
        initializedDataTypeIndexes.add(i);
      }
    }

    for (int i = 0; i < dataTypes.length; ++i) {
      if (dataTypes[i] == null && fields.get(i) != null && fields.get(i).getDataType() != null) {
        dataTypes[i] = fields.get(i).getDataType();
        initializedDataTypeIndexes.add(i);
      }
    }
    return initializedDataTypeIndexes;
  }

  private void tryInitColumns(List<Integer> initializedDataTypeIndexes) {
    for (int i : initializedDataTypeIndexes) {
      switch (dataTypes[i]) {
        case BOOLEAN:
          columns[i] = new boolean[tabletRowLimit];
          break;
        case INT32:
          columns[i] = new int[tabletRowLimit];
          break;
        case INT64:
          columns[i] = new long[tabletRowLimit];
          break;
        case FLOAT:
          columns[i] = new float[tabletRowLimit];
          break;
        case DOUBLE:
          columns[i] = new double[tabletRowLimit];
          break;
        case TEXT:
          columns[i] = new Binary[tabletRowLimit];
          break;
        default:
          throw new UnSupportedDataTypeException(
              String.format("Data type %s is not supported.", dataTypes[i]));
      }
    }
  }

  public InsertTabletPlan generateInsertTabletPlan() throws IllegalPathException {
    List<String> nonEmptyColumnNames = new ArrayList<>();

    int countOfNonEmptyColumns = 0;
    for (int i = 0; i < columns.length; ++i) {
      if (columns[i] == null) {
        continue;
      }

      nonEmptyColumnNames.add(targetMeasurementIds.get(i));
      columns[countOfNonEmptyColumns] = columns[i];
      bitMaps[countOfNonEmptyColumns] = bitMaps[i];
      dataTypes[countOfNonEmptyColumns] = dataTypes[i];

      ++countOfNonEmptyColumns;
    }

    InsertTabletPlan insertTabletPlan =
        new InsertTabletPlan(new PartialPath(targetDevice), nonEmptyColumnNames);
    insertTabletPlan.setAligned(false);
    insertTabletPlan.setRowCount(rowCount);

    if (countOfNonEmptyColumns != columns.length) {
      columns = Arrays.copyOf(columns, countOfNonEmptyColumns);
      bitMaps = Arrays.copyOf(bitMaps, countOfNonEmptyColumns);
      dataTypes = Arrays.copyOf(dataTypes, countOfNonEmptyColumns);
    }

    if (rowCount != tabletRowLimit) {
      times = Arrays.copyOf(times, rowCount);
      for (int i = 0; i < columns.length; ++i) {
        switch (dataTypes[i]) {
          case BOOLEAN:
            columns[i] = Arrays.copyOf((boolean[]) columns[i], rowCount);
            break;
          case INT32:
            columns[i] = Arrays.copyOf((int[]) columns[i], rowCount);
            break;
          case INT64:
            columns[i] = Arrays.copyOf((long[]) columns[i], rowCount);
            break;
          case FLOAT:
            columns[i] = Arrays.copyOf((float[]) columns[i], rowCount);
            break;
          case DOUBLE:
            columns[i] = Arrays.copyOf((double[]) columns[i], rowCount);
            break;
          case TEXT:
            columns[i] = Arrays.copyOf((Binary[]) columns[i], rowCount);
            break;
          default:
            throw new UnSupportedDataTypeException(
                String.format("Data type %s is not supported.", dataTypes[i]));
        }
      }
    }

    insertTabletPlan.setTimes(times);
    insertTabletPlan.setColumns(columns);
    insertTabletPlan.setBitMaps(bitMaps);
    insertTabletPlan.setDataTypes(dataTypes);

    return insertTabletPlan;
  }
}
