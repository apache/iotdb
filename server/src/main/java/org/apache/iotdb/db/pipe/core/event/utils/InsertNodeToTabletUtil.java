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

package org.apache.iotdb.db.pipe.core.event.utils;

import org.apache.iotdb.db.mpp.plan.planner.plan.node.write.InsertNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.write.InsertRowNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.write.InsertTabletNode;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.utils.BitMap;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class InsertNodeToTabletUtil {
  private final InsertNode insertNode;
  private long[] timestamps;
  private List<TSDataType> dataTypeList;
  private final List<Path> columnNameList;
  private String deviceId;
  private Object[][] rowRecords;
  private BitMap[] bitMaps;
  private int rowSize;
  private int columnSize;

  public InsertNodeToTabletUtil(InsertNode insertNode) {
    this.insertNode = insertNode;
    this.columnNameList = new ArrayList<>();
  }

  public Tablet convertToTablet() {
    if (insertNode instanceof InsertRowNode) {
      return processRowNode((InsertRowNode) insertNode);
    } else if (insertNode instanceof InsertTabletNode) {
      return processTabletNode((InsertTabletNode) insertNode);
    } else {
      throw new UnSupportedDataTypeException(
          String.format("InsertNode type %s is not supported.", insertNode.getClass().getName()));
    }
  }

  private Tablet processRowNode(InsertRowNode insertRowNode) {
    constructMeta(insertRowNode);

    Object[] values = insertRowNode.getValues();
    System.arraycopy(values, 0, rowRecords[0], 0, columnSize);
    this.timestamps = new long[] {insertRowNode.getTime()};

    return constructTablet();
  }

  private Tablet processTabletNode(InsertTabletNode insertTabletNode) {
    constructMeta(insertTabletNode);
    Object[] columns = insertTabletNode.getColumns();
    this.rowSize = insertTabletNode.getRowCount();
    this.timestamps = insertTabletNode.getTimes();
    this.bitMaps = insertTabletNode.getBitMaps();

    this.rowRecords = new Object[rowSize][columnSize];

    for (int columnIndex = 0; columnIndex < columnSize; columnIndex++) {
      Object[] column = (Object[]) columns[columnIndex];
      for (int rowIndex = 0; rowIndex < rowSize; rowIndex++) {
        rowRecords[rowIndex][columnIndex] = column[rowIndex];
      }
    }

    return constructTablet();
  }

  private List<MeasurementSchema> createMeasurementSchemas() {
    List<MeasurementSchema> schemas = new ArrayList<>();
    for (int i = 0; i < columnNameList.size(); i++) {
      schemas.add(
          new MeasurementSchema(columnNameList.get(i).getMeasurement(), dataTypeList.get(i)));
    }
    return schemas;
  }

  private void constructMeta(InsertNode insertNode) {
    this.dataTypeList = Arrays.asList(insertNode.getDataTypes());
    String[] measurementList = insertNode.getMeasurements();
    this.columnSize = measurementList.length;
    this.deviceId = insertNode.getDevicePath().getFullPath();

    for (int i = 0; i < columnSize; i++) {
      this.columnNameList.add(new Path(deviceId, measurementList[i], false));
    }

    this.rowRecords = new Object[rowSize][columnSize];
  }

  private Tablet constructTablet() {
    List<MeasurementSchema> schemas = createMeasurementSchemas();
    return new Tablet(deviceId, schemas, timestamps, rowRecords, bitMaps, rowSize);
  }
}
