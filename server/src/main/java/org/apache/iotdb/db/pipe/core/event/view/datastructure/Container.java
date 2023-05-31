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

package org.apache.iotdb.db.pipe.core.event.view.datastructure;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.write.InsertNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.write.InsertRowNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.write.InsertTabletNode;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.BitMap;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class Container {

  private static final Logger LOGGER = LoggerFactory.getLogger(Container.class);

  private List<TSDataType> columnTypeList;
  private List<String> columnNameStringList;
  private List<MeasurementSchema> measurementSchemaList;
  private String deviceId;
  private Object[][] columns;
  private long[] timestamps;
  private String pattern;

  private List<Integer> indexList;

  public Container() {
    // do nothing
  }

  public InsertNode convertToInsertNode(InsertNode insertNode, String pattern) {
    this.pattern = pattern;
    try {
      if (insertNode instanceof InsertRowNode) {
        return processRowNode((InsertRowNode) insertNode);
      } else if (insertNode instanceof InsertTabletNode) {
        return processTabletNode((InsertTabletNode) insertNode);
      } else {
        throw new UnSupportedDataTypeException(
                  String.format("InsertNode type %s is not supported.", insertNode.getClass().getName()));
      }
    } catch (IllegalPathException ignored) {
      // TODO:

    }
    return null;
  }

  //////////////////////////// InsertRowNode ////////////////////////////
  private InsertNode processRowNode(InsertRowNode insertRowNode) throws IllegalPathException {
    TSDataType[] originDataTypeList = insertRowNode.getDataTypes();
    String[] originMeasurementList = insertRowNode.getMeasurements();
    Object[] originValues = insertRowNode.getValues();
    MeasurementSchema[] originMeasurementSchemaList = insertRowNode.getMeasurementSchemas();
    this.deviceId = insertRowNode.getDevicePath().getFullPath();
    this.timestamps = new long[]{insertRowNode.getTime()};

    processPatternWithSingleRow(originDataTypeList, originMeasurementList, originValues, originMeasurementSchemaList);


    return new InsertRowNode(
              insertRowNode.getPlanNodeId(),
              new PartialPath(deviceId),
              insertRowNode.isAligned(),
              columnNameStringList.toArray(new String[0]),
              columnTypeList.toArray(new TSDataType[0]),
              measurementSchemaList.toArray(new MeasurementSchema[0]),
              insertRowNode.getTime(),
              columns,
              insertRowNode.isNeedInferType());
  }

  private void processPatternWithSingleRow(
            TSDataType[] originDataTypeList, String[] originMeasurementList, Object[] originValues, MeasurementSchema[]
            originMeasurementSchemaList) {
    this.columnTypeList = new ArrayList<>();
//    this.columnNameList = new ArrayList<>();
    this.columnNameStringList = new ArrayList<>();
    this.measurementSchemaList = new ArrayList<>();
    List<Integer> indexList = new ArrayList<>();

    processPatternByDevice(originMeasurementList, originDataTypeList, originMeasurementSchemaList, indexList);

    for (int i = 0; i < indexList.size(); i++) {
      columns[0][i] = originValues[indexList.get(i)];
    }
  }


  //////////////////////////// InsertTabletNode ////////////////////////////
  private InsertNode processTabletNode(InsertTabletNode insertTabletNode) {
    TSDataType[] originDataTypeList = insertTabletNode.getDataTypes();
    String[] originMeasurementList = insertTabletNode.getMeasurements();
    Object[] originColumns = insertTabletNode.getColumns();
    MeasurementSchema[] originMeasurementSchemaList = insertTabletNode.getMeasurementSchemas();
    int rowSize = insertTabletNode.getRowCount();
    this.deviceId = insertTabletNode.getDevicePath().getFullPath();
    this.timestamps = insertTabletNode.getTimes();

    processPatternWithColumns(originDataTypeList, originMeasurementList, originColumns, originMeasurementSchemaList, rowSize);

    BitMap[] filterBitMaps = new BitMap[indexList.size()];
    BitMap[] originBitMaps = insertTabletNode.getBitMaps();
    if (originBitMaps != null) {
      for (int i = 0; i < indexList.size(); i++) {
        filterBitMaps[i] = originBitMaps[indexList.get(i)];
      }
    }
    return new InsertTabletNode(
              insertTabletNode.getPlanNodeId(),
              new PartialPath(deviceId),
              insertTabletNode.isAligned(),
              columnNameStringList.toArray(new String[0]),
              columnTypeList.toArray(new TSDataType[0]),
              measurementSchemaList.toArray(new MeasurementSchema[0]),
              timestamps,
              filterBitMaps,
              columns,
              rowSize);
  }

  private void processPatternWithColumns(
            TSDataType[] originDataTypeList,
            String[] originMeasurementList,
            Object[] originColumns,
            MeasurementSchema[] originMeasurementSchemaList,
            int rowSize) {
    this.columnTypeList = new ArrayList<>();
    this.columnNameStringList = new ArrayList<>();
    this.measurementSchemaList = new ArrayList<>();
    this.indexList = new ArrayList<>();

    processPatternByDevice(originMeasurementList, originDataTypeList, originMeasurementSchemaList,indexList);

    int columnSize = indexList.size();
    this.columns = new Object[columnSize][rowSize];

    for (int columnIndex = 0; columnIndex < columnSize; columnIndex++) {
      columns[columnIndex] = (Object[]) originColumns[indexList.get(columnIndex)];
    }
  }

  //////////////////////////// Common ////////////////////////////

  private void processPatternByDevice(
            String[] originMeasurementList, TSDataType[] originDataTypeList, MeasurementSchema[]
            originMeasurementSchemaList, List<Integer> indexList) {
    int originColumnSize = originMeasurementList.length;
    // case 1: for example, pattern is root.a.b or pattern is null and device is root.a.b.c
    // in this case, all data can be matched without checking the measurements
    if (pattern == null || pattern.length() <= deviceId.length() && deviceId.startsWith(pattern)) {
      for (int i = 0; i < originColumnSize; i++) {
        columnNameStringList.add(originMeasurementList[i]);
        columnTypeList.add(originDataTypeList[i]);
        measurementSchemaList.add(originMeasurementSchemaList[i]);
        indexList.add(i);
      }
    }

    // case 2: for example, pattern is root.a.b.c and device is root.a.b
    // in this case, we need to check the full path
    else if (pattern.length() > deviceId.length() && pattern.startsWith(deviceId)) {
      for (int i = 0; i < originColumnSize; i++) {
        String measurement = originMeasurementList[i];

        // low cost check comes first
        if (pattern.length() == deviceId.length() + measurement.length() + 1
                  // high cost check comes later
                  && pattern.endsWith(TsFileConstant.PATH_SEPARATOR + measurement)) {
          columnTypeList.add(originDataTypeList[i]);
          columnNameStringList.add(originMeasurementList[i]);
          measurementSchemaList.add(originMeasurementSchemaList[i]);
          indexList.add(i);
        }
      }
    }
  }
}
