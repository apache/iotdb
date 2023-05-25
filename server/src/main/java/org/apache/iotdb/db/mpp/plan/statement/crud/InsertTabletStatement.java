/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.db.mpp.plan.statement.crud;

import org.apache.iotdb.common.rpc.thrift.TTimePartitionSlot;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.schema.view.LogicalViewSchema;
import org.apache.iotdb.db.exception.metadata.AlignedTimeseriesException;
import org.apache.iotdb.db.exception.metadata.DataTypeMismatchException;
import org.apache.iotdb.db.exception.metadata.PathNotExistException;
import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.db.mpp.common.schematree.IMeasurementSchemaInfo;
import org.apache.iotdb.db.mpp.plan.analyze.schema.ISchemaValidation;
import org.apache.iotdb.db.mpp.plan.statement.StatementType;
import org.apache.iotdb.db.mpp.plan.statement.StatementVisitor;
import org.apache.iotdb.db.utils.CommonUtils;
import org.apache.iotdb.db.utils.TimePartitionUtils;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.BitMap;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class InsertTabletStatement extends InsertBaseStatement implements ISchemaValidation {

  private static final Logger LOGGER = LoggerFactory.getLogger(InsertTabletStatement.class);

  private static final String DATATYPE_UNSUPPORTED = "Data type %s is not supported.";

  private long[] times; // times should be sorted. It is done in the session API.
  private BitMap[] bitMaps;
  private Object[] columns;

  private int rowCount = 0;

  public InsertTabletStatement() {
    super();
    statementType = StatementType.BATCH_INSERT;
    this.logicalViewSchemaList = new ArrayList<>();
    this.indexListOfLogicalViewPaths = new ArrayList<>();
    this.recordedBeginOfLogicalViewSchemaList = 0;
    this.recordedEndOfLogicalViewSchemaList = 0;
  }

  public int getRowCount() {
    return rowCount;
  }

  public void setRowCount(int rowCount) {
    this.rowCount = rowCount;
  }

  public Object[] getColumns() {
    return columns;
  }

  public void setColumns(Object[] columns) {
    this.columns = columns;
  }

  public BitMap[] getBitMaps() {
    return bitMaps;
  }

  public void setBitMaps(BitMap[] bitMaps) {
    this.bitMaps = bitMaps;
  }

  public long[] getTimes() {
    return times;
  }

  public void setTimes(long[] times) {
    this.times = times;
  }

  @Override
  public boolean isEmpty() {
    return rowCount == 0
        || times.length == 0
        || measurements.length == 0
        || dataTypes.length == 0
        || columns.length == 0;
  }

  public List<TTimePartitionSlot> getTimePartitionSlots() {
    List<TTimePartitionSlot> result = new ArrayList<>();
    long startTime =
        (times[0] / TimePartitionUtils.timePartitionInterval)
            * TimePartitionUtils.timePartitionInterval; // included
    long endTime = startTime + TimePartitionUtils.timePartitionInterval; // excluded
    TTimePartitionSlot timePartitionSlot = TimePartitionUtils.getTimePartition(times[0]);
    for (int i = 1; i < times.length; i++) { // times are sorted in session API.
      if (times[i] >= endTime) {
        result.add(timePartitionSlot);
        // next init
        endTime =
            (times[i] / TimePartitionUtils.timePartitionInterval + 1)
                * TimePartitionUtils.timePartitionInterval;
        timePartitionSlot = TimePartitionUtils.getTimePartition(times[i]);
      }
    }
    result.add(timePartitionSlot);
    return result;
  }

  @Override
  public <R, C> R accept(StatementVisitor<R, C> visitor, C context) {
    return visitor.visitInsertTablet(this, context);
  }

  @Override
  public List<PartialPath> getPaths() {
    List<PartialPath> ret = new ArrayList<>();
    for (String m : measurements) {
      PartialPath fullPath = devicePath.concatNode(m);
      ret.add(fullPath);
    }
    return ret;
  }

  @Override
  public ISchemaValidation getSchemaValidation() {
    return this;
  }

  @Override
  public List<ISchemaValidation> getSchemaValidationList() {
    throw new UnsupportedOperationException();
  }

  @Override
  protected boolean checkAndCastDataType(int columnIndex, TSDataType dataType) {
    if (CommonUtils.checkCanCastType(dataTypes[columnIndex], dataType)) {
      LOGGER.warn(
          "Inserting to {}.{} : Cast from {} to {}",
          devicePath,
          measurements[columnIndex],
          dataTypes[columnIndex],
          dataType);
      columns[columnIndex] =
          CommonUtils.castArray(dataTypes[columnIndex], dataType, columns[columnIndex]);
      dataTypes[columnIndex] = dataType;
      return true;
    }
    return false;
  }

  @Override
  public void markFailedMeasurement(int index, Exception cause) {
    if (measurements[index] == null) {
      return;
    }

    if (failedMeasurementIndex2Info == null) {
      failedMeasurementIndex2Info = new HashMap<>();
    }

    InsertBaseStatement.FailedMeasurementInfo failedMeasurementInfo =
        new InsertBaseStatement.FailedMeasurementInfo(
            measurements[index], dataTypes[index], columns[index], cause);
    failedMeasurementIndex2Info.putIfAbsent(index, failedMeasurementInfo);

    measurements[index] = null;
    dataTypes[index] = null;
    columns[index] = null;
  }

  protected Map<PartialPath, List<Pair<String, Integer>>> getMapFromDeviceToMeasurementAndIndex() {
    boolean[] isLogicalView = new boolean[this.measurements.length];
    for (int i = 0; i < this.measurements.length; i++) {
      isLogicalView[i] = false;
    }
    for (int realIndex : this.indexListOfLogicalViewPaths) {
      isLogicalView[realIndex] = true;
    }
    // construct map from device to measurements and record the index of its measurement schema
    Map<PartialPath, List<Pair<String, Integer>>> mapFromDeviceToMeasurementAndIndex =
        new HashMap<>();
    for (int i = 0; i < this.measurements.length; i++) {
      PartialPath devicePath;
      String measurementName;
      if (isLogicalView[i]) {
        devicePath = this.logicalViewSchemaList.get(i).getSourcePathIfWritable().getDevicePath();
        measurementName =
            this.logicalViewSchemaList.get(i).getSourcePathIfWritable().getMeasurement();
      } else {
        devicePath = this.devicePath;
        measurementName = this.measurements[i];
      }
      int index = i;
      final String finalMeasurementName = measurementName;
      mapFromDeviceToMeasurementAndIndex.compute(
          devicePath,
          (k, v) -> {
            if (v == null) {
              List<Pair<String, Integer>> valueList = new ArrayList<>();
              valueList.add(new Pair<>(finalMeasurementName, index));
              return valueList;
            } else {
              v.add(new Pair<>(finalMeasurementName, index));
              return v;
            }
          });
    }
    return mapFromDeviceToMeasurementAndIndex;
  }

  public boolean isNeedSplit() {
    return !this.indexListOfLogicalViewPaths.isEmpty();
  }

  public List<InsertTabletStatement> getSplitList() {
    if (!isNeedSplit()) {
      return Collections.singletonList(this);
    }
    Map<PartialPath, List<Pair<String, Integer>>> mapFromDeviceToMeasurementAndIndex =
        this.getMapFromDeviceToMeasurementAndIndex();
    // Reconstruct statements
    List<InsertTabletStatement> insertTabletStatementList = new ArrayList<>();
    for (Map.Entry<PartialPath, List<Pair<String, Integer>>> entry :
        mapFromDeviceToMeasurementAndIndex.entrySet()) {
      List<Pair<String, Integer>> pairList = entry.getValue();
      InsertTabletStatement statement = new InsertTabletStatement();
      statement.setTimes(this.times);
      statement.setAligned(this.isAligned);
      statement.setDevicePath(entry.getKey());
      statement.setRowCount(this.rowCount);
      Object[] columns = new Object[pairList.size()];
      String[] measurements = new String[pairList.size()];
      BitMap[] bitMaps = new BitMap[pairList.size()];
      MeasurementSchema[] measurementSchemas = new MeasurementSchema[pairList.size()];
      TSDataType[] dataTypes = new TSDataType[pairList.size()];
      for (int i = 0; i < pairList.size(); i++) {
        int realIndex = pairList.get(i).right;
        columns[i] = this.columns[realIndex];
        measurements[i] = pairList.get(i).left;
        measurementSchemas[i] = this.measurementSchemas[realIndex];
        dataTypes[i] = this.dataTypes[realIndex];
        bitMaps[i] = this.bitMaps[realIndex];
      }
      statement.setColumns(columns);
      statement.setMeasurements(measurements);
      statement.setMeasurementSchemas(measurementSchemas);
      statement.setDataTypes(dataTypes);
      statement.setBitMaps(bitMaps);
      statement.setFailedMeasurementIndex2Info(failedMeasurementIndex2Info);
      insertTabletStatementList.add(statement);
    }
    return insertTabletStatementList;
  }

  @Override
  public InsertBaseStatement split() {
    if (this.indexListOfLogicalViewPaths.isEmpty()) {
      return this;
    }
    List<InsertTabletStatement> insertTabletStatementList = this.getSplitList();
    if (insertTabletStatementList.size() == 1) {
      return insertTabletStatementList.get(0);
    }
    InsertMultiTabletsStatement insertMultiTabletsStatement = new InsertMultiTabletsStatement();
    insertMultiTabletsStatement.setInsertTabletStatementList(insertTabletStatementList);
    return insertMultiTabletsStatement;
  }

  @Override
  public long getMinTime() {
    return times[0];
  }

  @Override
  public Object getFirstValueOfIndex(int index) {
    Object value;
    switch (dataTypes[index]) {
      case INT32:
        int[] intValues = (int[]) columns[index];
        value = intValues[0];
        break;
      case INT64:
        long[] longValues = (long[]) columns[index];
        value = longValues[0];
        break;
      case FLOAT:
        float[] floatValues = (float[]) columns[index];
        value = floatValues[0];
        break;
      case DOUBLE:
        double[] doubleValues = (double[]) columns[index];
        value = doubleValues[0];
        break;
      case BOOLEAN:
        boolean[] boolValues = (boolean[]) columns[index];
        value = boolValues[0];
        break;
      case TEXT:
        Binary[] binaryValues = (Binary[]) columns[index];
        value = binaryValues[0];
        break;
      default:
        throw new UnSupportedDataTypeException(
            String.format(DATATYPE_UNSUPPORTED, dataTypes[index]));
    }
    return value;
  }

  @Override
  public TSDataType getDataType(int index) {
    return dataTypes[index];
  }

  @Override
  public TSEncoding getEncoding(int index) {
    return null;
  }

  @Override
  public CompressionType getCompressionType(int index) {
    return null;
  }

  @Override
  public void validateDeviceSchema(boolean isAligned) {
    if (this.isAligned != isAligned) {
      throw new SemanticException(
          new AlignedTimeseriesException(
              String.format(
                  "timeseries under this device are%s aligned, " + "please use %s interface",
                  isAligned ? "" : " not", isAligned ? "aligned" : "non-aligned"),
              devicePath.getFullPath()));
    }
  }

  @Override
  public void validateMeasurementSchema(int index, IMeasurementSchemaInfo measurementSchemaInfo) {
    if (measurementSchemas == null) {
      measurementSchemas = new MeasurementSchema[measurements.length];
    }
    if (logicalViewSchemaList == null || indexListOfLogicalViewPaths == null) {
      logicalViewSchemaList = new ArrayList<>();
      indexListOfLogicalViewPaths = new ArrayList<>();
    }
    if (measurementSchemaInfo == null) {
      measurementSchemas[index] = null;
    } else {
      if (measurementSchemaInfo.isLogicalView()) {
        logicalViewSchemaList.add(measurementSchemaInfo.getSchemaAsLogicalViewSchema());
        indexListOfLogicalViewPaths.add(index);
      } else {
        measurementSchemas[index] = measurementSchemaInfo.getSchemaAsMeasurementSchema();
      }
    }

    try {
      selfCheckDataTypes(index);
    } catch (DataTypeMismatchException | PathNotExistException e) {
      throw new SemanticException(e);
    }
  }

  @Override
  public List<LogicalViewSchema> getLogicalViewSchemaList() {
    return this.logicalViewSchemaList;
  }

  @Override
  public List<Integer> getIndexListOfLogicalViewPaths() {
    return this.indexListOfLogicalViewPaths;
  }

  @Override
  public void recordSizeOfLogicalViewSchemaListNow() {
    this.recordedBeginOfLogicalViewSchemaList = this.recordedEndOfLogicalViewSchemaList;
    this.recordedEndOfLogicalViewSchemaList = this.logicalViewSchemaList.size();
  }

  @Override
  public Pair<Integer, Integer> getSizeOfLogicalViewSchemaListRecorded() {
    return new Pair<>(
        this.recordedBeginOfLogicalViewSchemaList, this.recordedEndOfLogicalViewSchemaList);
  }
}
