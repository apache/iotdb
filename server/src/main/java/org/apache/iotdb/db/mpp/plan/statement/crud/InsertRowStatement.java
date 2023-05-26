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
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.schema.view.LogicalViewSchema;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.metadata.AlignedTimeseriesException;
import org.apache.iotdb.db.exception.metadata.DataTypeMismatchException;
import org.apache.iotdb.db.exception.metadata.PathNotExistException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.db.mpp.common.schematree.IMeasurementSchemaInfo;
import org.apache.iotdb.db.mpp.plan.analyze.schema.ISchemaValidation;
import org.apache.iotdb.db.mpp.plan.statement.StatementType;
import org.apache.iotdb.db.mpp.plan.statement.StatementVisitor;
import org.apache.iotdb.db.utils.CommonUtils;
import org.apache.iotdb.db.utils.TimePartitionUtils;
import org.apache.iotdb.db.utils.TypeInferenceUtils;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class InsertRowStatement extends InsertBaseStatement implements ISchemaValidation {

  private static final Logger LOGGER = LoggerFactory.getLogger(InsertRowStatement.class);

  private static final byte TYPE_RAW_STRING = -1;
  private static final byte TYPE_NULL = -2;

  private long time;
  private Object[] values;
  private boolean isNeedInferType = false;

  public InsertRowStatement() {
    super();
    statementType = StatementType.INSERT;
    this.logicalViewSchemaList = new ArrayList<>();
    this.indexListOfLogicalViewPaths = new ArrayList<>();
    this.recordedBeginOfLogicalViewSchemaList = 0;
    this.recordedEndOfLogicalViewSchemaList = 0;
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

  public long getTime() {
    return time;
  }

  public void setTime(long time) {
    this.time = time;
  }

  public Object[] getValues() {
    return values;
  }

  public void setValues(Object[] values) {
    this.values = values;
  }

  public boolean isNeedInferType() {
    return isNeedInferType;
  }

  public void setNeedInferType(boolean needInferType) {
    isNeedInferType = needInferType;
  }

  @Override
  public boolean isEmpty() {
    return values.length == 0;
  }

  public void fillValues(ByteBuffer buffer) throws QueryProcessException {
    this.values = new Object[measurements.length];
    this.dataTypes = new TSDataType[measurements.length];
    for (int i = 0; i < dataTypes.length; i++) {
      // Types are not determined, the situation mainly occurs when the plan uses string values
      // and is forwarded to other nodes
      byte typeNum = (byte) ReadWriteIOUtils.read(buffer);
      if (typeNum == TYPE_RAW_STRING || typeNum == TYPE_NULL) {
        values[i] = typeNum == TYPE_RAW_STRING ? ReadWriteIOUtils.readString(buffer) : null;
        continue;
      }
      dataTypes[i] = TSDataType.values()[typeNum];
      switch (dataTypes[i]) {
        case BOOLEAN:
          values[i] = ReadWriteIOUtils.readBool(buffer);
          break;
        case INT32:
          values[i] = ReadWriteIOUtils.readInt(buffer);
          break;
        case INT64:
          values[i] = ReadWriteIOUtils.readLong(buffer);
          break;
        case FLOAT:
          values[i] = ReadWriteIOUtils.readFloat(buffer);
          break;
        case DOUBLE:
          values[i] = ReadWriteIOUtils.readDouble(buffer);
          break;
        case TEXT:
          values[i] = ReadWriteIOUtils.readBinary(buffer);
          break;
        default:
          throw new QueryProcessException("Unsupported data type:" + dataTypes[i]);
      }
    }
  }

  public TTimePartitionSlot getTimePartitionSlot() {
    return TimePartitionUtils.getTimePartition(time);
  }

  @Override
  public <R, C> R accept(StatementVisitor<R, C> visitor, C context) {
    return visitor.visitInsertRow(this, context);
  }

  @Override
  public long getMinTime() {
    return getTime();
  }

  @Override
  public Object getFirstValueOfIndex(int index) {
    return values[index];
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
      values[columnIndex] =
          CommonUtils.castValue(dataTypes[columnIndex], dataType, values[columnIndex]);
      dataTypes[columnIndex] = dataType;
      return true;
    }
    return false;
  }

  /**
   * transfer String[] values to specific data types when isNeedInferType is true. <br>
   * Notice: measurementSchemas must be initialized before calling this method
   */
  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  public void transferType() throws QueryProcessException {

    for (int i = 0; i < measurementSchemas.length; i++) {
      // null when time series doesn't exist
      if (measurementSchemas[i] == null) {
        if (!IoTDBDescriptor.getInstance().getConfig().isEnablePartialInsert()) {
          throw new QueryProcessException(
              new PathNotExistException(
                  devicePath.getFullPath() + IoTDBConstant.PATH_SEPARATOR + measurements[i]));
        } else {
          markFailedMeasurement(
              i,
              new QueryProcessException(
                  new PathNotExistException(
                      devicePath.getFullPath() + IoTDBConstant.PATH_SEPARATOR + measurements[i])));
        }
        continue;
      }
      // parse string value to specific type
      dataTypes[i] = measurementSchemas[i].getType();
      try {
        values[i] = CommonUtils.parseValue(dataTypes[i], values[i].toString());
      } catch (Exception e) {
        LOGGER.warn(
            "data type of {}.{} is not consistent, "
                + "registered type {}, inserting timestamp {}, value {}",
            devicePath,
            measurements[i],
            dataTypes[i],
            time,
            values[i]);
        if (!IoTDBDescriptor.getInstance().getConfig().isEnablePartialInsert()) {
          throw e;
        } else {
          markFailedMeasurement(i, e);
        }
      }
    }
    isNeedInferType = false;
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
            measurements[index], dataTypes[index], values[index], cause);
    failedMeasurementIndex2Info.putIfAbsent(index, failedMeasurementInfo);

    measurements[index] = null;
    dataTypes[index] = null;
    values[index] = null;
  }

  /**
   * This function is used in splitting. Traverse two lists: logicalViewSchemaList, measurements,
   * then find out all devices in this statement. Those devices will map to their measurements,
   * recorded in a pair of measurement name and the index of measurement schemas
   * (this.measurementSchemas).
   *
   * @return map from device path to its measurements.
   */
  protected Map<PartialPath, List<Pair<String, Integer>>> getMapFromDeviceToMeasurementAndIndex() {
    boolean[] isLogicalView = new boolean[this.measurements.length];
    int[] indexMapToLogicalViewList = new int[this.measurements.length];
    for (int i = 0; i < this.measurements.length; i++) {
      isLogicalView[i] = false;
    }
    for (int i = 0; i < this.indexListOfLogicalViewPaths.size(); i++) {
      int realIndex = this.indexListOfLogicalViewPaths.get(i);
      isLogicalView[realIndex] = true;
      indexMapToLogicalViewList[realIndex] = i;
    }
    // construct map from device to measurements and record the index of its measurement schema
    Map<PartialPath, List<Pair<String, Integer>>> mapFromDeviceToMeasurementAndIndex =
        new HashMap<>();
    for (int i = 0; i < this.measurements.length; i++) {
      PartialPath devicePath;
      String measurementName;
      if (isLogicalView[i]) {
        int viewIndex = indexMapToLogicalViewList[i];
        devicePath =
            this.logicalViewSchemaList.get(viewIndex).getSourcePathIfWritable().getDevicePath();
        measurementName =
            this.logicalViewSchemaList.get(viewIndex).getSourcePathIfWritable().getMeasurement();
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

  public List<InsertRowStatement> getSplitList() {
    if (!isNeedSplit()) {
      return Collections.singletonList(this);
    }
    Map<PartialPath, List<Pair<String, Integer>>> mapFromDeviceToMeasurementAndIndex =
        this.getMapFromDeviceToMeasurementAndIndex();
    // Reconstruct statements
    List<InsertRowStatement> insertRowStatementList = new ArrayList<>();
    for (Map.Entry<PartialPath, List<Pair<String, Integer>>> entry :
        mapFromDeviceToMeasurementAndIndex.entrySet()) {
      List<Pair<String, Integer>> pairList = entry.getValue();
      InsertRowStatement statement = new InsertRowStatement();
      statement.setTime(this.time);
      statement.setAligned(this.isAligned);
      statement.setNeedInferType(this.isNeedInferType);
      statement.setDevicePath(entry.getKey());
      Object[] values = new Object[pairList.size()];
      String[] measurements = new String[pairList.size()];
      MeasurementSchema[] measurementSchemas = new MeasurementSchema[pairList.size()];
      TSDataType[] dataTypes = new TSDataType[pairList.size()];
      for (int i = 0; i < pairList.size(); i++) {
        int realIndex = pairList.get(i).right;
        values[i] = this.values[realIndex];
        measurements[i] = pairList.get(i).left;
        measurementSchemas[i] = this.measurementSchemas[realIndex];
        dataTypes[i] = this.dataTypes[realIndex];
      }
      statement.setValues(values);
      statement.setMeasurements(measurements);
      statement.setMeasurementSchemas(measurementSchemas);
      statement.setDataTypes(dataTypes);
      statement.setFailedMeasurementIndex2Info(failedMeasurementIndex2Info);
      insertRowStatementList.add(statement);
    }
    return insertRowStatementList;
  }

  @Override
  public InsertBaseStatement split() {
    if (this.indexListOfLogicalViewPaths.isEmpty()) {
      return this;
    }
    List<InsertRowStatement> insertRowStatementList = this.getSplitList();
    if (insertRowStatementList.size() == 1) {
      return insertRowStatementList.get(0);
    }
    InsertRowsStatement insertRowsStatement = new InsertRowsStatement();
    insertRowsStatement.setInsertRowStatementList(insertRowStatementList);
    return insertRowsStatement;
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
  public void updateAfterSchemaValidation() throws QueryProcessException {
    if (isNeedInferType) {
      transferType();
    }
  }

  @Override
  public TSDataType getDataType(int index) {
    if (isNeedInferType) {
      return TypeInferenceUtils.getPredictedDataType(values[index], true);
    } else {
      return dataTypes[index];
    }
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
        return;
      } else {
        measurementSchemas[index] = measurementSchemaInfo.getSchemaAsMeasurementSchema();
      }
    }
    if (isNeedInferType) {
      return;
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
