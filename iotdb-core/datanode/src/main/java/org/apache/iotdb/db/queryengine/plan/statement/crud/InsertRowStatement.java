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

package org.apache.iotdb.db.queryengine.plan.statement.crud;

import org.apache.iotdb.common.rpc.thrift.TTimePartitionSlot;
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.schema.view.LogicalViewSchema;
import org.apache.iotdb.commons.utils.TimePartitionUtils;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.metadata.DataTypeMismatchException;
import org.apache.iotdb.db.exception.metadata.PathNotExistException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.schematree.IMeasurementSchemaInfo;
import org.apache.iotdb.db.queryengine.plan.analyze.schema.ISchemaValidation;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.ColumnSchema;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.InsertRow;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Statement;
import org.apache.iotdb.db.queryengine.plan.statement.StatementType;
import org.apache.iotdb.db.queryengine.plan.statement.StatementVisitor;
import org.apache.iotdb.db.utils.CommonUtils;
import org.apache.iotdb.db.utils.TypeInferenceUtils;
import org.apache.iotdb.db.utils.annotations.TableModel;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.IDeviceID.Factory;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class InsertRowStatement extends InsertBaseStatement implements ISchemaValidation {

  private static final Logger LOGGER = LoggerFactory.getLogger(InsertRowStatement.class);

  protected static final byte TYPE_RAW_STRING = -1;
  protected static final byte TYPE_NULL = -2;

  protected long time;
  protected Object[] values;
  protected boolean isNeedInferType = false;

  /**
   * This param record whether the source of logical view is aligned. Only used when there are
   * views.
   */
  protected boolean[] measurementIsAligned;

  private IDeviceID deviceID;

  public InsertRowStatement() {
    super();
    statementType = StatementType.INSERT;
    this.recordedBeginOfLogicalViewSchemaList = 0;
    this.recordedEndOfLogicalViewSchemaList = 0;
  }

  @Override
  public List<PartialPath> getPaths() {
    List<PartialPath> ret = new ArrayList<>();
    for (String m : measurements) {
      PartialPath fullPath = devicePath.concatAsMeasurementPath(m);
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
        case DATE:
          values[i] = ReadWriteIOUtils.readInt(buffer);
          break;
        case INT64:
        case TIMESTAMP:
          values[i] = ReadWriteIOUtils.readLong(buffer);
          break;
        case FLOAT:
          values[i] = ReadWriteIOUtils.readFloat(buffer);
          break;
        case DOUBLE:
          values[i] = ReadWriteIOUtils.readDouble(buffer);
          break;
        case TEXT:
        case BLOB:
        case STRING:
          values[i] = ReadWriteIOUtils.readBinary(buffer);
          break;
        default:
          throw new QueryProcessException("Unsupported data type:" + dataTypes[i]);
      }
    }
  }

  public TTimePartitionSlot getTimePartitionSlot() {
    return TimePartitionUtils.getTimePartitionSlot(time);
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
  public void transferType(ZoneId zoneId) throws QueryProcessException {

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
        // if the type is binary and the value is already binary, do not convert
        if (values[i] instanceof String
            || values[i] != null && !(dataTypes[i].isBinary() && values[i] instanceof Binary)) {
          values[i] = CommonUtils.parseValue(dataTypes[i], values[i].toString(), zoneId);
        }
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
          markFailedMeasurement(
              i, new SemanticException(e, TSStatusCode.DATA_TYPE_MISMATCH.getStatusCode()));
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

  @Override
  public void removeAllFailedMeasurementMarks() {
    if (failedMeasurementIndex2Info == null) {
      return;
    }
    failedMeasurementIndex2Info.forEach(
        (index, info) -> {
          measurements[index] = info.getMeasurement();
          dataTypes[index] = info.getDataType();
          values[index] = info.getValue();
        });
    failedMeasurementIndex2Info.clear();
  }

  @Override
  public void semanticCheck() {
    super.semanticCheck();
    if (measurements.length != values.length) {
      throw new SemanticException(
          String.format(
              "the measurementList's size %d is not consistent with the valueList's size %d",
              measurements.length, values.length));
    }
  }

  public boolean isNeedSplit() {
    return hasLogicalViewNeedProcess();
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
      statement.setNeedInferType(this.isNeedInferType);
      statement.setDevicePath(entry.getKey());
      statement.setAligned(this.isAligned);
      Object[] copiedValues = new Object[pairList.size()];
      String[] measurements = new String[pairList.size()];
      MeasurementSchema[] measurementSchemas = new MeasurementSchema[pairList.size()];
      TSDataType[] dataTypes = new TSDataType[pairList.size()];
      for (int i = 0; i < pairList.size(); i++) {
        int realIndex = pairList.get(i).right;
        copiedValues[i] = this.values[realIndex];
        measurements[i] = pairList.get(i).left;
        measurementSchemas[i] = this.measurementSchemas[realIndex];
        dataTypes[i] = this.dataTypes[realIndex];
        if (this.measurementIsAligned != null) {
          statement.setAligned(this.measurementIsAligned[realIndex]);
        }
      }
      statement.setValues(copiedValues);
      statement.setMeasurements(measurements);
      statement.setMeasurementSchemas(measurementSchemas);
      statement.setDataTypes(dataTypes);
      statement.setFailedMeasurementIndex2Info(failedMeasurementIndex2Info);
      insertRowStatementList.add(statement);
    }
    return insertRowStatementList;
  }

  @Override
  public InsertBaseStatement removeLogicalView() {
    if (!isNeedSplit()) {
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
  public void updateAfterSchemaValidation(MPPQueryContext context) throws QueryProcessException {
    if (isNeedInferType) {
      transferType(context == null ? ZoneId.systemDefault() : context.getZoneId());
    }
  }

  @Override
  public TSDataType getDataType(int index) {
    if (isNeedInferType && (dataTypes == null || dataTypes[index] == null)) {
      if (dataTypes == null) {
        dataTypes = new TSDataType[measurements.length];
      }
      dataTypes[index] = TypeInferenceUtils.getPredictedDataType(values[index], true);
      return dataTypes[index];
    } else {
      return dataTypes != null ? dataTypes[index] : null;
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
    this.isAligned = isAligned;
  }

  @Override
  public void validateMeasurementSchema(int index, IMeasurementSchemaInfo measurementSchemaInfo) {
    if (measurementSchemas == null) {
      measurementSchemas = new MeasurementSchema[measurements.length];
    }
    if (measurementSchemaInfo == null) {
      measurementSchemas[index] = null;
    } else {
      if (measurementSchemaInfo.isLogicalView()) {
        if (logicalViewSchemaList == null || indexOfSourcePathsOfLogicalViews == null) {
          logicalViewSchemaList = new ArrayList<>();
          indexOfSourcePathsOfLogicalViews = new ArrayList<>();
        }
        logicalViewSchemaList.add(measurementSchemaInfo.getSchemaAsLogicalViewSchema());
        indexOfSourcePathsOfLogicalViews.add(index);
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
  public void validateMeasurementSchema(
      int index, IMeasurementSchemaInfo measurementSchemaInfo, boolean isAligned) {
    this.validateMeasurementSchema(index, measurementSchemaInfo);
    if (this.measurementIsAligned == null) {
      this.measurementIsAligned = new boolean[this.measurements.length];
      Arrays.fill(this.measurementIsAligned, this.isAligned);
    }
    this.measurementIsAligned[index] = isAligned;
  }

  @Override
  public boolean hasLogicalViewNeedProcess() {
    if (this.indexOfSourcePathsOfLogicalViews == null) {
      return false;
    }
    return !this.indexOfSourcePathsOfLogicalViews.isEmpty();
  }

  @Override
  public List<LogicalViewSchema> getLogicalViewSchemaList() {
    return this.logicalViewSchemaList;
  }

  @Override
  public List<Integer> getIndexListOfLogicalViewPaths() {
    return this.indexOfSourcePathsOfLogicalViews;
  }

  @Override
  public void recordRangeOfLogicalViewSchemaListNow() {
    if (this.logicalViewSchemaList != null) {
      this.recordedBeginOfLogicalViewSchemaList = this.recordedEndOfLogicalViewSchemaList;
      this.recordedEndOfLogicalViewSchemaList = this.logicalViewSchemaList.size();
    }
  }

  @Override
  public Pair<Integer, Integer> getRangeOfLogicalViewSchemaListRecorded() {
    return new Pair<>(
        this.recordedBeginOfLogicalViewSchemaList, this.recordedEndOfLogicalViewSchemaList);
  }

  @TableModel
  public IDeviceID getTableDeviceID() {
    if (deviceID == null) {
      String[] deviceIdSegments = new String[getIdColumnIndices().size() + 1];
      deviceIdSegments[0] = this.getTableName();
      for (int i = 0; i < getIdColumnIndices().size(); i++) {
        final Integer columnIndex = getIdColumnIndices().get(i);
        deviceIdSegments[i + 1] =
            values[columnIndex] != null ? values[columnIndex].toString() : null;
      }
      deviceID = Factory.DEFAULT_FACTORY.create(deviceIdSegments);
    }

    return deviceID;
  }

  @TableModel
  @Override
  public Statement toRelationalStatement(MPPQueryContext context) {
    return new InsertRow(this, context);
  }

  @TableModel
  @Override
  public void insertColumn(int pos, ColumnSchema columnSchema) {
    super.insertColumn(pos, columnSchema);
    Object[] tmpValues = new Object[values.length + 1];
    System.arraycopy(values, 0, tmpValues, 0, pos);
    System.arraycopy(values, pos, tmpValues, pos + 1, values.length - pos);
    values = tmpValues;
  }

  @TableModel
  @Override
  public void swapColumn(int src, int target) {
    super.swapColumn(src, target);
    CommonUtils.swapArray(values, src, target);
  }
}
