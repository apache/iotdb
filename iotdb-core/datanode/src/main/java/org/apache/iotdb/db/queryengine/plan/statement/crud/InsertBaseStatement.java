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

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.auth.entity.PrivilegeType;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory;
import org.apache.iotdb.commons.schema.view.LogicalViewSchema;
import org.apache.iotdb.db.auth.AuthorityChecker;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.metadata.DataTypeMismatchException;
import org.apache.iotdb.db.exception.metadata.DuplicateInsertException;
import org.apache.iotdb.db.exception.metadata.PathNotExistException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.plan.analyze.schema.ISchemaValidation;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.ColumnSchema;
import org.apache.iotdb.db.queryengine.plan.relational.type.InternalTypeManager;
import org.apache.iotdb.db.queryengine.plan.statement.Statement;
import org.apache.iotdb.db.utils.CommonUtils;
import org.apache.iotdb.db.utils.annotations.TableModel;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.write.schema.MeasurementSchema;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public abstract class InsertBaseStatement extends Statement {

  /**
   * if use id table, this filed is id form of device path <br>
   * if not, this filed is device path<br>
   * When using table model, this is the table name.
   */
  protected PartialPath devicePath;

  protected boolean isAligned;

  protected MeasurementSchema[] measurementSchemas;

  protected String[] measurements;
  // get from client
  protected TSDataType[] dataTypes;

  /** index of failed measurements -> info including measurement, data type and value */
  protected Map<Integer, FailedMeasurementInfo> failedMeasurementIndex2Info;

  protected TsTableColumnCategory[] columnCategories;
  protected List<Integer> idColumnIndices;
  protected List<Integer> attrColumnIndices;
  protected boolean writeToTable = false;

  // region params used by analyzing logical views.

  /** This param records the logical view schema appeared in this statement. */
  protected List<LogicalViewSchema> logicalViewSchemaList;

  /**
   * This param records the index of the location where the source of this view should be placed.
   *
   * <p>For example, indexListOfLogicalViewPaths[alpha] = beta means source of
   * logicalViewSchemaList[alpha] should be filled into measurementSchemas[beta].
   */
  protected List<Integer> indexOfSourcePathsOfLogicalViews;

  /** it is the end of last range, the beginning of current range. */
  protected int recordedBeginOfLogicalViewSchemaList = 0;

  /** it is the end of current range. */
  protected int recordedEndOfLogicalViewSchemaList = 0;

  @TableModel private String databaseName;

  // endregion

  public PartialPath getDevicePath() {
    return devicePath;
  }

  public void setDevicePath(PartialPath devicePath) {
    this.devicePath = devicePath;
  }

  public String[] getMeasurements() {
    return measurements;
  }

  public void setMeasurements(String[] measurements) {
    this.measurements = measurements;
  }

  public MeasurementSchema[] getMeasurementSchemas() {
    return measurementSchemas;
  }

  public void setMeasurementSchemas(MeasurementSchema[] measurementSchemas) {
    this.measurementSchemas = measurementSchemas;
  }

  public void setMeasurementSchema(MeasurementSchema measurementSchema, int i) {
    if (measurementSchemas == null) {
      measurementSchemas = new MeasurementSchema[measurements.length];
    }
    measurementSchemas[i] = measurementSchema;
  }

  public boolean isAligned() {
    return isAligned;
  }

  public void setAligned(boolean aligned) {
    isAligned = aligned;
  }

  public TSDataType[] getDataTypes() {
    return dataTypes;
  }

  public TSDataType getDataType(int i) {
    if (dataTypes == null) {
      return null;
    }
    return dataTypes[i];
  }

  public void setDataTypes(TSDataType[] dataTypes) {
    this.dataTypes = dataTypes;
  }

  public void setDataType(TSDataType dataType, int i) {
    if (dataTypes == null) {
      dataTypes = new TSDataType[measurements.length];
    }
    this.dataTypes[i] = dataType;
  }

  /** Returns true when this statement is empty and no need to write into the server */
  public abstract boolean isEmpty();

  @Override
  public List<PartialPath> getPaths() {
    return Collections.emptyList();
  }

  @Override
  public TSStatus checkPermissionBeforeProcess(String userName) {
    if (AuthorityChecker.SUPER_USER.equals(userName)) {
      return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    }
    List<PartialPath> checkedPaths = getPaths().stream().distinct().collect(Collectors.toList());
    return AuthorityChecker.getTSStatus(
        AuthorityChecker.checkFullPathListPermission(
            userName, checkedPaths, PrivilegeType.WRITE_DATA.ordinal()),
        checkedPaths,
        PrivilegeType.WRITE_DATA);
  }

  public abstract ISchemaValidation getSchemaValidation();

  public abstract List<ISchemaValidation> getSchemaValidationList();

  public void updateAfterSchemaValidation(MPPQueryContext context) throws QueryProcessException {}

  /** Check whether data types are matched with measurement schemas */
  protected void selfCheckDataTypes(int index)
      throws DataTypeMismatchException, PathNotExistException {
    if (IoTDBDescriptor.getInstance().getConfig().isEnablePartialInsert()) {
      // if enable partial insert, mark failed measurements with exception
      if (measurementSchemas[index] == null) {
        markFailedMeasurement(
            index,
            new PathNotExistException(devicePath.concatNode(measurements[index]).getFullPath()));
      } else if ((dataTypes[index] != measurementSchemas[index].getType()
          && !checkAndCastDataType(index, measurementSchemas[index].getType()))) {
        markFailedMeasurement(
            index,
            new DataTypeMismatchException(
                devicePath.getFullPath(),
                measurements[index],
                dataTypes[index],
                measurementSchemas[index].getType(),
                getMinTime(),
                getFirstValueOfIndex(index)));
      }
    } else {
      // if not enable partial insert, throw the exception directly
      if (measurementSchemas[index] == null) {
        throw new PathNotExistException(devicePath.concatNode(measurements[index]).getFullPath());
      } else if ((dataTypes[index] != measurementSchemas[index].getType()
          && !checkAndCastDataType(index, measurementSchemas[index].getType()))) {
        throw new DataTypeMismatchException(
            devicePath.getFullPath(),
            measurements[index],
            dataTypes[index],
            measurementSchemas[index].getType(),
            getMinTime(),
            getFirstValueOfIndex(index));
      }
    }
  }

  protected abstract boolean checkAndCastDataType(int columnIndex, TSDataType dataType);

  public abstract long getMinTime();

  public abstract Object getFirstValueOfIndex(int index);

  public void semanticCheck() {
    Set<String> deduplicatedMeasurements = new HashSet<>();
    for (String measurement : measurements) {
      if (measurement == null || measurement.isEmpty()) {
        throw new SemanticException(
            "Measurement contains null or empty string: " + Arrays.toString(measurements));
      }
      if (deduplicatedMeasurements.contains(measurement)) {
        throw new SemanticException("Insertion contains duplicated measurement: " + measurement);
      } else {
        deduplicatedMeasurements.add(measurement);
      }
    }
  }

  // region partial insert
  /**
   * Mark failed measurement, measurements[index], dataTypes[index] and values/columns[index] would
   * be null. We'd better use "measurements[index] == null" to determine if the measurement failed.
   * <br>
   * This method is not concurrency-safe.
   *
   * @param index failed measurement index
   * @param cause cause Exception of failure
   */
  public void markFailedMeasurement(int index, Exception cause) {
    throw new UnsupportedOperationException();
  }

  /** * Resets the state of all measurements marked as failed, clearing the failure records. */
  public void removeAllFailedMeasurementMarks() {
    throw new UnsupportedOperationException();
  }

  public boolean hasValidMeasurements() {
    for (Object o : measurements) {
      if (o != null) {
        return true;
      }
    }
    return false;
  }

  public TsTableColumnCategory[] getColumnCategories() {
    return columnCategories;
  }

  public TsTableColumnCategory getColumnCategory(int i) {
    if (columnCategories == null) {
      return null;
    }
    return columnCategories[i];
  }

  public void setColumnCategories(TsTableColumnCategory[] columnCategories) {
    this.columnCategories = columnCategories;
  }

  public void setColumnCategory(TsTableColumnCategory columnCategory, int i) {
    if (columnCategories == null) {
      columnCategories = new TsTableColumnCategory[measurements.length];
    }
    this.columnCategories[i] = columnCategory;
    this.idColumnIndices = null;
  }

  public List<Integer> getIdColumnIndices() {
    if (idColumnIndices == null && columnCategories != null) {
      idColumnIndices = new ArrayList<>();
      for (int i = 0; i < columnCategories.length; i++) {
        if (columnCategories[i].equals(TsTableColumnCategory.ID)) {
          idColumnIndices.add(i);
        }
      }
    }
    return idColumnIndices;
  }

  public List<Integer> getAttrColumnIndices() {
    if (attrColumnIndices == null && columnCategories != null) {
      attrColumnIndices = new ArrayList<>();
      for (int i = 0; i < columnCategories.length; i++) {
        if (columnCategories[i].equals(TsTableColumnCategory.ATTRIBUTE)) {
          attrColumnIndices.add(i);
        }
      }
    }
    return attrColumnIndices;
  }

  public boolean hasFailedMeasurements() {
    return failedMeasurementIndex2Info != null && !failedMeasurementIndex2Info.isEmpty();
  }

  public int getFailedMeasurementNumber() {
    return failedMeasurementIndex2Info == null ? 0 : failedMeasurementIndex2Info.size();
  }

  public List<String> getFailedMeasurements() {
    return failedMeasurementIndex2Info == null
        ? Collections.emptyList()
        : failedMeasurementIndex2Info.values().stream()
            .map(info -> info.measurement)
            .collect(Collectors.toList());
  }

  public List<Exception> getFailedExceptions() {
    return failedMeasurementIndex2Info == null
        ? Collections.emptyList()
        : failedMeasurementIndex2Info.values().stream()
            .map(info -> info.cause)
            .collect(Collectors.toList());
  }

  public List<String> getFailedMessages() {
    return failedMeasurementIndex2Info == null
        ? Collections.emptyList()
        : failedMeasurementIndex2Info.values().stream()
            .map(
                info -> {
                  Throwable cause = info.cause;
                  while (cause.getCause() != null) {
                    cause = cause.getCause();
                  }
                  return cause.getMessage();
                })
            .collect(Collectors.toList());
  }

  protected static class FailedMeasurementInfo {
    protected String measurement;
    protected TSDataType dataType;
    protected Object value;
    protected Exception cause;

    public FailedMeasurementInfo(
        String measurement, TSDataType dataType, Object value, Exception cause) {
      this.measurement = measurement;
      this.dataType = dataType;
      this.value = value;
      this.cause = cause;
    }

    public String getMeasurement() {
      return measurement;
    }

    public TSDataType getDataType() {
      return dataType;
    }

    public Object getValue() {
      return value;
    }
  }

  // endregion

  // region functions used by analyzing logical views
  /**
   * Remove logical view in this statement according to validated schemas. So this function should
   * be called after validating schemas.
   */
  public abstract InsertBaseStatement removeLogicalView();

  public void setFailedMeasurementIndex2Info(
      Map<Integer, FailedMeasurementInfo> failedMeasurementIndex2Info) {
    this.failedMeasurementIndex2Info = failedMeasurementIndex2Info;
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
    Arrays.fill(isLogicalView, false);
    if (this.indexOfSourcePathsOfLogicalViews != null) {
      for (int i = 0; i < this.indexOfSourcePathsOfLogicalViews.size(); i++) {
        int realIndex = this.indexOfSourcePathsOfLogicalViews.get(i);
        isLogicalView[realIndex] = true;
        indexMapToLogicalViewList[realIndex] = i;
      }
    }
    // construct map from device to measurements and record the index of its measurement
    // schema
    Map<PartialPath, List<Pair<String, Integer>>> mapFromDeviceToMeasurementAndIndex =
        new HashMap<>();
    for (int i = 0; i < this.measurements.length; i++) {
      PartialPath targetDevicePath;
      String measurementName;
      if (isLogicalView[i]) {
        int viewIndex = indexMapToLogicalViewList[i];
        targetDevicePath =
            this.logicalViewSchemaList.get(viewIndex).getSourcePathIfWritable().getDevicePath();
        measurementName =
            this.logicalViewSchemaList.get(viewIndex).getSourcePathIfWritable().getMeasurement();
      } else {
        targetDevicePath = this.devicePath;
        measurementName = this.measurements[i];
      }
      int index = i;
      final String finalMeasurementName = measurementName;
      mapFromDeviceToMeasurementAndIndex.compute(
          targetDevicePath,
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
    // check this map, ensure that all time series (measurements in each device) only appear once
    validateMapFromDeviceToMeasurement(mapFromDeviceToMeasurementAndIndex);
    return mapFromDeviceToMeasurementAndIndex;
  }

  protected static void validateMapFromDeviceToMeasurement(
      Map<PartialPath, List<Pair<String, Integer>>> map) {
    if (map == null) {
      return;
    }
    for (Map.Entry<PartialPath, List<Pair<String, Integer>>> entry : map.entrySet()) {
      List<Pair<String, Integer>> measurementList = entry.getValue();
      if (measurementList.size() <= 1) {
        continue;
      }
      Set<String> measurementSet = new HashSet<>();
      for (Pair<String, Integer> thisPair : measurementList) {
        boolean measurementNotExists = measurementSet.add(thisPair.left);
        if (!measurementNotExists) {
          PartialPath devicePath = entry.getKey();
          throw new SemanticException(
              new DuplicateInsertException(devicePath.getFullPath(), thisPair.left));
        }
      }
    }
  }

  public void insertColumn(int pos, ColumnSchema columnSchema) {
    if (pos < 0 || pos > measurements.length) {
      throw new ArrayIndexOutOfBoundsException(pos);
    }

    if (measurementSchemas != null) {
      MeasurementSchema[] tmp = new MeasurementSchema[measurementSchemas.length + 1];
      System.arraycopy(measurementSchemas, 0, tmp, 0, pos);
      tmp[pos] =
          new MeasurementSchema(
              columnSchema.getName(), InternalTypeManager.getTSDataType(columnSchema.getType()));
      System.arraycopy(measurementSchemas, pos, tmp, pos + 1, measurementSchemas.length - pos);
      measurementSchemas = tmp;
    }

    String[] tmpMeasurements = new String[measurements.length + 1];
    System.arraycopy(measurements, 0, tmpMeasurements, 0, pos);
    tmpMeasurements[pos] = columnSchema.getName();
    System.arraycopy(measurements, pos, tmpMeasurements, pos + 1, measurements.length - pos);
    measurements = tmpMeasurements;

    if (dataTypes == null) {
      // sql insertion
      dataTypes = new TSDataType[measurements.length + 1];
      dataTypes[pos] = InternalTypeManager.getTSDataType(columnSchema.getType());
    } else {
      TSDataType[] tmpTypes = new TSDataType[dataTypes.length + 1];
      System.arraycopy(dataTypes, 0, tmpTypes, 0, pos);
      tmpTypes[pos] = InternalTypeManager.getTSDataType(columnSchema.getType());
      System.arraycopy(dataTypes, pos, tmpTypes, pos + 1, dataTypes.length - pos);
      dataTypes = tmpTypes;
    }

    if (columnCategories == null) {
      columnCategories = new TsTableColumnCategory[measurements.length + 1];
      columnCategories[pos] = columnSchema.getColumnCategory();
    } else {
      TsTableColumnCategory[] tmpCategories =
          new TsTableColumnCategory[columnCategories.length + 1];
      System.arraycopy(columnCategories, 0, tmpCategories, 0, pos);
      tmpCategories[pos] = columnSchema.getColumnCategory();
      System.arraycopy(
          columnCategories, pos, tmpCategories, pos + 1, columnCategories.length - pos);
      columnCategories = tmpCategories;
      idColumnIndices = null;
    }
  }

  public void swapColumn(int src, int target) {
    if (src < 0 || src >= measurements.length || target < 0 || target >= measurements.length) {
      throw new ArrayIndexOutOfBoundsException(src + "/" + target);
    }
    if (measurementSchemas != null) {
      CommonUtils.swapArray(measurementSchemas, src, target);
    }
    CommonUtils.swapArray(measurements, src, target);
    // dataTypes is null for sql insertion
    if (dataTypes != null) {
      CommonUtils.swapArray(dataTypes, src, target);
    }
    if (columnCategories != null) {
      CommonUtils.swapArray(columnCategories, src, target);
    }
    idColumnIndices = null;
  }

  public boolean isWriteToTable() {
    return writeToTable;
  }

  public void setWriteToTable(boolean writeToTable) {
    this.writeToTable = writeToTable;
    if (writeToTable) {
      isAligned = true;
    }
  }

  @TableModel
  public void setDatabaseName(String databaseName) {
    this.databaseName = databaseName;
  }

  @TableModel
  public Optional<String> getDatabaseName() {
    return Optional.ofNullable(databaseName);
  }

  // endregion

  @TableModel
  public void toLowerCase() {
    devicePath.toLowerCase();
    if (measurements == null) {
      return;
    }
    for (int i = 0; i < measurements.length; i++) {
      if (measurements[i] != null) {
        measurements[i] = measurements[i].toLowerCase();
      }
    }
    if (measurementSchemas != null) {
      for (MeasurementSchema measurementSchema : measurementSchemas) {
        measurementSchema.setMeasurementId(measurementSchema.getMeasurementId().toLowerCase());
      }
    }
  }

  @TableModel
  public List<String> getAttributeColumnNameList() {
    List<String> attributeColumnNameList = new ArrayList<>();
    for (int i = 0; i < getColumnCategories().length; i++) {
      if (getColumnCategories()[i] == TsTableColumnCategory.ATTRIBUTE) {
        attributeColumnNameList.add(getMeasurements()[i]);
      }
    }
    return attributeColumnNameList;
  }

  @TableModel
  public String getTableName() {
    return devicePath.getFullPath();
  }
}
