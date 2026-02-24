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

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory;
import org.apache.iotdb.commons.schema.view.LogicalViewSchema;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.metadata.DataTypeMismatchException;
import org.apache.iotdb.db.exception.metadata.DuplicateInsertException;
import org.apache.iotdb.db.exception.metadata.PathNotExistException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.db.pipe.resource.memory.InsertNodeMemoryEstimator;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.plan.analyze.schema.ISchemaValidation;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.InputLocation;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.ColumnSchema;
import org.apache.iotdb.db.queryengine.plan.relational.type.InternalTypeManager;
import org.apache.iotdb.db.queryengine.plan.statement.Statement;
import org.apache.iotdb.db.schemaengine.schemaregion.attribute.update.UpdateDetailContainer;
import org.apache.iotdb.db.utils.CommonUtils;

import org.apache.tsfile.annotations.TableModel;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.type.Type;
import org.apache.tsfile.utils.Accountable;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.utils.RamUsageEstimator;
import org.apache.tsfile.write.schema.MeasurementSchema;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public abstract class InsertBaseStatement extends Statement implements Accountable {

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

  protected Type[] typeConvertors;
  protected InputLocation[] inputLocations;

  /** index of failed measurements -> info including measurement, data type and value */
  protected Map<Integer, FailedMeasurementInfo> failedMeasurementIndex2Info;

  protected TsTableColumnCategory[] columnCategories;
  protected List<Integer> tagColumnIndices;
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

  @TableModel protected String databaseName;

  protected long ramBytesUsed = Long.MIN_VALUE;

  /** Flag to indicate if semantic check has been performed */
  private boolean hasSemanticChecked = false;

  /** Flag indicating whether ATTRIBUTE columns currently exist (default: true). */
  private boolean attributeColumnsPresent = true;

  /** Flag indicating whether the lower-case transformation has already been applied. */
  private boolean toLowerCaseApplied = false;

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

  public void setTypeConvertors(Type[] typeConvertors) {
    this.typeConvertors = typeConvertors;
  }

  public void setInputLocations(InputLocation[] inputLocations) {
    this.inputLocations = inputLocations;
  }

  /** Returns true when this statement is empty and no need to write into the server */
  public abstract boolean isEmpty();

  @Override
  public List<PartialPath> getPaths() {
    return Collections.emptyList();
  }

  public abstract ISchemaValidation getSchemaValidation();

  public abstract List<ISchemaValidation> getSchemaValidationList();

  public void updateAfterSchemaValidation(MPPQueryContext context) throws QueryProcessException {}

  /** Check whether data types are matched with measurement schemas */
  public void selfCheckDataTypes(int index)
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
    // Skip if semantic check has already been performed
    if (hasSemanticChecked) {
      return;
    }

    Set<String> deduplicatedMeasurements = new HashSet<>(measurements.length);
    int index = 0;
    int failedMeasurements = 0;
    for (String measurement : measurements) {
      if (measurement == null || measurement.isEmpty()) {
        if ((failedMeasurementIndex2Info != null
            && failedMeasurementIndex2Info.containsKey(index + failedMeasurements))) {
          failedMeasurements++;
          continue;
        }
        throw new SemanticException(
            "Measurement contains null or empty string: " + Arrays.toString(measurements));
      }
      index++;
      deduplicatedMeasurements.add(measurement);
      if (deduplicatedMeasurements.size() != index) {
        throw new SemanticException("Insertion contains duplicated measurement: " + measurement);
      }
    }

    // Mark as checked to avoid redundant checks
    setSemanticChecked(true);
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
    this.tagColumnIndices = null;
  }

  public List<Integer> getTagColumnIndices() {
    if (tagColumnIndices == null && columnCategories != null) {
      tagColumnIndices = new ArrayList<>();
      for (int i = 0; i < columnCategories.length; i++) {
        if (columnCategories[i].equals(TsTableColumnCategory.TAG)) {
          tagColumnIndices.add(i);
        }
      }
    }
    return tagColumnIndices;
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

  public boolean isAttributeColumnsPresent() {
    return attributeColumnsPresent;
  }

  public void setAttributeColumnsPresent(final boolean attributeColumnsPresent) {
    this.attributeColumnsPresent = attributeColumnsPresent;
  }

  public boolean isToLowerCaseApplied() {
    return toLowerCaseApplied;
  }

  public void setToLowerCaseApplied(final boolean toLowerCaseApplied) {
    this.toLowerCaseApplied = toLowerCaseApplied;
  }

  public boolean isSemanticChecked() {
    return hasSemanticChecked;
  }

  public void setSemanticChecked(final boolean semanticChecked) {
    this.hasSemanticChecked = semanticChecked;
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

  public Map<Integer, FailedMeasurementInfo> getFailedMeasurementInfoMap() {
    return failedMeasurementIndex2Info;
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

  @TableModel
  public void removeAttributeColumns() {
    if (!attributeColumnsPresent) {
      return;
    }
    if (columnCategories == null) {
      attributeColumnsPresent = false;
      return;
    }

    List<Integer> columnsToKeep = new ArrayList<>();
    for (int i = 0; i < columnCategories.length; i++) {
      if (!columnCategories[i].equals(TsTableColumnCategory.ATTRIBUTE)) {
        columnsToKeep.add(i);
      }
    }

    if (columnsToKeep.size() == columnCategories.length) {
      attributeColumnsPresent = false;
      return;
    }

    if (failedMeasurementIndex2Info != null) {
      failedMeasurementIndex2Info =
          failedMeasurementIndex2Info.entrySet().stream()
              .collect(Collectors.toMap(e -> columnsToKeep.indexOf(e.getKey()), Entry::getValue));
    }

    if (measurementSchemas != null) {
      measurementSchemas =
          columnsToKeep.stream().map(i -> measurementSchemas[i]).toArray(MeasurementSchema[]::new);
    }
    if (measurements != null) {
      measurements = columnsToKeep.stream().map(i -> measurements[i]).toArray(String[]::new);
    }
    if (dataTypes != null) {
      dataTypes = columnsToKeep.stream().map(i -> dataTypes[i]).toArray(TSDataType[]::new);
    }
    if (columnCategories != null) {
      columnCategories =
          columnsToKeep.stream()
              .map(i -> columnCategories[i])
              .toArray(TsTableColumnCategory[]::new);
    }

    subRemoveAttributeColumns(columnsToKeep);

    // to reconstruct indices
    tagColumnIndices = null;
    attrColumnIndices = null;
    attributeColumnsPresent = false;
  }

  protected abstract void subRemoveAttributeColumns(List<Integer> columnsToKeep);

  public static class FailedMeasurementInfo {
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

  public void insertColumn(final int pos, final ColumnSchema columnSchema) {
    if (pos < 0 || pos > measurements.length) {
      throw new ArrayIndexOutOfBoundsException(pos);
    }

    if (measurementSchemas != null) {
      final MeasurementSchema[] tmp = new MeasurementSchema[measurementSchemas.length + 1];
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
      final TSDataType[] tmpTypes = new TSDataType[dataTypes.length + 1];
      System.arraycopy(dataTypes, 0, tmpTypes, 0, pos);
      tmpTypes[pos] = InternalTypeManager.getTSDataType(columnSchema.getType());
      System.arraycopy(dataTypes, pos, tmpTypes, pos + 1, dataTypes.length - pos);
      dataTypes = tmpTypes;
    }

    if (columnCategories == null) {
      columnCategories = new TsTableColumnCategory[measurements.length + 1];
      columnCategories[pos] = columnSchema.getColumnCategory();
    } else {
      final TsTableColumnCategory[] tmpCategories =
          new TsTableColumnCategory[columnCategories.length + 1];
      System.arraycopy(columnCategories, 0, tmpCategories, 0, pos);
      tmpCategories[pos] = columnSchema.getColumnCategory();
      System.arraycopy(
          columnCategories, pos, tmpCategories, pos + 1, columnCategories.length - pos);
      columnCategories = tmpCategories;
      tagColumnIndices = null;
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
    if (typeConvertors != null) {
      CommonUtils.swapArray(typeConvertors, src, target);
    }
    if (inputLocations != null) {
      CommonUtils.swapArray(inputLocations, src, target);
    }
    tagColumnIndices = null;
  }

  /**
   * The oldToNewMapping array provides the mapping from old array positions to new array positions:
   * newArray[oldToNewMapping[oldIdx]] = oldArray[oldIdx]
   *
   * @param oldToNewMapping maps each old index to its new position in the reorganized array
   */
  @TableModel
  public void rebuildArraysAfterExpansion(
      final int[] newToOldMapping, final String[] newMeasurements) {
    final int newLength = newToOldMapping.length;

    // Save old arrays
    final MeasurementSchema[] oldMeasurementSchemas = measurementSchemas;
    final TSDataType[] oldDataTypes = dataTypes;
    final TsTableColumnCategory[] oldColumnCategories = columnCategories;
    final Type[] oldTypeConvertors = typeConvertors;
    final InputLocation[] oldInputLocations = inputLocations;

    // Set new measurements array
    measurements = newMeasurements;

    // Create new arrays
    final MeasurementSchema[] newMeasurementSchemas = new MeasurementSchema[newLength];
    final TSDataType[] newDataTypes = new TSDataType[newLength];
    final TsTableColumnCategory[] newColumnCategories = new TsTableColumnCategory[newLength];
    final Type[] newTypeConvertors = typeConvertors != null ? new Type[newLength] : null;
    final InputLocation[] newInputLocations =
        oldInputLocations != null ? new InputLocation[newLength] : null;

    // Rebuild arrays using mapping: newToOldMapping[newIdx] = oldIdx
    // If oldIdx == -1, it's a missing TAG column, fill with default values
    for (int newIdx = 0; newIdx < newLength; newIdx++) {
      final int oldIdx = newToOldMapping[newIdx];
      if (oldIdx == -1) {
        // Missing TAG column, fill with default values
        final String columnName = newMeasurements[newIdx];
        newMeasurementSchemas[newIdx] = new MeasurementSchema(columnName, TSDataType.STRING);
        newDataTypes[newIdx] = TSDataType.STRING;
        newColumnCategories[newIdx] = TsTableColumnCategory.TAG;
        // typeConvertors and inputLocations remain null for missing columns
      } else {
        // Copy from old array
        if (oldMeasurementSchemas != null) {
          newMeasurementSchemas[newIdx] = oldMeasurementSchemas[oldIdx];
        }
        if (oldDataTypes != null) {
          newDataTypes[newIdx] = oldDataTypes[oldIdx];
        }
        if (oldColumnCategories != null) {
          newColumnCategories[newIdx] = oldColumnCategories[oldIdx];
        }
        if (oldTypeConvertors != null) {
          newTypeConvertors[newIdx] = oldTypeConvertors[oldIdx];
        }
        if (oldInputLocations != null) {
          newInputLocations[newIdx] = oldInputLocations[oldIdx];
        }
      }
    }

    // Replace old arrays with new arrays
    measurementSchemas = newMeasurementSchemas;
    dataTypes = newDataTypes;
    columnCategories = newColumnCategories;
    typeConvertors = newTypeConvertors;
    inputLocations = newInputLocations;

    // Clear cached indices
    tagColumnIndices = null;
    attrColumnIndices = null;
  }

  public boolean isWriteToTable() {
    return writeToTable;
  }

  public void setWriteToTable(final boolean writeToTable) {
    this.writeToTable = writeToTable;
    if (writeToTable) {
      isAligned = true;
    }
  }

  @TableModel
  public void setDatabaseName(final String databaseName) {
    this.databaseName = databaseName;
  }

  @TableModel
  public Optional<String> getDatabaseName() {
    return Optional.ofNullable(databaseName);
  }

  // endregion

  @TableModel
  public void toLowerCase() {
    if (toLowerCaseApplied) {
      return;
    }

    if (measurements == null) {
      return;
    }
    for (int i = 0; i < measurements.length; i++) {
      if (measurements[i] != null) {
        measurements[i] = measurements[i].toLowerCase();
      }
    }
    if (measurementSchemas != null) {
      for (final MeasurementSchema measurementSchema : measurementSchemas) {
        if (measurementSchema != null) {
          measurementSchema.setMeasurementName(
              measurementSchema.getMeasurementName().toLowerCase());
        }
      }
    }
    toLowerCaseApplied = true;
  }

  @TableModel
  public void toLowerCaseForDevicePath() {
    devicePath.toLowerCase();
  }

  @TableModel
  public List<String> getAttributeColumnNameList() {
    final List<String> attributeColumnNameList = new ArrayList<>();
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

  // Only Pipe will set the return value to true
  @TableModel
  public boolean isForceTypeConversion() {
    return false;
  }

  @Override
  public long ramBytesUsed() {
    if (ramBytesUsed > 0) {
      return ramBytesUsed;
    }
    ramBytesUsed =
        InsertNodeMemoryEstimator.sizeOfPartialPath(devicePath)
            + InsertNodeMemoryEstimator.sizeOfMeasurementSchemas(measurementSchemas)
            + RamUsageEstimator.sizeOf(measurements)
            + RamUsageEstimator.shallowSizeOf(dataTypes)
            + RamUsageEstimator.shallowSizeOf(columnCategories)
            // We assume that the integers are all cached by JVM
            + shallowSizeOfList(tagColumnIndices)
            + shallowSizeOfList(attrColumnIndices)
            + shallowSizeOfList(logicalViewSchemaList)
            + (Objects.nonNull(logicalViewSchemaList)
                ? logicalViewSchemaList.stream()
                    .mapToLong(LogicalViewSchema::ramBytesUsed)
                    .reduce(0L, Long::sum)
                : 0L)
            + shallowSizeOfList(indexOfSourcePathsOfLogicalViews)
            + RamUsageEstimator.sizeOf(databaseName)
            + calculateBytesUsed();
    return ramBytesUsed;
  }

  /**
   * Set the pre-calculated memory size. This is used when memory size is calculated during
   * deserialization to avoid recalculation.
   *
   * @param ramBytesUsed the calculated memory size in bytes
   */
  public void setRamBytesUsed(long ramBytesUsed) {
    this.ramBytesUsed = ramBytesUsed;
  }

  private long shallowSizeOfList(List<?> list) {
    return Objects.nonNull(list)
        ? UpdateDetailContainer.LIST_SIZE
            + RamUsageEstimator.alignObjectSize(
                RamUsageEstimator.NUM_BYTES_ARRAY_HEADER
                    + (long) RamUsageEstimator.NUM_BYTES_OBJECT_REF * list.size())
        : 0L;
  }

  public List<PartialPath> getDevicePaths() {
    return Collections.singletonList(devicePath);
  }

  protected abstract long calculateBytesUsed();
}
