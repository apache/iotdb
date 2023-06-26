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
import org.apache.iotdb.commons.schema.view.LogicalViewSchema;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.metadata.DataTypeMismatchException;
import org.apache.iotdb.db.exception.metadata.DuplicateInsertException;
import org.apache.iotdb.db.exception.metadata.PathNotExistException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.db.queryengine.plan.analyze.schema.ISchemaValidation;
import org.apache.iotdb.db.queryengine.plan.statement.Statement;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public abstract class InsertBaseStatement extends Statement {

  /**
   * if use id table, this filed is id form of device path <br>
   * if not, this filed is device path<br>
   */
  protected PartialPath devicePath;

  protected boolean isAligned;

  protected MeasurementSchema[] measurementSchemas;

  protected String[] measurements;
  // get from client
  protected TSDataType[] dataTypes;

  /** index of failed measurements -> info including measurement, data type and value */
  protected Map<Integer, FailedMeasurementInfo> failedMeasurementIndex2Info;

  // region params used by analyzing logical views.

  /** This param records the logical view schemaengine appeared in this statement. */
  List<LogicalViewSchema> logicalViewSchemaList;

  /**
   * This param records the index of the location where the source of this view should be placed.
   *
   * <p>For example, indexListOfLogicalViewPaths[alpha] = beta means source of
   * logicalViewSchemaList[alpha] should be filled into measurementSchemas[beta].
   */
  List<Integer> indexOfSourcePathsOfLogicalViews;

  /** it is the end of last range, the beginning of current range. */
  int recordedBeginOfLogicalViewSchemaList = 0;

  /** it is the end of current range. */
  int recordedEndOfLogicalViewSchemaList = 0;

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

  public boolean isAligned() {
    return isAligned;
  }

  public void setAligned(boolean aligned) {
    isAligned = aligned;
  }

  public TSDataType[] getDataTypes() {
    return dataTypes;
  }

  public void setDataTypes(TSDataType[] dataTypes) {
    this.dataTypes = dataTypes;
  }

  /** Returns true when this statement is empty and no need to write into the server */
  public abstract boolean isEmpty();

  @Override
  public List<PartialPath> getPaths() {
    return Collections.emptyList();
  }

  public abstract ISchemaValidation getSchemaValidation();

  public abstract List<ISchemaValidation> getSchemaValidationList();

  public void updateAfterSchemaValidation() throws QueryProcessException {}

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

  public boolean hasValidMeasurements() {
    for (Object o : measurements) {
      if (o != null) {
        return true;
      }
    }
    return false;
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
  protected final Map<PartialPath, List<Pair<String, Integer>>>
      getMapFromDeviceToMeasurementAndIndex() {
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
    // schemaengine
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
  // endregion
}
