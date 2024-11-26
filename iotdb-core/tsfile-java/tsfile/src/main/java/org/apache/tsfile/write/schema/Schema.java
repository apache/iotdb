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
package org.apache.tsfile.write.schema;

import org.apache.tsfile.file.metadata.ChunkGroupMetadata;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.LogicalTableSchema;
import org.apache.tsfile.file.metadata.TableSchema;
import org.apache.tsfile.read.common.Path;
import org.apache.tsfile.utils.MeasurementGroup;

import java.io.Serializable;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * The schema of timeseries that exist in this file. The schemaTemplates is a simplified manner to
 * batch create schema of timeseries.
 */
public class Schema implements Serializable {

  /**
   * IDeviceID -> measurementSchema By default, use the LinkedHashMap to store the order of
   * insertion
   */
  private Map<IDeviceID, MeasurementGroup> registeredTimeseries;

  /** template name -> (measurement -> MeasurementSchema) */
  private Map<String, MeasurementGroup> schemaTemplates;

  private Map<String, TableSchema> tableSchemaMap = new HashMap<>();
  private boolean enabledUpdateSchema = true;

  public Schema() {
    this.registeredTimeseries = new LinkedHashMap<>();
  }

  public Schema(Map<IDeviceID, MeasurementGroup> knownSchema) {
    this.registeredTimeseries = knownSchema;
  }

  @Deprecated
  public void registerTimeseries(Path devicePath, IMeasurementSchema measurementSchema) {
    registerTimeseries(devicePath.getIDeviceID(), measurementSchema);
  }

  // This method can only register nonAligned timeseries.
  public void registerTimeseries(IDeviceID deviceID, IMeasurementSchema measurementSchema) {
    MeasurementGroup group =
        registeredTimeseries.getOrDefault(deviceID, new MeasurementGroup(false));
    group.getMeasurementSchemaMap().put(measurementSchema.getMeasurementName(), measurementSchema);
    this.registeredTimeseries.put(deviceID, group);
  }

  @Deprecated
  public void registerMeasurementGroup(Path devicePath, MeasurementGroup measurementGroup) {
    this.registeredTimeseries.put(devicePath.getIDeviceID(), measurementGroup);
  }

  public void registerMeasurementGroup(IDeviceID deviceID, MeasurementGroup measurementGroup) {
    this.registeredTimeseries.put(deviceID, measurementGroup);
  }

  public void registerSchemaTemplate(String templateName, MeasurementGroup measurementGroup) {
    if (schemaTemplates == null) {
      schemaTemplates = new HashMap<>();
    }
    this.schemaTemplates.put(templateName, measurementGroup);
  }

  public void registerTableSchema(TableSchema tableSchema) {
    tableSchemaMap.put(tableSchema.getTableName(), tableSchema);
  }

  /** If template does not exist, an nonAligned timeseries is created by default */
  public void extendTemplate(String templateName, MeasurementSchema descriptor) {
    if (schemaTemplates == null) {
      schemaTemplates = new HashMap<>();
    }
    MeasurementGroup measurementGroup =
        this.schemaTemplates.getOrDefault(
            templateName, new MeasurementGroup(false, new HashMap<>()));
    measurementGroup.getMeasurementSchemaMap().put(descriptor.getMeasurementName(), descriptor);
    this.schemaTemplates.put(templateName, measurementGroup);
  }

  public void registerDevice(String deviceIdString, String templateName) {
    registerDevice(IDeviceID.Factory.DEFAULT_FACTORY.create(deviceIdString), templateName);
  }

  public void registerDevice(IDeviceID deviceId, String templateName) {
    if (!schemaTemplates.containsKey(templateName)) {
      return;
    }
    Map<String, IMeasurementSchema> template =
        schemaTemplates.get(templateName).getMeasurementSchemaMap();
    boolean isAligned = schemaTemplates.get(templateName).isAligned();
    registerMeasurementGroup(deviceId, new MeasurementGroup(isAligned, template));
  }

  @Deprecated
  public MeasurementGroup getSeriesSchema(Path devicePath) {
    return registeredTimeseries.get(devicePath.getIDeviceID());
  }

  public MeasurementGroup getSeriesSchema(IDeviceID devicePath) {
    return registeredTimeseries.get(devicePath);
  }

  public Map<String, MeasurementGroup> getSchemaTemplates() {
    return schemaTemplates;
  }

  /** check if this schema contains a measurement named measurementId. */
  public boolean containsDevice(IDeviceID devicePath) {
    return registeredTimeseries.containsKey(devicePath);
  }

  public void setRegisteredTimeseries(Map<IDeviceID, MeasurementGroup> registeredTimeseries) {
    this.registeredTimeseries = registeredTimeseries;
  }

  // for test
  public Map<IDeviceID, MeasurementGroup> getRegisteredTimeseriesMap() {
    return registeredTimeseries;
  }

  public void updateTableSchema(ChunkGroupMetadata chunkGroupMetadata) {
    if (!enabledUpdateSchema) {
      return;
    }
    IDeviceID deviceID = chunkGroupMetadata.getDevice();
    String tableName = deviceID.getTableName();
    TableSchema tableSchema = tableSchemaMap.computeIfAbsent(tableName, LogicalTableSchema::new);
    tableSchema.update(chunkGroupMetadata);
  }

  public Map<String, TableSchema> getTableSchemaMap() {
    return tableSchemaMap;
  }

  public boolean isEnabledUpdateSchema() {
    return enabledUpdateSchema;
  }

  public void setEnabledUpdateSchema(boolean enabledUpdateSchema) {
    this.enabledUpdateSchema = enabledUpdateSchema;
  }
}
