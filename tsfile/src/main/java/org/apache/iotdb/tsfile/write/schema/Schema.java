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
package org.apache.iotdb.tsfile.write.schema;

import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.utils.MeasurementGroup;

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
   * Path (devicePath) -> measurementSchema By default, use the LinkedHashMap to store the order of
   * insertion
   */
  private final Map<Path, MeasurementGroup> registeredTimeseries;

  /** template name -> (measurement -> MeasurementSchema) */
  private Map<String, MeasurementGroup> schemaTemplates;

  public Schema() {
    this.registeredTimeseries = new LinkedHashMap<>();
  }

  public Schema(Map<Path, MeasurementGroup> knownSchema) {
    this.registeredTimeseries = knownSchema;
  }

  // This method can only register nonAligned timeseries.
  public void registerTimeseries(Path devicePath, UnaryMeasurementSchema measurementSchema) {
    MeasurementGroup group =
        registeredTimeseries.getOrDefault(devicePath, new MeasurementGroup(false));
    group.getMeasurementSchemaMap().put(measurementSchema.getMeasurementId(), measurementSchema);
    this.registeredTimeseries.put(devicePath, group);
  }

  public void registerMeasurementGroup(Path devicePath, MeasurementGroup measurementGroup) {
    this.registeredTimeseries.put(devicePath, measurementGroup);
  }

  public void registerSchemaTemplate(String templateName, MeasurementGroup measurementGroup) {
    if (schemaTemplates == null) {
      schemaTemplates = new HashMap<>();
    }
    this.schemaTemplates.put(templateName, measurementGroup);
  }

  /** If template does not exist, an nonAligned timeseries is created by default */
  public void extendTemplate(String templateName, UnaryMeasurementSchema descriptor) {
    if (schemaTemplates == null) {
      schemaTemplates = new HashMap<>();
    }
    MeasurementGroup measurementGroup =
        this.schemaTemplates.getOrDefault(
            templateName, new MeasurementGroup(false, new HashMap<>()));
    measurementGroup.getMeasurementSchemaMap().put(descriptor.getMeasurementId(), descriptor);
    this.schemaTemplates.put(templateName, measurementGroup);
  }

  public void registerDevice(String deviceId, String templateName) {
    if (!schemaTemplates.containsKey(templateName)) {
      return;
    }
    Map<String, UnaryMeasurementSchema> template =
        schemaTemplates.get(templateName).getMeasurementSchemaMap();
    boolean isAligned = schemaTemplates.get(templateName).isAligned();
    registerMeasurementGroup(new Path(deviceId), new MeasurementGroup(isAligned, template));
  }

  public MeasurementGroup getSeriesSchema(Path devicePath) {
    return registeredTimeseries.get(devicePath);
  }

  public Map<String, MeasurementGroup> getSchemaTemplates() {
    return schemaTemplates;
  }

  /** check if this schema contains a measurement named measurementId. */
  public boolean containsDevice(Path devicePath) {
    return registeredTimeseries.containsKey(devicePath);
  }

  // for test
  public Map<Path, MeasurementGroup> getRegisteredTimeseriesMap() {
    return registeredTimeseries;
  }
}
