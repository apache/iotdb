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

import java.io.Serializable;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.utils.MeasurementGroup;

/**
 * The schema of timeseries that exist in this file. The schemaTemplates is a simplified manner to
 * batch create schema of timeseries.
 */
public class Schema implements Serializable {

  /**
   * Path (devicePath + DeviceInfo) -> measurementSchema By default, use the LinkedHashMap to store
   * the order of insertion
   */
  private Map<Path, MeasurementGroup> registeredTimeseries;

  /** template name -> (measurement -> MeasurementSchema) */
  private Map<String, MeasurementGroup> schemaTemplates;

  public Schema() {
    this.registeredTimeseries = new LinkedHashMap<>();
  }

  public Schema(Map<Path, MeasurementGroup> knownSchema) {
    this.registeredTimeseries = knownSchema;
  }

  // public void registerAlignedTimeseries(Path )

  public void registerTimeseries(
      Path devicePath, IMeasurementSchema measurementSchema, boolean isAligned) {
    MeasurementGroup group =
        registeredTimeseries.getOrDefault(devicePath, new MeasurementGroup(isAligned));
    group.getMeasurementSchemaMap().put(measurementSchema.getMeasurementId(), measurementSchema);
  }

  public void registerTimeseries(Path timeSpath, IMeasurementSchema measurementSchema) { // Todo:

    /*if (this.registeredTimeseries.containsKey(devicePath)) {
         if (measurementGroup.isAligned()) {
           throw new WriteProcessException(
               "given aligned device has existed and should not be expanded! " + devicePath);
         } else {
           for (String measurementId : measurementGroup.getMeasurementSchemaMap().keySet()) {
             if (this.registeredTimeseries
                 .get(devicePath)
                 .getMeasurementSchemaMap()
                 .containsKey(measurementId)) {
               throw new WriteProcessException(
                   "given nonAligned timeseries has existed! " + (devicePath + "." +
    measurementId));
             }
           }
         }
       }*/
  }

  public void registerTimeseries(Path devicePath, MeasurementGroup measurementGroup) {
    this.registeredTimeseries.put(devicePath, measurementGroup);
  }

  public void registerSchemaTemplate(String templateName, MeasurementGroup measurementGroup) {
    if (schemaTemplates == null) {
      schemaTemplates = new HashMap<>();
    }
    this.schemaTemplates.put(templateName, measurementGroup);
  }

  /**
   * If template does not exist, an nonAligned timeseries is created by default
   *
   * @param templateName
   * @param descriptor
   */
  public void extendTemplate(String templateName, IMeasurementSchema descriptor) {
    if (schemaTemplates == null) {
      schemaTemplates = new HashMap<>();
    }
    MeasurementGroup measurementGroup =
        this.schemaTemplates.getOrDefault(
            templateName, new MeasurementGroup(false, new HashMap<String, IMeasurementSchema>()));
    measurementGroup.getMeasurementSchemaMap().put(descriptor.getMeasurementId(), descriptor);
    this.schemaTemplates.put(templateName, measurementGroup);
  }

  public void registerDevice(String deviceId, String templateName) {
    if (!schemaTemplates.containsKey(templateName)) {
      return;
    }
    Map<String, IMeasurementSchema> template =
        schemaTemplates.get(templateName).getMeasurementSchemaMap();
    boolean isAligned = schemaTemplates.get(templateName).isAligned();
    registerTimeseries(new Path(deviceId), new MeasurementGroup(isAligned, template));
  }

  public MeasurementGroup getSeriesSchema(Path devicePath) {
    return registeredTimeseries.get(devicePath);
  }

  public TSDataType getTimeseriesDataType(Path path) {
    Path devicePath = new Path(path.getDevice());
    if (!registeredTimeseries.containsKey(devicePath)) {
      return null;
    }
    return registeredTimeseries
        .get(devicePath)
        .getMeasurementSchemaMap()
        .get(path.getMeasurement())
        .getType();
  }

  public Map<String, MeasurementGroup> getSchemaTemplates() {
    return schemaTemplates;
  }

  /** check if this schema contains a measurement named measurementId. */
  public boolean containsTimeseries(Path devicePath) {
    return registeredTimeseries.containsKey(devicePath);
  }

  // for test
  public Map<Path, MeasurementGroup> getRegisteredTimeseriesMap() {
    return registeredTimeseries;
  }
}
