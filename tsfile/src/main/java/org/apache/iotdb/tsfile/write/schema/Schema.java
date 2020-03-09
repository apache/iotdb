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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.write.record.RowBatch;

/**
 * The schema of timeseries that exist in this file. The deviceTemplates is a simplified manner to
 * batch create schema of timeseries.
 */
public class Schema {

  /**
   * Path (device + measurement) -> measurementSchema By default, use the LinkedHashMap to store the
   * order of insertion
   */
  private Map<Path, MeasurementSchema> registeredTimeseries;

  /**
   * template name -> (measuremnet -> MeasurementSchema)
   */
  private Map<String, Map<String, MeasurementSchema>> deviceTemplates;

  public Schema() {
    this.registeredTimeseries = new LinkedHashMap<>();
  }

  public Schema(Map<Path, MeasurementSchema> knownSchema) {
    this.registeredTimeseries = knownSchema;
  }

  /**
   * Create a row batch to write aligned data
   *
   * @param deviceId the name of the device specified to be written in
   */
  public RowBatch createRowBatch(String deviceId) {
    return new RowBatch(deviceId, new ArrayList<>(registeredTimeseries.values()));
  }

  /**
   * Create a row batch to write aligned data
   *
   * @param deviceId     the name of the device specified to be written in
   * @param maxBatchSize max size of rows in batch
   */
  public RowBatch createRowBatch(String deviceId, int maxBatchSize) {
    return new RowBatch(deviceId, new ArrayList<>(registeredTimeseries.values()), maxBatchSize);
  }

  public void registerTimeseries(Path path, MeasurementSchema descriptor) {
    this.registeredTimeseries.put(path, descriptor);
  }

  public void registerDeviceTemplate(String templateName, Map<String, MeasurementSchema> template) {
    if (deviceTemplates == null) {
      deviceTemplates = new HashMap<>();
    }
    this.deviceTemplates.put(templateName, template);
  }

  public void extendTemplate(String templateName, MeasurementSchema descriptor) {
    Map<String, MeasurementSchema> template = this.deviceTemplates
        .getOrDefault(templateName, new HashMap<>());
    template.put(descriptor.getMeasurementId(), descriptor);
    this.deviceTemplates.put(templateName, template);
  }

  public void registerDevice(String deviceId, String templateName) {
    if (!deviceTemplates.containsKey(templateName)) {
      return;
    }
    Map<String, MeasurementSchema> template = deviceTemplates.get(templateName);
    for (Map.Entry<String, MeasurementSchema> entry : template.entrySet()) {
      Path path = new Path(deviceId, entry.getKey());
      registerTimeseries(path, entry.getValue());
    }
  }

  public MeasurementSchema getSeriesSchema(Path path) {
    return registeredTimeseries.get(path);
  }

  public TSDataType getTimeseriesDataType(Path path) {
    if (!registeredTimeseries.containsKey(path)) {
      return null;
    }
    return registeredTimeseries.get(path).getType();
  }

  /**
   * check if this schema contains a measurement named measurementId.
   */
  public boolean containsTimeseries(Path path) {
    return registeredTimeseries.containsKey(path);
  }

  // for test
  public Map<Path, MeasurementSchema> getRegisteredTimeseriesMap() {
    return registeredTimeseries;
  }

}
