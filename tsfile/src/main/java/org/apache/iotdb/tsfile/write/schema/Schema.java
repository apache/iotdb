/**
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

import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.write.record.RowBatch;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Schema stores the schema of the measurements and devices that exist in this file. All
 * devices written to the same TsFile shall have the same schema. Schema takes the JSON schema
 * file as a parameter and registers measurements in such JSON. Schema also records all existing
 * device IDs in this file.
 */
public class Schema {

  /**
   * the key is the measurementId.
   * By default, use the LinkedHashMap to store the order of insertion
   */
  private Map<String, MeasurementSchema> measurementSchemaMap;


  /**
   * init measurementSchemaMap as an empty map and an empty list.
   */
  public Schema() {
    this.measurementSchemaMap = new LinkedHashMap<>();
  }

  /**
   * Construct a Schema using provided schema map.
   * @param measurements a map whose key is the measurementId and value is the schema of
   *                     the measurement.
   */
  public Schema(Map<String, MeasurementSchema> measurements) {
    this();
    this.registerMeasurements(measurements);
  }

  /**
   * Construct a Schema using provided schema list.
   * @param measurements a list with schemas of measurements
   */
  public Schema(List<MeasurementSchema> measurements) {
    this();
    this.registerMeasurements(measurements);
  }

  /**
   * Construct a Schema using provided schema array.
   * @param measurements an array with schemas of measurements
   */
  public Schema(MeasurementSchema[] measurements) {
    this();
    this.registerMeasurements(measurements);
  }

  /**
   * Create a row batch to write aligned data
   * @param deviceId the name of the device specified to be written in
   */
  public RowBatch createRowBatch(String deviceId) {
    return new RowBatch(deviceId, new ArrayList<>(measurementSchemaMap.values()));
  }

  /**
   * Create a row batch to write aligned data
   * @param deviceId the name of the device specified to be written in
   * @param maxBatchSize max size of rows in batch
   */
  public RowBatch createRowBatch(String deviceId, int maxBatchSize) {
    return new RowBatch(deviceId, new ArrayList<>(measurementSchemaMap.values()), maxBatchSize);
  }

  /**
   * Get the data type fo a measurement specified by measurementId.
   * @param measurementId the name of the measurement being queried.
   */
  public TSDataType getMeasurementDataType(String measurementId) {
    MeasurementSchema measurement = this.measurementSchemaMap.get(measurementId);
    if (measurement == null) {
      return null;
    }
    return measurement.getType();

  }

  public MeasurementSchema getMeasurementSchema(String measurementId) {
    return measurementSchemaMap.get(measurementId);
  }

  public Map<String, MeasurementSchema> getMeasurementSchemaMap() {
    return measurementSchemaMap;
  }


  /**
   * register a measurement schema map.
   */
  public void registerMeasurement(MeasurementSchema descriptor) {
    // add to measurementSchemaMap as <measurementID, MeasurementSchema>
    this.measurementSchemaMap.put(descriptor.getMeasurementId(), descriptor);
  }

  /**
   * register all measurements in measurement schema map.
   */
  public void registerMeasurements(Map<String, MeasurementSchema> measurements) {
    measurements.forEach((id, md) -> registerMeasurement(md));
  }

  /**
   * register all measurements in measurement schema map.
   */
  public void registerMeasurements(List<MeasurementSchema> measurements) {
    measurements.forEach(this::registerMeasurement);
  }

  /**
   * register all measurements in measurement schema map.
   */
  public void registerMeasurements(MeasurementSchema[] measurements) {
    for (MeasurementSchema measurement : measurements) {
      registerMeasurement(measurement);
    }
  }

  /**
   * check if this schema contains a measurement named measurementId.
   */
  public boolean hasMeasurement(String measurementId) {
    return measurementSchemaMap.containsKey(measurementId);
  }

}
