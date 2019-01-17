/**
 * Copyright © 2019 Apache IoTDB(incubating) (dev@iotdb.apache.org)
 *
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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.iotdb.tsfile.write.schema;

import java.util.HashMap;
import java.util.Map;
import org.apache.iotdb.tsfile.exception.write.InvalidJsonSchemaException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * FileSchema stores the schema of the measurements and devices that exist in this file. All
 * devices written to the same TsFile shall have the same schema. FileSchema takes the JSON schema
 * file as a parameter and registers measurements in such JSON. FileSchema also records all existing
 * device IDs in this file.
 *
 * @author kangrong
 */
public class FileSchema {

  private static final Logger LOG = LoggerFactory.getLogger(FileSchema.class);

  /**
   * the key is the measurementId.
   */
  private Map<String, MeasurementSchema> measurementSchema;

  /**
   * init measurementSchema as an empty map.
   */
  public FileSchema() {
    this.measurementSchema = new HashMap<>();
  }

  /**
   * example: { "measurement_id": "sensor_cpu_50", "data_type": "INT32", "encoding": "RLE" }.
   * {"schema": [ { "measurement_id": "sensor_1", "data_type": "FLOAT", "encoding": "RLE" },
   * { "measurement_id": "sensor_2", "data_type": "INT32", "encoding": "TS_2DIFF" },
   * { "measurement_id": "sensor_3", "data_type": "INT32","encoding": "TS_2DIFF" } ] };
   *
   * @param jsonSchema
   *            file schema in json format
   */
  @Deprecated
  public FileSchema(JSONObject jsonSchema) throws InvalidJsonSchemaException {
    this(JsonConverter.converterJsonToMeasurementSchemas(jsonSchema));
  }

  /**
   * Construct a FileSchema using provided schema map.
   * @param measurements a map whose key is the measurementId and value is the schema of
   *                     the measurement.
   */
  public FileSchema(Map<String, MeasurementSchema> measurements) {
    this();
    this.registerMeasurements(measurements);
  }

  /**
   * Get the data type fo a measurement specified by measurementId.
   * @param measurementId the name of the measurement being queried.
   * @return
   */
  public TSDataType getMeasurementDataType(String measurementId) {
    MeasurementSchema measurementSchema = this.measurementSchema.get(measurementId);
    if (measurementSchema == null) {
      return null;
    }
    return measurementSchema.getType();

  }

  public MeasurementSchema getMeasurementSchema(String measurementId) {
    return measurementSchema.get(measurementId);
  }

  public Map<String, MeasurementSchema> getAllMeasurementSchema() {
    return measurementSchema;
  }

  /**
   * register a measurementSchema.
   */
  public void registerMeasurement(MeasurementSchema descriptor) {
    // add to measurementSchema as <measurementID, MeasurementSchema>
    this.measurementSchema.put(descriptor.getMeasurementId(), descriptor);
  }

  /**
   * register all measurementSchemas in measurements.
   */
  private void registerMeasurements(Map<String, MeasurementSchema> measurements) {
    measurements.forEach((id, md) -> registerMeasurement(md));
  }

  /**
   * check if this schema contains a measurement named measurementId.
   */
  public boolean hasMeasurement(String measurementId) {
    return measurementSchema.containsKey(measurementId);
  }

}
