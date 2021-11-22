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
package org.apache.iotdb.tsfile.utils;

import org.apache.iotdb.tsfile.write.schema.UnaryMeasurementSchema;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MeasurementGroup {
  private boolean isAligned;
  private Map<String, UnaryMeasurementSchema> measurementSchemaMap;

  public MeasurementGroup(boolean isAligned) {
    this.isAligned = isAligned;
    measurementSchemaMap = new HashMap<>();
  }

  public MeasurementGroup(boolean isAligned, List<UnaryMeasurementSchema> measurementSchemas) {
    this.isAligned = isAligned;
    measurementSchemaMap = new HashMap<>();
    for (UnaryMeasurementSchema schema : measurementSchemas) {
      measurementSchemaMap.put(schema.getMeasurementId(), schema);
    }
  }

  public MeasurementGroup(
      boolean isAligned, Map<String, UnaryMeasurementSchema> measurementSchemaMap) {
    this.isAligned = isAligned;
    this.measurementSchemaMap = measurementSchemaMap;
  }

  public boolean isAligned() {
    return isAligned;
  }

  public void setAligned(boolean aligned) {
    isAligned = aligned;
  }

  public Map<String, UnaryMeasurementSchema> getMeasurementSchemaMap() {
    return measurementSchemaMap;
  }

  public void setMeasurementSchemaMap(Map<String, UnaryMeasurementSchema> measurementSchemaMap) {
    this.measurementSchemaMap = measurementSchemaMap;
  }
}
