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
package org.apache.tsfile.utils;

import org.apache.tsfile.write.schema.IMeasurementSchema;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MeasurementGroup implements Serializable {
  private boolean isAligned;
  private Map<String, IMeasurementSchema> measurementSchemaMap;

  public MeasurementGroup(boolean isAligned) {
    this.isAligned = isAligned;
    measurementSchemaMap = new HashMap<>();
  }

  public MeasurementGroup(boolean isAligned, List<IMeasurementSchema> measurementSchemas) {
    this.isAligned = isAligned;
    measurementSchemaMap = new HashMap<>();
    for (IMeasurementSchema schema : measurementSchemas) {
      measurementSchemaMap.put(schema.getMeasurementName(), schema);
    }
  }

  public MeasurementGroup(boolean isAligned, Map<String, IMeasurementSchema> measurementSchemaMap) {
    this.isAligned = isAligned;
    this.measurementSchemaMap = measurementSchemaMap;
  }

  public boolean isAligned() {
    return isAligned;
  }

  public void setAligned(boolean aligned) {
    isAligned = aligned;
  }

  public Map<String, IMeasurementSchema> getMeasurementSchemaMap() {
    return measurementSchemaMap;
  }

  public void setMeasurementSchemaMap(Map<String, IMeasurementSchema> measurementSchemaMap) {
    this.measurementSchemaMap = measurementSchemaMap;
  }
}
