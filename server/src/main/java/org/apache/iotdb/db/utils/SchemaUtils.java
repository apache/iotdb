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
package org.apache.iotdb.db.utils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.Schema;


public class SchemaUtils {

  private SchemaUtils() {
  }

  /**
   * Construct the Schema of the FileNode named processorName.
   *
   * @param processorName the name of a FileNode.
   * @return the schema of the FileNode named processorName.
   */
  public static Schema constructSchema(String processorName) throws MetadataException {
    Set<String> devices = MManager.getInstance().getDevices(processorName);
    Map<Path, MeasurementSchema> measurementSchemaMap = new HashMap<>();
    for (String device : devices) {
      Map<String, MeasurementSchema> schema = MManager.getInstance().getDeviceSchemaMap(device);
      for (Entry<String, MeasurementSchema> entry : schema.entrySet()) {
        measurementSchemaMap.put(new Path(device, entry.getKey()), entry.getValue());
      }
    }
    return getSchemaFromColumnSchema(measurementSchemaMap);
  }

  /**
   * getSchemaFromColumnSchema construct a Schema using the schema of the columns and device type.
   *
   * @param schemaList the schema of the columns in this file.
   * @return a Schema contains the provided schemas.
   */
  public static Schema getSchemaFromColumnSchema(Map<Path, MeasurementSchema> schemaMap) {
    Schema schema = new Schema();
    for (Map.Entry<Path, MeasurementSchema> entry : schemaMap.entrySet()) {
      schema.registerTimeseries(entry.getKey(), entry.getValue());
    }
    return schema;
  }
}
