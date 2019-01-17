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
package org.apache.iotdb.db.utils;

import java.util.List;
import java.util.Map.Entry;
import org.apache.iotdb.db.metadata.ColumnSchema;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.tsfile.common.constant.JsonFormatConstant;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.write.schema.FileSchema;
import org.json.JSONArray;
import org.json.JSONObject;


public class FileSchemaUtils {

  /**
   * Construct the FileSchema of the FileNode named processorName.
   * @param processorName the name of a FileNode.
   * @return the schema of the FileNode named processorName.
   * @throws WriteProcessException when the fileSchema cannot be created.
   */
  public static FileSchema constructFileSchema(String processorName) throws WriteProcessException {

    List<ColumnSchema> columnSchemaList;
    columnSchemaList = MManager.getInstance().getSchemaForFileName(processorName);

    FileSchema fileSchema = null;
    try {
      fileSchema = getFileSchemaFromColumnSchema(columnSchemaList, processorName);
    } catch (WriteProcessException e) {
      throw e;
    }
    return fileSchema;

  }

  /**
   * getFileSchemaFromColumnSchema construct a FileSchema using the schema of the columns and the
   * device type.
   * @param schemaList the schema of the columns in this file.
   * @param deviceType the name of the FileNode.
   * @return a FileSchema contains the provided schemas.
   * @throws WriteProcessException when the FileSchema cannot be constructed.
   */
  public static FileSchema getFileSchemaFromColumnSchema(List<ColumnSchema> schemaList,
      String deviceType)
      throws WriteProcessException {
    // TODO: is using a JSON as the media necessary?
    JSONArray rowGroup = new JSONArray();

    for (ColumnSchema col : schemaList) {
      JSONObject measurement = new JSONObject();
      measurement.put(JsonFormatConstant.MEASUREMENT_UID, col.name);
      measurement.put(JsonFormatConstant.DATA_TYPE, col.dataType.toString());
      measurement.put(JsonFormatConstant.MEASUREMENT_ENCODING, col.encoding.toString());
      for (Entry<String, String> entry : col.getArgsMap().entrySet()) {
        if (JsonFormatConstant.ENUM_VALUES.equals(entry.getKey())) {
          String[] valueArray = entry.getValue().split(",");
          measurement.put(JsonFormatConstant.ENUM_VALUES, new JSONArray(valueArray));
        } else {
          measurement.put(entry.getKey(), entry.getValue());
        }
      }
      rowGroup.put(measurement);
    }
    JSONObject jsonSchema = new JSONObject();
    jsonSchema.put(JsonFormatConstant.JSON_SCHEMA, rowGroup);
    jsonSchema.put(JsonFormatConstant.DELTA_TYPE, deviceType);
    return new FileSchema(jsonSchema);
  }

}
