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
package org.apache.iotdb.db.metadata.tagSchemaRegion.utils;

import org.apache.iotdb.db.metadata.idtable.entry.SchemaEntry;
import org.apache.iotdb.db.query.dataset.ShowTimeSeriesResult;

import java.util.HashMap;

public class ShowTimeSeriesResultUtils {
  public static ShowTimeSeriesResult generateShowTimeSeriesResult(
      String sgName, String devicePath, String measurement, SchemaEntry schemaEntry) {
    return new ShowTimeSeriesResult(
        devicePath + "." + measurement,
        null,
        sgName,
        schemaEntry.getTSDataType(),
        schemaEntry.getTSEncoding(),
        schemaEntry.getCompressionType(),
        schemaEntry.getLastTime(),
        new HashMap<>(),
        new HashMap<>());
  }

  public static ShowTimeSeriesResult generateShowTimeSeriesResult(
      String sgName, String timeSeriesPath, SchemaEntry schemaEntry) {
    return new ShowTimeSeriesResult(
        timeSeriesPath,
        null,
        sgName,
        schemaEntry.getTSDataType(),
        schemaEntry.getTSEncoding(),
        schemaEntry.getCompressionType(),
        schemaEntry.getLastTime(),
        new HashMap<>(),
        new HashMap<>());
  }
}
