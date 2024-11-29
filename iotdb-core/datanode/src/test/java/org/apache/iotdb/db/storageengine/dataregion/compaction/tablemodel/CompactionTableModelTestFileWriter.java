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

package org.apache.iotdb.db.storageengine.dataregion.compaction.tablemodel;

import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.CompactionTableSchema;
import org.apache.iotdb.db.storageengine.dataregion.compaction.utils.CompactionTestFileWriter;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.StringArrayDeviceID;
import org.apache.tsfile.file.metadata.TableSchema;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.write.record.Tablet.ColumnCategory;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.apache.tsfile.write.schema.Schema;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class CompactionTableModelTestFileWriter extends CompactionTestFileWriter {
  private Schema schema;

  public CompactionTableModelTestFileWriter(TsFileResource emptyFile) throws IOException {
    super(emptyFile);
    schema = new Schema();
    fileWriter.setSchema(schema);
  }

  public void registerTableSchema(String tableName, List<String> idColumnNames) {
    CompactionTableSchema tableSchema = new CompactionTableSchema(tableName);
    List<IMeasurementSchema> measurementSchemas = new ArrayList<>();
    List<ColumnCategory> columnTypes = new ArrayList<>();
    for (String idColumnName : idColumnNames) {
      measurementSchemas.add(
          new MeasurementSchema(
              idColumnName, TSDataType.TEXT, TSEncoding.PLAIN, CompressionType.UNCOMPRESSED));
      columnTypes.add(ColumnCategory.ID);
    }
    tableSchema.merge(new TableSchema(tableName, measurementSchemas, columnTypes));
    schema.registerTableSchema(tableSchema);
  }

  public IDeviceID startChunkGroup(String tableName, List<String> idColumnFields)
      throws IOException {
    int idx = 0;
    String[] segments = new String[idColumnFields.size() + 1];
    segments[idx++] = tableName;
    for (String idField : idColumnFields) {
      segments[idx++] = idField;
    }
    currentDeviceId = new StringArrayDeviceID(segments);
    fileWriter.startChunkGroup(currentDeviceId);
    currentDeviceStartTime = Long.MAX_VALUE;
    currentDeviceEndTime = Long.MIN_VALUE;
    return currentDeviceId;
  }
}
