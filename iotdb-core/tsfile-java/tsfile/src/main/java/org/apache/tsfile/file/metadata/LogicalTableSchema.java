/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.tsfile.file.metadata;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.write.record.Tablet.ColumnCategory;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * TableSchema for devices with path-based DeviceIds. It generates the Id columns based on the max
 * level of paths.
 */
public class LogicalTableSchema extends TableSchema {

  private int maxLevel;

  public LogicalTableSchema(String tableName) {
    super(tableName);
  }

  @Override
  public void update(ChunkGroupMetadata chunkGroupMetadata) {
    super.update(chunkGroupMetadata);
    this.maxLevel = Math.max(this.maxLevel, chunkGroupMetadata.getDevice().segmentNum());
  }

  private List<IMeasurementSchema> generateIdColumns() {
    List<IMeasurementSchema> generatedIdColumns = new ArrayList<>();
    // level 0 is table name, not id column
    for (int i = 1; i < maxLevel; i++) {
      generatedIdColumns.add(
          new MeasurementSchema(
              "__level" + i, TSDataType.STRING, TSEncoding.PLAIN, CompressionType.UNCOMPRESSED));
    }
    return generatedIdColumns;
  }

  public void finalizeColumnSchema() {
    if (!updatable) {
      return;
    }

    List<IMeasurementSchema> allColumns = new ArrayList<>(generateIdColumns());
    List<ColumnCategory> allColumnCategories =
        ColumnCategory.nCopy(ColumnCategory.ID, allColumns.size());
    allColumns.addAll(columnSchemas);
    allColumnCategories.addAll(columnCategories);
    columnSchemas = allColumns;
    columnCategories = allColumnCategories;
    updatable = false;
  }

  @Override
  public int serialize(OutputStream out) throws IOException {
    finalizeColumnSchema();
    return super.serialize(out);
  }
}
