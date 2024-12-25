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

package org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils;

import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;

import org.apache.tsfile.file.metadata.TableSchema;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.write.schema.Schema;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class CompactionTableSchemaCollector {
  private CompactionTableSchemaCollector() {}

  public static List<Schema> collectSchema(
      List<TsFileResource> seqFiles,
      List<TsFileResource> unseqFiles,
      Map<TsFileResource, TsFileSequenceReader> readerMap)
      throws IOException {
    List<Schema> targetSchemas = new ArrayList<>(seqFiles.size());
    Schema schema =
        collectSchema(
            Stream.concat(seqFiles.stream(), unseqFiles.stream()).collect(Collectors.toList()),
            readerMap);

    targetSchemas.add(schema);
    for (int i = 1; i < seqFiles.size(); i++) {
      Schema copySchema = copySchema(schema);
      targetSchemas.add(copySchema);
    }
    return targetSchemas;
  }

  public static Schema copySchema(Schema source) {
    Schema copySchema = new Schema();
    for (TableSchema tableSchema : source.getTableSchemaMap().values()) {
      copySchema.registerTableSchema(((CompactionTableSchema) tableSchema).copy());
    }
    return copySchema;
  }

  public static Schema collectSchema(
      List<TsFileResource> sourceFiles, Map<TsFileResource, TsFileSequenceReader> readerMap)
      throws IOException {
    Schema targetSchema = new Schema();
    Map<String, TableSchema> targetTableSchemaMap = new HashMap<>();
    for (TsFileResource resource : sourceFiles) {
      TsFileSequenceReader reader = readerMap.get(resource);
      Map<String, TableSchema> tableSchemaMap = reader.readFileMetadata().getTableSchemaMap();
      if (tableSchemaMap == null) {
        // v3 tsfile
        continue;
      }
      for (Map.Entry<String, TableSchema> entry : tableSchemaMap.entrySet()) {
        String tableName = entry.getKey();
        TableSchema currentTableSchema = entry.getValue();
        if (isTreeModel(currentTableSchema)) {
          continue;
        }
        // merge all id columns, measurement schema will be generated automatically when end chunk
        // group
        CompactionTableSchema collectedTableSchema =
            (CompactionTableSchema) targetTableSchemaMap.get(tableName);
        if (collectedTableSchema == null) {
          collectedTableSchema = new CompactionTableSchema(tableName);
          targetTableSchemaMap.put(tableName, collectedTableSchema);
        }
        collectedTableSchema.merge(currentTableSchema);
      }
    }
    targetTableSchemaMap.values().forEach(targetSchema::registerTableSchema);
    return targetSchema;
  }

  private static boolean isTreeModel(TableSchema tableSchema) {
    return tableSchema == null;
  }
}
