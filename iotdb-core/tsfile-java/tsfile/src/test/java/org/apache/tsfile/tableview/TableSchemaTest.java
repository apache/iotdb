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

package org.apache.tsfile.tableview;

import org.apache.tsfile.compatibility.DeserializeConfig;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.ChunkGroupMetadata;
import org.apache.tsfile.file.metadata.ChunkMetadata;
import org.apache.tsfile.file.metadata.LogicalTableSchema;
import org.apache.tsfile.file.metadata.StringArrayDeviceID;
import org.apache.tsfile.file.metadata.TableSchema;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.write.record.Tablet.ColumnCategory;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;

import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static org.apache.tsfile.write.record.Tablet.ColumnCategory.ID;
import static org.apache.tsfile.write.record.Tablet.ColumnCategory.MEASUREMENT;
import static org.junit.Assert.assertEquals;

public class TableSchemaTest {

  private String tableName = "test_table";
  private int idSchemaCnt = 5;
  private int measurementSchemaCnt = 5;

  public static List<IMeasurementSchema> prepareIdSchemas(int schemaNum) {
    List<IMeasurementSchema> schemas = new ArrayList<>();
    for (int i = 0; i < schemaNum; i++) {
      final MeasurementSchema measurementSchema =
          new MeasurementSchema(
              "__level" + i, TSDataType.TEXT, TSEncoding.PLAIN, CompressionType.UNCOMPRESSED);
      schemas.add(measurementSchema);
    }
    return schemas;
  }

  public static List<IMeasurementSchema> prepareMeasurementSchemas(int schemaNum) {
    List<IMeasurementSchema> schemas = new ArrayList<>();
    for (int i = 0; i < schemaNum; i++) {
      final MeasurementSchema measurementSchema =
          new MeasurementSchema(
              "s" + i, TSDataType.INT64, TSEncoding.PLAIN, CompressionType.UNCOMPRESSED);
      schemas.add(measurementSchema);
    }
    return schemas;
  }

  @Test
  public void testTableSchema() throws IOException {
    final List<IMeasurementSchema> measurementSchemas = prepareIdSchemas(idSchemaCnt);
    measurementSchemas.addAll(prepareMeasurementSchemas(measurementSchemaCnt));
    final List<ColumnCategory> columnCategories = ColumnCategory.nCopy(ID, idSchemaCnt);
    columnCategories.addAll(ColumnCategory.nCopy(MEASUREMENT, measurementSchemaCnt));
    TableSchema tableSchema = new TableSchema(tableName, measurementSchemas, columnCategories);

    try (ByteArrayOutputStream stream = new ByteArrayOutputStream()) {
      tableSchema.serialize(stream);
      final ByteBuffer buffer = ByteBuffer.wrap(stream.toByteArray());
      final TableSchema deserialized = TableSchema.deserialize(buffer, new DeserializeConfig());
      deserialized.setTableName(tableName);
      assertEquals(tableSchema, deserialized);
    }
  }

  @Test
  public void testLogicalTableSchema() throws IOException {
    TableSchema tableSchema = new LogicalTableSchema(tableName);
    for (int i = 0; i < measurementSchemaCnt; i++) {
      List<ChunkMetadata> chunkMetadataList = new ArrayList<>();
      for (int chunkNum = 0; chunkNum <= i; chunkNum++) {
        chunkMetadataList.add(
            new ChunkMetadata(
                "s" + chunkNum,
                TSDataType.INT64,
                TSEncoding.PLAIN,
                CompressionType.UNCOMPRESSED,
                0,
                null));
      }
      ChunkGroupMetadata groupMetadata =
          new ChunkGroupMetadata(
              new StringArrayDeviceID("root.a.b" + ".c.d" + i), chunkMetadataList);
      tableSchema.update(groupMetadata);
    }
    assertEquals(measurementSchemaCnt, tableSchema.getColumnSchemas().size());

    try (ByteArrayOutputStream stream = new ByteArrayOutputStream()) {
      tableSchema.serialize(stream);
      final ByteBuffer buffer = ByteBuffer.wrap(stream.toByteArray());
      final TableSchema deserialized = TableSchema.deserialize(buffer, new DeserializeConfig());
      deserialized.setTableName(tableName);
      assertEquals(tableSchema, deserialized);
      assertEquals(measurementSchemaCnt + 2, deserialized.getColumnSchemas().size());
    }
  }
}
