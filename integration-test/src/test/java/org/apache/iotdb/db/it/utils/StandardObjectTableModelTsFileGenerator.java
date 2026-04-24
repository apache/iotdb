/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.it.utils;

import org.apache.iotdb.calc.utils.IObjectPath;
import org.apache.iotdb.calc.utils.ObjectTypeUtils;
import org.apache.iotdb.db.storageengine.dataregion.modification.DeletionPredicate;
import org.apache.iotdb.db.storageengine.dataregion.modification.IDPredicate;
import org.apache.iotdb.db.storageengine.dataregion.modification.ModificationFile;
import org.apache.iotdb.db.storageengine.dataregion.modification.TableDeletionEntry;

import com.timecho.iotdb.calc.storageengine.dataregion.Base32ObjectPath;
import org.apache.tsfile.enums.ColumnCategory;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.TableSchema;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.common.TimeRange;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.BitMap;
import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.apache.tsfile.write.TsFileWriter;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class StandardObjectTableModelTsFileGenerator implements AutoCloseable {

  private static final int DEFAULT_REGION_ID = 1;
  private static final String ID_COLUMN_NAME = "id";

  public static final List<IMeasurementSchema> FIXED_FIELD_SCHEMAS =
      Arrays.asList(
          new MeasurementSchema("sensor_obj", TSDataType.OBJECT, TSEncoding.PLAIN),
          new MeasurementSchema("sensor_int32", TSDataType.INT32, TSEncoding.RLE),
          new MeasurementSchema("sensor_int64", TSDataType.INT64, TSEncoding.RLE),
          new MeasurementSchema("sensor_float", TSDataType.FLOAT, TSEncoding.GORILLA),
          new MeasurementSchema("sensor_double", TSDataType.DOUBLE, TSEncoding.GORILLA),
          new MeasurementSchema("sensor_bool", TSDataType.BOOLEAN, TSEncoding.PLAIN),
          new MeasurementSchema("sensor_text", TSDataType.STRING, TSEncoding.PLAIN));

  private final TsFileWriter writer;
  private final ModificationFile modificationFile;
  private final File objectRoot;
  private final String tsFileName;

  private final Map<String, Tablet> tabletMap = new HashMap<>();

  private final List<IMeasurementSchema> allSchemas = new ArrayList<>();
  private final List<ColumnCategory> columnCategories = new ArrayList<>();
  private final List<String> columnNames = new ArrayList<>();
  private final List<TSDataType> dataTypes = new ArrayList<>();

  public StandardObjectTableModelTsFileGenerator(File tsFile) throws IOException {
    this.writer = new TsFileWriter(tsFile);
    this.modificationFile =
        new ModificationFile(tsFile.getAbsolutePath() + ModificationFile.FILE_SUFFIX, false);
    this.objectRoot = tsFile.getParentFile();
    this.tsFileName = tsFile.getName();

    allSchemas.add(new MeasurementSchema(ID_COLUMN_NAME, TSDataType.STRING, TSEncoding.PLAIN));
    columnCategories.add(ColumnCategory.TAG);
    columnNames.add(ID_COLUMN_NAME);
    dataTypes.add(TSDataType.STRING);

    for (IMeasurementSchema fieldSchema : FIXED_FIELD_SCHEMAS) {
      allSchemas.add(fieldSchema);
      columnCategories.add(ColumnCategory.FIELD);
      columnNames.add(fieldSchema.getMeasurementName());
      dataTypes.add(fieldSchema.getType());
    }
  }

  public void generateDeletion(String tableName, String id, long startTime, long endTime)
      throws IOException {
    IDeviceID deviceID = IDeviceID.Factory.DEFAULT_FACTORY.create(new String[] {tableName, id});

    TableDeletionEntry deletionEntry =
        new TableDeletionEntry(
            new DeletionPredicate(tableName, new IDPredicate.FullExactMatch(deviceID)),
            new TimeRange(startTime, endTime));

    modificationFile.write(deletionEntry);
  }

  public void generateDeletion(String tableName, String id, long endTime) throws IOException {
    generateDeletion(tableName, id, Long.MIN_VALUE, endTime);
  }

  // ----------------------------------------

  public long writeDeviceData(
      String tableName, String tagValue, long startTime, long endTime, long timeInterval)
      throws Exception {
    if (tableName == null || tableName.isEmpty() || tagValue == null || tagValue.isEmpty()) {
      throw new IllegalArgumentException("TableName and TagValue cannot be null or empty.");
    }

    Tablet tablet =
        tabletMap.computeIfAbsent(
            tableName,
            name -> {
              TableSchema tableSchema = new TableSchema(name, allSchemas, columnCategories);
              writer.registerTableSchema(tableSchema);
              return new Tablet(name, columnNames, dataTypes, columnCategories);
            });

    long writtenPoints = 0;

    IDeviceID deviceID =
        IDeviceID.Factory.DEFAULT_FACTORY.create(new String[] {tableName, tagValue});

    String dirName =
        tsFileName.endsWith(".tsfile")
            ? tsFileName.substring(0, tsFileName.length() - 7)
            : tsFileName;

    for (long currentTime = startTime; currentTime <= endTime; currentTime += timeInterval) {
      int row = tablet.getRowSize();
      tablet.addTimestamp(row, currentTime);
      tablet.addValue(ID_COLUMN_NAME, row, tagValue);

      int index = 1;
      for (IMeasurementSchema schema : FIXED_FIELD_SCHEMAS) {
        String measurement = schema.getMeasurementName();

        switch (schema.getType()) {
          case OBJECT:
            Base32ObjectPath objectPath =
                new Base32ObjectPath(DEFAULT_REGION_ID, currentTime, deviceID, measurement);

            String relativePathStr = dirName + File.separator + objectPath.getPath().toString();
            File objectFile = new File(objectRoot, relativePathStr);

            if (objectFile.getParentFile() != null && !objectFile.getParentFile().exists()) {
              Files.createDirectories(objectFile.getParentFile().toPath());
            }

            byte[] mockContent =
                String.format(
                        "AutoGenerated|Table=%s|ID=%s|Time=%d", tableName, tagValue, currentTime)
                    .getBytes(StandardCharsets.UTF_8);
            Files.write(objectFile.toPath(), mockContent);

            final Binary objectBinary =
                ObjectTypeUtils.generateObjectBinary(mockContent.length, objectPath);
            final Binary[] objectColumn = (Binary[]) tablet.getValues()[index];
            objectColumn[row] = objectBinary;
            final BitMap[] objBitMaps = tablet.getBitMaps();
            if (objBitMaps != null && objBitMaps[index] != null) {
              objBitMaps[index].unmark(row);
            }
            break;

          case INT32:
            tablet.addValue(measurement, row, (int) (currentTime % 100_000));
            break;
          case INT64:
            tablet.addValue(measurement, row, currentTime);
            break;
          case FLOAT:
            tablet.addValue(measurement, row, currentTime / 1000.0f);
            break;
          case DOUBLE:
            tablet.addValue(measurement, row, currentTime / 1000.0d);
            break;
          case BOOLEAN:
            tablet.addValue(measurement, row, (currentTime % 2) == 0);
            break;
          case TEXT:
          case STRING:
            tablet.addValue(measurement, row, "text_" + currentTime);
            break;
          default:
            break;
        }
        index++;
      }
      writtenPoints++;

      if (tablet.getRowSize() == tablet.getMaxRowNumber()) {
        writer.writeTable(tablet);
        tablet.reset();
      }
    }
    return writtenPoints;
  }

  @Override
  public void close() throws Exception {
    if (writer != null) {
      for (Tablet tablet : tabletMap.values()) {
        if (tablet.getRowSize() > 0) {
          writer.writeTable(tablet);
          tablet.reset();
        }
      }
      writer.close();
    }
    if (modificationFile != null) {
      modificationFile.close();
    }
  }

  public static Binary generateObjectBinary(long objectSize, IObjectPath objectPath) {
    byte[] valueBytes = new byte[objectPath.getSerializeSizeToObjectValue() + Long.BYTES];
    ByteBuffer buffer = ByteBuffer.wrap(valueBytes);
    ReadWriteIOUtils.write(objectSize, buffer);
    objectPath.serializeToObjectValue(buffer);
    return new Binary(buffer.array());
  }
}
