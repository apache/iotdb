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

package org.apache.tsfile.read;

import org.apache.tsfile.constant.TestConstant;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.TableSchema;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.common.TimeRange;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.write.chunk.AlignedChunkWriterImpl;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.apache.tsfile.write.writer.TsFileIOWriter;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class TsFileDeviceIteratorTest {
  private static final String FILE_PATH =
      TestConstant.BASE_OUTPUT_PATH.concat("TsFileDeviceIterator.tsfile");

  @After
  public void teardown() {
    new File(FILE_PATH).delete();
  }

  @Test
  public void test() throws IOException {
    int totalDeviceNum = 0;
    try (TsFileIOWriter writer = new TsFileIOWriter(new File(FILE_PATH))) {
      for (int i = 1; i <= 10; i++) {
        String tableName = "table" + i;
        registerTableSchema(writer, tableName);
        int deviceNum = i;
        if (i % 2 == 0) {
          deviceNum *= 10000;
        } else {
          deviceNum *= 10;
        }
        totalDeviceNum += deviceNum;
        generateDevice(writer, tableName, deviceNum);
      }
      writer.endFile();
    }
    int deviceFromIterator = 0;
    try (TsFileSequenceReader reader = new TsFileSequenceReader(FILE_PATH)) {
      TsFileDeviceIterator deviceIterator = reader.getAllDevicesIteratorWithIsAligned();
      IDeviceID previous = null;
      while (deviceIterator.hasNext()) {
        Pair<IDeviceID, Boolean> next = deviceIterator.next();
        deviceFromIterator++;
        if (previous != null) {
          Assert.assertTrue(previous.compareTo(next.getLeft()) < 0);
        }
        previous = next.getLeft();
      }
    }
    Assert.assertEquals(totalDeviceNum, deviceFromIterator);
  }

  private void registerTableSchema(TsFileIOWriter writer, String tableName) {
    List<IMeasurementSchema> schemas =
        Arrays.asList(
            new MeasurementSchema(
                "id", TSDataType.TEXT, TSEncoding.PLAIN, CompressionType.UNCOMPRESSED),
            new MeasurementSchema("s1", TSDataType.INT64),
            new MeasurementSchema("s2", TSDataType.INT64),
            new MeasurementSchema("s3", TSDataType.INT64),
            new MeasurementSchema("s4", TSDataType.INT64));
    List<Tablet.ColumnCategory> columnCategories =
        Arrays.asList(
            Tablet.ColumnCategory.ID,
            Tablet.ColumnCategory.MEASUREMENT,
            Tablet.ColumnCategory.MEASUREMENT,
            Tablet.ColumnCategory.MEASUREMENT,
            Tablet.ColumnCategory.MEASUREMENT);
    TableSchema tableSchema = new TableSchema(tableName, schemas, columnCategories);
    writer.getSchema().registerTableSchema(tableSchema);
  }

  private void generateDevice(TsFileIOWriter writer, String tableName, int deviceNum)
      throws IOException {
    for (int i = 0; i < deviceNum; i++) {
      IDeviceID deviceID =
          IDeviceID.Factory.DEFAULT_FACTORY.create(new String[] {tableName, "d" + i});
      writer.startChunkGroup(deviceID);
      generateSimpleAlignedSeriesToCurrentDevice(
          writer, Arrays.asList("s1", "s2", "s3", "s4"), new TimeRange[] {new TimeRange(10, 20)});
      writer.endChunkGroup();
    }
  }

  public void generateSimpleAlignedSeriesToCurrentDevice(
      TsFileIOWriter writer, List<String> measurementNames, TimeRange[] toGenerateChunkTimeRanges)
      throws IOException {
    List<IMeasurementSchema> measurementSchemas = new ArrayList<>();
    for (String measurementName : measurementNames) {
      measurementSchemas.add(
          new MeasurementSchema(
              measurementName, TSDataType.INT64, TSEncoding.RLE, CompressionType.LZ4));
    }
    for (TimeRange toGenerateChunk : toGenerateChunkTimeRanges) {
      AlignedChunkWriterImpl alignedChunkWriter = new AlignedChunkWriterImpl(measurementSchemas);
      for (long time = toGenerateChunk.getMin(); time <= toGenerateChunk.getMax(); time++) {
        alignedChunkWriter.getTimeChunkWriter().write(time);
        for (int i = 0; i < measurementNames.size(); i++) {
          alignedChunkWriter.getValueChunkWriterByIndex(i).write(time, time, false);
        }
      }
      alignedChunkWriter.writeToFileWriter(writer);
    }
  }
}
