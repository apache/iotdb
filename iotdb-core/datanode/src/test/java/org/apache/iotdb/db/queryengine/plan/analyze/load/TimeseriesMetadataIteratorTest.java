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

package org.apache.iotdb.db.queryengine.plan.analyze.load;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.TimeseriesMetadata;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.read.TsFileSequenceReaderTimeseriesMetadataIterator;
import org.apache.tsfile.write.TsFileWriter;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class TimeseriesMetadataIteratorTest {

  private static final int DEVICE_COUNT = 1000;

  private static final int COLUMN_COUNT = 3;

  private static final long FIXED_TIMESTAMP = 1L;

  @Test
  public void testTimeseriesMetadataIterator() throws Exception {
    String outputPath = "testTsFile.tsfile";
    File file = createTsFile(outputPath);

    IDeviceID iDeviceID = null;
    Set<IDeviceID> set = new HashSet<>();
    try (TsFileSequenceReader reader = new TsFileSequenceReader(file.getAbsolutePath())) {
      TsFileSequenceReaderTimeseriesMetadataIterator iterator =
          new TsFileSequenceReaderTimeseriesMetadataIterator(reader, true, 2);
      while (iterator.hasNext()) {
        Map<IDeviceID, List<TimeseriesMetadata>> map = iterator.next();
        for (Map.Entry<IDeviceID, List<TimeseriesMetadata>> entry : map.entrySet()) {
          if (iDeviceID != null) {
            if (!iDeviceID.equals(entry.getKey())) {
              if (set.contains(iDeviceID)) {
                Assert.fail("time series metadata iterator needs to be ordered by device id");
              }
              set.add(iDeviceID);
            }
          }
          iDeviceID = entry.getKey();
        }
      }
    } finally {
      if (file.exists()) {
        file.delete();
      }
    }
  }

  public static File createTsFile(String outputPath) throws Exception {
    File file = new File(outputPath);
    if (file.exists()) {
      file.delete();
    }

    try (TsFileWriter tsFileWriter = new TsFileWriter(file)) {
      createSchema(tsFileWriter);
      for (int deviceIndex = 0; deviceIndex < DEVICE_COUNT; deviceIndex++) {
        String deviceId = "root.d." + "device" + deviceIndex;
        Tablet tablet = createTablet(deviceId);

        tsFileWriter.writeAligned(tablet);
        tsFileWriter.flush();
      }
    }

    return file;
  }

  private static void createSchema(TsFileWriter tsFileWriter) throws Exception {
    List<IMeasurementSchema> schemaList = new ArrayList<>();
    for (int colIndex = 0; colIndex < COLUMN_COUNT; colIndex++) {
      String measurementName = "s" + colIndex;
      TSDataType dataType = getDataType(colIndex);
      schemaList.add(new MeasurementSchema(measurementName, dataType));
    }
    for (int deviceIndex = 0; deviceIndex < DEVICE_COUNT; deviceIndex++) {
      tsFileWriter.registerAlignedTimeseries("root.d." + "device" + deviceIndex, schemaList);
    }
  }

  private static TSDataType getDataType(int colIndex) {
    TSDataType[] types = {
      TSDataType.INT32,
      TSDataType.INT64,
      TSDataType.FLOAT,
      TSDataType.DOUBLE,
      TSDataType.BOOLEAN,
      TSDataType.TEXT
    };
    return types[colIndex % types.length];
  }

  private static Tablet createTablet(String deviceId) {
    List<IMeasurementSchema> schemaList = new ArrayList<>();

    for (int colIndex = 0; colIndex < COLUMN_COUNT; colIndex++) {
      String measurementName = "s" + colIndex;
      TSDataType dataType = getDataType(colIndex);

      schemaList.add(new MeasurementSchema(measurementName, dataType));
    }

    Tablet tablet = new Tablet(deviceId, schemaList, 1);

    tablet.initBitMaps();

    tablet.addTimestamp(0, FIXED_TIMESTAMP);

    for (int colIndex = 0; colIndex < COLUMN_COUNT; colIndex++) {
      String measurementName = "s" + colIndex;
      TSDataType dataType = getDataType(colIndex);

      Object value = generateValue(dataType, colIndex);
      tablet.addValue(measurementName, 0, value);
    }

    tablet.setRowSize(1);

    return tablet;
  }

  private static Object generateValue(TSDataType dataType, int colIndex) {
    switch (dataType) {
      case INT32:
        return colIndex % 1000;
      case INT64:
        return (long) (colIndex * 1000);
      case FLOAT:
        return (float) (colIndex * 0.1);
      case DOUBLE:
        return (double) (colIndex * 0.01);
      case BOOLEAN:
        return colIndex % 2 == 0;
      case TEXT:
        return new org.apache.tsfile.utils.Binary(
            ("value_" + colIndex).getBytes(StandardCharsets.UTF_8));
      default:
        return colIndex;
    }
  }
}
