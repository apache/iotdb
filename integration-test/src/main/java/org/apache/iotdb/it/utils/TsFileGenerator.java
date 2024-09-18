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

package org.apache.iotdb.it.utils;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.db.storageengine.dataregion.modification.Deletion;
import org.apache.iotdb.db.storageengine.dataregion.modification.ModificationFile;

import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.exception.write.WriteProcessException;
import org.apache.tsfile.read.common.Path;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.write.TsFileWriter;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.time.LocalDate;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeSet;

public class TsFileGenerator implements AutoCloseable {
  private static final Logger LOGGER = LoggerFactory.getLogger(TsFileGenerator.class);

  private final File tsFile;
  private final TsFileWriter writer;
  private final Map<String, TreeSet<Long>> device2TimeSet;
  private final Map<String, List<IMeasurementSchema>> device2MeasurementSchema;
  private Random random;

  public TsFileGenerator(final File tsFile) throws IOException {
    this.tsFile = tsFile;
    this.writer = new TsFileWriter(tsFile);
    this.device2TimeSet = new HashMap<>();
    this.device2MeasurementSchema = new HashMap<>();
    this.random = new Random();
  }

  public void resetRandom() {
    random = new Random();
  }

  public void resetRandom(final long seed) {
    random = new Random(seed);
  }

  public void registerTimeseries(
      final String path, final List<IMeasurementSchema> measurementSchemaList) {
    if (device2MeasurementSchema.containsKey(path)) {
      LOGGER.error("Register same device {}.", path);
      return;
    }
    writer.registerTimeseries(new Path(path), measurementSchemaList);
    device2TimeSet.put(path, new TreeSet<>());
    device2MeasurementSchema.put(path, measurementSchemaList);
  }

  public void registerAlignedTimeseries(
      final String path, final List<IMeasurementSchema> measurementSchemaList)
      throws WriteProcessException {
    if (device2MeasurementSchema.containsKey(path)) {
      LOGGER.error("Register same device {}.", path);
      return;
    }
    writer.registerAlignedTimeseries(new Path(path), measurementSchemaList);
    device2TimeSet.put(path, new TreeSet<>());
    device2MeasurementSchema.put(path, measurementSchemaList);
  }

  public void generateData(
      final String device, final int number, final long timeGap, final boolean isAligned)
      throws IOException, WriteProcessException {
    final List<IMeasurementSchema> schemas = device2MeasurementSchema.get(device);
    final TreeSet<Long> timeSet = device2TimeSet.get(device);
    final Tablet tablet = new Tablet(device, schemas);
    final long[] timestamps = tablet.timestamps;
    final Object[] values = tablet.values;
    final long sensorNum = schemas.size();
    long startTime = timeSet.isEmpty() ? 0L : timeSet.last();

    for (long r = 0; r < number; r++) {
      int row = tablet.rowSize++;
      startTime += timeGap;
      timestamps[row] = startTime;
      timeSet.add(startTime);
      for (int i = 0; i < sensorNum; i++) {
        generateDataPoint(values[i], row, schemas.get(i));
      }
      // write
      if (tablet.rowSize == tablet.getMaxRowNumber()) {
        if (!isAligned) {
          writer.write(tablet);
        } else {
          writer.writeAligned(tablet);
        }
        tablet.reset();
      }
    }
    // write
    if (tablet.rowSize != 0) {
      if (!isAligned) {
        writer.write(tablet);
      } else {
        writer.writeAligned(tablet);
      }
      tablet.reset();
    }

    LOGGER.info("Write {} points into device {}", number, device);
  }

  public void generateData(
      final String device,
      final int number,
      final long timeGap,
      final boolean isAligned,
      final long startTimestamp)
      throws IOException, WriteProcessException {
    final List<IMeasurementSchema> schemas = device2MeasurementSchema.get(device);
    final TreeSet<Long> timeSet = device2TimeSet.get(device);
    final Tablet tablet = new Tablet(device, schemas);
    final long[] timestamps = tablet.timestamps;
    final Object[] values = tablet.values;
    final long sensorNum = schemas.size();
    long startTime = startTimestamp;

    for (long r = 0; r < number; r++) {
      final int row = tablet.rowSize++;
      startTime += timeGap;
      timestamps[row] = startTime;
      timeSet.add(startTime);
      for (int i = 0; i < sensorNum; i++) {
        generateDataPoint(values[i], row, schemas.get(i));
      }
      // write
      if (tablet.rowSize == tablet.getMaxRowNumber()) {
        if (!isAligned) {
          writer.write(tablet);
        } else {
          writer.writeAligned(tablet);
        }
        tablet.reset();
      }
    }
    // write
    if (tablet.rowSize != 0) {
      if (!isAligned) {
        writer.write(tablet);
      } else {
        writer.writeAligned(tablet);
      }
      tablet.reset();
    }

    LOGGER.info("Write {} points into device {}", number, device);
  }

  private void generateDataPoint(final Object obj, final int row, final IMeasurementSchema schema) {
    switch (schema.getType()) {
      case INT32:
        generateINT32(obj, row);
        break;
      case DATE:
        generateDATE(obj, row);
        break;
      case INT64:
      case TIMESTAMP:
        generateINT64(obj, row);
        break;
      case FLOAT:
        generateFLOAT(obj, row);
        break;
      case DOUBLE:
        generateDOUBLE(obj, row);
        break;
      case BOOLEAN:
        generateBOOLEAN(obj, row);
        break;
      case TEXT:
      case BLOB:
      case STRING:
        generateTEXT(obj, row);
        break;
      default:
        LOGGER.error("Wrong data type {}.", schema.getType());
    }
  }

  private void generateINT32(final Object obj, final int row) {
    final int[] ints = (int[]) obj;
    ints[row] = random.nextInt();
  }

  private void generateDATE(final Object obj, final int row) {
    final LocalDate[] dates = (LocalDate[]) obj;
    dates[row] =
        LocalDate.of(1000 + random.nextInt(9000), 1 + random.nextInt(12), 1 + random.nextInt(28));
  }

  private void generateINT64(final Object obj, final int row) {
    final long[] longs = (long[]) obj;
    longs[row] = random.nextLong();
  }

  private void generateFLOAT(final Object obj, final int row) {
    final float[] floats = (float[]) obj;
    floats[row] = random.nextFloat();
  }

  private void generateDOUBLE(final Object obj, final int row) {
    final double[] doubles = (double[]) obj;
    doubles[row] = random.nextDouble();
  }

  private void generateBOOLEAN(final Object obj, final int row) {
    final boolean[] booleans = (boolean[]) obj;
    booleans[row] = random.nextBoolean();
  }

  private void generateTEXT(final Object obj, final int row) {
    final Binary[] binaries = (Binary[]) obj;
    binaries[row] =
        new Binary(String.format("test point %d", random.nextInt()), TSFileConfig.STRING_CHARSET);
  }

  public void generateDeletion(final String device, final int number)
      throws IOException, IllegalPathException {
    try (final ModificationFile modificationFile =
        new ModificationFile(tsFile.getAbsolutePath() + ModificationFile.FILE_SUFFIX)) {
      writer.flushAllChunkGroups();
      final TreeSet<Long> timeSet = device2TimeSet.get(device);
      if (timeSet.isEmpty()) {
        return;
      }

      final long fileOffset = tsFile.length();
      final long maxTime = timeSet.last() - 1;
      for (int i = 0; i < number; i++) {
        final int endTime = random.nextInt((int) (maxTime)) + 1;
        final int startTime = random.nextInt(endTime);
        for (final IMeasurementSchema measurementSchema : device2MeasurementSchema.get(device)) {
          final Deletion deletion =
              new Deletion(
                  new MeasurementPath(device, measurementSchema.getMeasurementId()),
                  fileOffset,
                  startTime,
                  endTime);
          modificationFile.write(deletion);
        }
        for (long j = startTime; j <= endTime; j++) {
          timeSet.remove(j);
        }
        LOGGER.info("Delete {} - {} timestamp of device {}", startTime, endTime, device);
      }
    }
  }

  public long getTotalNumber() {
    return device2TimeSet.entrySet().stream()
        .mapToInt(
            entry -> entry.getValue().size() * device2MeasurementSchema.get(entry.getKey()).size())
        .sum();
  }

  @Override
  public void close() throws Exception {
    writer.close();
  }
}
