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

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.db.storageengine.dataregion.modification.ModEntry;
import org.apache.iotdb.db.storageengine.dataregion.modification.ModificationFile;
import org.apache.iotdb.db.storageengine.dataregion.modification.TreeDeletionEntry;

import org.apache.tsfile.exception.write.WriteProcessException;
import org.apache.tsfile.read.common.Path;
import org.apache.tsfile.write.TsFileWriter;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
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
    final long sensorNum = schemas.size();
    long startTime = timeSet.isEmpty() ? 0L : timeSet.last();

    for (long r = 0; r < number; r++) {
      int row = tablet.getRowSize();
      startTime += timeGap;
      tablet.addTimestamp(row, startTime);
      timeSet.add(startTime);
      for (int i = 0; i < sensorNum; i++) {
        generateDataPoint(tablet, i, row, schemas.get(i));
      }
      // write
      if (tablet.getRowSize() == tablet.getMaxRowNumber()) {
        writer.writeTree(tablet);
        tablet.reset();
      }
    }
    // write
    if (tablet.getRowSize() != 0) {
      writer.writeTree(tablet);
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
    final long sensorNum = schemas.size();
    long startTime = startTimestamp;

    for (long r = 0; r < number; r++) {
      final int row = tablet.getRowSize();
      startTime += timeGap;
      tablet.addTimestamp(row, startTime);
      timeSet.add(startTime);
      for (int i = 0; i < sensorNum; i++) {
        generateDataPoint(tablet, i, row, schemas.get(i));
      }
      // write
      if (tablet.getRowSize() == tablet.getMaxRowNumber()) {
        writer.writeTree(tablet);
        tablet.reset();
      }
    }
    // write
    if (tablet.getRowSize() != 0) {
      if (!isAligned) {
        writer.writeTree(tablet);
      } else {
        writer.writeAligned(tablet);
      }
      tablet.reset();
    }

    LOGGER.info("Write {} points into device {}", number, device);
  }

  private void generateDataPoint(
      final Tablet tablet, final int column, final int row, final IMeasurementSchema schema) {
    switch (schema.getType()) {
      case INT32:
        generateINT32(tablet, column, row);
        break;
      case DATE:
        generateDATE(tablet, column, row);
        break;
      case INT64:
      case TIMESTAMP:
        generateINT64(tablet, column, row);
        break;
      case FLOAT:
        generateFLOAT(tablet, column, row);
        break;
      case DOUBLE:
        generateDOUBLE(tablet, column, row);
        break;
      case BOOLEAN:
        generateBOOLEAN(tablet, column, row);
        break;
      case TEXT:
      case BLOB:
      case STRING:
        generateTEXT(tablet, column, row);
        break;
      default:
        LOGGER.error("Wrong data type {}.", schema.getType());
    }
  }

  private void generateINT32(final Tablet tablet, final int column, final int row) {
    tablet.addValue(row, column, random.nextInt());
  }

  private void generateDATE(final Tablet tablet, final int column, final int row) {
    tablet.addValue(
        row,
        column,
        LocalDate.of(1000 + random.nextInt(9000), 1 + random.nextInt(12), 1 + random.nextInt(28)));
  }

  private void generateINT64(final Tablet tablet, final int column, final int row) {
    tablet.addValue(row, column, random.nextLong());
  }

  private void generateFLOAT(final Tablet tablet, final int column, final int row) {
    tablet.addValue(row, column, random.nextFloat());
  }

  private void generateDOUBLE(final Tablet tablet, final int column, final int row) {
    tablet.addValue(row, column, random.nextDouble());
  }

  private void generateBOOLEAN(final Tablet tablet, final int column, final int row) {
    tablet.addValue(row, column, random.nextBoolean());
  }

  private void generateTEXT(final Tablet tablet, final int column, final int row) {
    tablet.addValue(row, column, String.format("test point %d", random.nextInt()));
  }

  public void generateDeletion(final String device) throws IOException, IllegalPathException {
    try (final ModificationFile modificationFile =
        new ModificationFile(ModificationFile.getExclusiveMods(tsFile), false)) {
      modificationFile.write(
          new TreeDeletionEntry(
              new MeasurementPath(device, IoTDBConstant.ONE_LEVEL_PATH_WILDCARD),
              Long.MIN_VALUE,
              Long.MAX_VALUE));
      device2TimeSet.remove(device);
      device2MeasurementSchema.remove(device);
    }
  }

  public void generateDeletion(final String device, final MeasurementSchema measurement)
      throws IOException, IllegalPathException {
    try (final ModificationFile modificationFile =
        new ModificationFile(ModificationFile.getExclusiveMods(tsFile), false)) {
      modificationFile.write(
          new TreeDeletionEntry(
              new MeasurementPath(device, measurement.getMeasurementName()),
              Long.MIN_VALUE,
              Long.MAX_VALUE));
      device2MeasurementSchema.get(device).remove(measurement);
    }
  }

  public void generateDeletion(final String device, final int number)
      throws IOException, IllegalPathException {
    try (final ModificationFile modificationFile =
        new ModificationFile(ModificationFile.getExclusiveMods(tsFile), false)) {
      writer.flush();
      final TreeSet<Long> timeSet = device2TimeSet.get(device);
      if (timeSet.isEmpty()) {
        return;
      }

      final long maxTime = timeSet.last() - 1;
      for (int i = 0; i < number; i++) {
        final int endTime = random.nextInt((int) (maxTime)) + 1;
        final int startTime = random.nextInt(endTime);
        for (final IMeasurementSchema measurementSchema : device2MeasurementSchema.get(device)) {
          final ModEntry deletion =
              new TreeDeletionEntry(
                  new MeasurementPath(device, measurementSchema.getMeasurementName()),
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
