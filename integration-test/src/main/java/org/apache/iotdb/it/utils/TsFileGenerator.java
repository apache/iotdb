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
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.engine.modification.Deletion;
import org.apache.iotdb.db.engine.modification.ModificationFile;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.write.TsFileWriter;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeSet;

public class TsFileGenerator implements AutoCloseable {
  private static final Logger logger = LoggerFactory.getLogger(TsFileGenerator.class);

  private final File tsFile;
  private final TsFileWriter writer;
  private final Map<String, TreeSet<Long>> device2TimeSet;
  private final Map<String, List<MeasurementSchema>> device2MeasurementSchema;
  private Random random;

  public TsFileGenerator(File tsFile) throws IOException {
    this.tsFile = tsFile;
    this.writer = new TsFileWriter(tsFile);
    this.device2TimeSet = new HashMap<>();
    this.device2MeasurementSchema = new HashMap<>();
    this.random = new Random();
  }

  public void resetRandom() {
    random = new Random();
  }

  public void resetRandom(long seed) {
    random = new Random(seed);
  }

  public void registerTimeseries(String path, List<MeasurementSchema> measurementSchemaList) {
    if (device2MeasurementSchema.containsKey(path)) {
      logger.error(String.format("Register same device %s.", path));
      return;
    }
    writer.registerTimeseries(new Path(path), measurementSchemaList);
    device2TimeSet.put(path, new TreeSet<>());
    device2MeasurementSchema.put(path, measurementSchemaList);
  }

  public void registerAlignedTimeseries(String path, List<MeasurementSchema> measurementSchemaList)
      throws WriteProcessException {
    if (device2MeasurementSchema.containsKey(path)) {
      logger.error(String.format("Register same device %s.", path));
      return;
    }
    writer.registerAlignedTimeseries(new Path(path), measurementSchemaList);
    device2TimeSet.put(path, new TreeSet<>());
    device2MeasurementSchema.put(path, measurementSchemaList);
  }

  public void generateData(String device, int number, long timeGap, boolean isAligned)
      throws IOException, WriteProcessException {
    List<MeasurementSchema> schemas = device2MeasurementSchema.get(device);
    TreeSet<Long> timeSet = device2TimeSet.get(device);
    Tablet tablet = new Tablet(device, schemas);
    long[] timestamps = tablet.timestamps;
    Object[] values = tablet.values;
    long sensorNum = schemas.size();
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

    logger.info(String.format("Write %d points into device %s", number, device));
  }

  private void generateDataPoint(Object obj, int row, MeasurementSchema schema) {
    switch (schema.getType()) {
      case INT32:
        generateINT32(obj, row);
        break;
      case INT64:
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
        generateTEXT(obj, row);
        break;
      default:
        logger.error(String.format("Wrong data type %s.", schema.getType()));
    }
  }

  private void generateINT32(Object obj, int row) {
    int[] ints = (int[]) obj;
    ints[row] = random.nextInt();
  }

  private void generateINT64(Object obj, int row) {
    long[] longs = (long[]) obj;
    longs[row] = random.nextLong();
  }

  private void generateFLOAT(Object obj, int row) {
    float[] floats = (float[]) obj;
    floats[row] = random.nextFloat();
  }

  private void generateDOUBLE(Object obj, int row) {
    double[] doubles = (double[]) obj;
    doubles[row] = random.nextDouble();
  }

  private void generateBOOLEAN(Object obj, int row) {
    boolean[] booleans = (boolean[]) obj;
    booleans[row] = random.nextBoolean();
  }

  private void generateTEXT(Object obj, int row) {
    Binary[] binaries = (Binary[]) obj;
    binaries[row] = new Binary(String.format("test point %d", random.nextInt()));
  }

  public void generateDeletion(String device, int number) throws IOException, IllegalPathException {
    try (ModificationFile modificationFile =
        new ModificationFile(tsFile.getAbsolutePath() + ModificationFile.FILE_SUFFIX)) {
      writer.flushAllChunkGroups();
      TreeSet<Long> timeSet = device2TimeSet.get(device);
      if (timeSet.isEmpty()) {
        return;
      }

      long fileOffset = tsFile.length();
      long maxTime = timeSet.last() - 1;
      for (int i = 0; i < number; i++) {
        int endTime = random.nextInt((int) (maxTime)) + 1;
        int startTime = random.nextInt(endTime);
        for (MeasurementSchema measurementSchema : device2MeasurementSchema.get(device)) {
          Deletion deletion =
              new Deletion(
                  new PartialPath(
                      device
                          + TsFileConstant.PATH_SEPARATOR
                          + measurementSchema.getMeasurementId()),
                  fileOffset,
                  startTime,
                  endTime);
          modificationFile.write(deletion);
        }
        for (long j = startTime; j <= endTime; j++) {
          timeSet.remove(j);
        }
        logger.info(
            String.format("Delete %d - %d timestamp of device %s", startTime, endTime, device));
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
