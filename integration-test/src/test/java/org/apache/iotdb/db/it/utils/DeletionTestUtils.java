/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.it.utils;

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.env.cluster.node.DataNodeWrapper;

import org.apache.tsfile.file.metadata.ChunkMetadata;
import org.apache.tsfile.file.metadata.IChunkMetadata;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.TimeseriesMetadata;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.read.common.BatchData;
import org.apache.tsfile.read.reader.chunk.ChunkReader;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.tsfile.common.constant.TsFileConstant.TSFILE_SUFFIX;
import static org.junit.Assert.fail;

/** Physical preconditions for deletion ITs; SQL assertions still verify the visible data. */
public final class DeletionTestUtils {
  private DeletionTestUtils() {}

  public static List<Path> getClosedTsFiles(String database, boolean sequence) throws IOException {
    List<Path> result = new ArrayList<>();
    for (DataNodeWrapper node : EnvFactory.getEnv().getDataNodeWrapperList()) {
      Path directory =
          Paths.get(
              node.getDataPath(),
              sequence ? IoTDBConstant.SEQUENCE_FOLDER_NAME : IoTDBConstant.UNSEQUENCE_FOLDER_NAME,
              database);
      if (Files.exists(directory)) {
        try (Stream<Path> files = Files.walk(directory)) {
          result.addAll(
              files
                  .filter(Files::isRegularFile)
                  .filter(file -> file.toString().endsWith(TSFILE_SUFFIX))
                  .filter(file -> Files.isRegularFile(Paths.get(file + ".resource")))
                  .sorted()
                  .collect(Collectors.toList()));
        }
      }
    }
    return result;
  }

  public static Path assertSharedTsFile(String database, IDeviceID... devices) throws IOException {
    for (Path file : getClosedTsFiles(database, true)) {
      try (TsFileSequenceReader reader = new TsFileSequenceReader(file.toString())) {
        if (reader.getAllDevices().containsAll(Arrays.asList(devices))) {
          return file;
        }
      }
    }
    fail("No closed TsFile contains all expected devices: " + Arrays.toString(devices));
    return null;
  }

  // Read the physical file without applying mods or merging it with newer files.
  public static Map<Long, Long> readTreePoints(Path file, String device, String measurement)
      throws IOException {
    Map<Long, Long> points = new TreeMap<>();
    try (TsFileSequenceReader reader = new TsFileSequenceReader(file.toString())) {
      List<TimeseriesMetadata> series =
          reader
              .getAllTimeseriesMetadata(true)
              .get(IDeviceID.Factory.DEFAULT_FACTORY.create(device));
      if (series != null) {
        for (TimeseriesMetadata metadata : series) {
          if (!measurement.equals(metadata.getMeasurementId())) {
            continue;
          }
          for (IChunkMetadata chunk : metadata.getChunkMetadataList()) {
            try (ChunkReader chunkReader =
                new ChunkReader(reader.readMemChunk((ChunkMetadata) chunk))) {
              while (chunkReader.hasNextSatisfiedPage()) {
                BatchData page = chunkReader.nextPageData();
                while (page.hasCurrent()) {
                  points.put(page.currentTime(), page.getLong());
                  page.next();
                }
              }
            }
          }
        }
      }
    }
    return points;
  }
}
