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

package org.apache.iotdb.db.engine.compaction.utils;

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.constant.TestConstant;
import org.apache.iotdb.db.engine.modification.Deletion;
import org.apache.iotdb.db.engine.modification.ModificationFile;
import org.apache.iotdb.db.engine.storagegroup.TsFileNameGenerator;
import org.apache.iotdb.db.engine.storagegroup.TsFileNameGenerator.TsFileName;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.write.chunk.ChunkWriterImpl;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.iotdb.tsfile.write.writer.RestorableTsFileIOWriter;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

public class CompactionFileGeneratorUtils {

  public static TsFileResource getTargetTsFileResourceFromSourceResource(
      TsFileResource sourceResource) throws IOException {
    TsFileName tsFileName = TsFileNameGenerator.getTsFileName(sourceResource.getTsFile().getName());
    return new TsFileResource(
        new File(
            TestConstant.BASE_OUTPUT_PATH.concat(
                tsFileName.getTime()
                    + IoTDBConstant.FILE_NAME_SEPARATOR
                    + tsFileName.getVersion()
                    + IoTDBConstant.FILE_NAME_SEPARATOR
                    + (tsFileName.getInnerCompactionCnt() + 1)
                    + IoTDBConstant.FILE_NAME_SEPARATOR
                    + tsFileName.getCrossCompactionCnt()
                    + IoTDBConstant.INNER_COMPACTION_TMP_FILE_SUFFIX)));
  }

  public static List<TsFileResource> getInnerCompactionTargetTsFileResources(
      List<TsFileResource> fileResources, boolean seq) throws IOException {
    List<TsFileResource> resources = new ArrayList<>();
    resources.add(
        new TsFileResource(
            TsFileNameGenerator.getInnerCompactionTargetFileResource(fileResources, seq)));
    return resources;
  }

  public static List<TsFileResource> getCrossCompactionTargetTsFileResources(
      List<TsFileResource> seqFileResources) throws IOException {
    return TsFileNameGenerator.getCrossCompactionTargetFileResources(seqFileResources);
  }

  public static TsFileResource generateTsFileResource(boolean sequence, int index) {
    if (sequence) {
      return new TsFileResource(
          new File(
              TestConstant.BASE_OUTPUT_PATH.concat(
                  index
                      + IoTDBConstant.FILE_NAME_SEPARATOR
                      + index
                      + IoTDBConstant.FILE_NAME_SEPARATOR
                      + 0
                      + IoTDBConstant.FILE_NAME_SEPARATOR
                      + 0
                      + ".tsfile")));
    } else {
      return new TsFileResource(
          new File(
              TestConstant.BASE_OUTPUT_PATH.concat(
                  (index + 10000)
                      + IoTDBConstant.FILE_NAME_SEPARATOR
                      + (index + 10000)
                      + IoTDBConstant.FILE_NAME_SEPARATOR
                      + 0
                      + IoTDBConstant.FILE_NAME_SEPARATOR
                      + 0
                      + ".tsfile")));
    }
  }

  public static TsFileResource generateTsFileResource(
      boolean sequence, int index, String storageGroupName) {
    if (sequence) {
      return new TsFileResource(
          new File(
              TestConstant.BASE_OUTPUT_PATH
                  .concat(File.separator)
                  .concat("sequence")
                  .concat(File.separator)
                  .concat(storageGroupName)
                  .concat(File.separator)
                  .concat("0")
                  .concat(File.separator)
                  .concat("0")
                  .concat(File.separator)
                  .concat(
                      index
                          + IoTDBConstant.FILE_NAME_SEPARATOR
                          + index
                          + IoTDBConstant.FILE_NAME_SEPARATOR
                          + 0
                          + IoTDBConstant.FILE_NAME_SEPARATOR
                          + 0
                          + ".tsfile")));
    } else {
      return new TsFileResource(
          new File(
              TestConstant.BASE_OUTPUT_PATH
                  .concat(File.separator)
                  .concat("unsequence")
                  .concat(File.separator)
                  .concat(storageGroupName)
                  .concat(File.separator)
                  .concat("0")
                  .concat(File.separator)
                  .concat("0")
                  .concat(File.separator)
                  .concat(
                      (index + 10000)
                          + IoTDBConstant.FILE_NAME_SEPARATOR
                          + (index + 10000)
                          + IoTDBConstant.FILE_NAME_SEPARATOR
                          + 0
                          + IoTDBConstant.FILE_NAME_SEPARATOR
                          + 0
                          + ".tsfile")));
    }
  }

  /**
   * Generate a new file. For each time series, insert a point (+1 for each point) into the file
   * from the start time util each sequence of the last file meets the target Chunk and Page size,
   * the value is also equal to the time
   *
   * @param fullPaths Set(fullPath)
   * @param chunkPagePointsNum chunkList->pageList->points
   * @param startTime The startTime to write
   * @param newTsFileResource The tsfile to write
   */
  public static void writeTsFile(
      Set<String> fullPaths,
      List<List<Long>> chunkPagePointsNum,
      long startTime,
      TsFileResource newTsFileResource)
      throws IOException, IllegalPathException {
    // disable auto page seal and seal page manually
    int prevMaxNumberOfPointsInPage =
        TSFileDescriptor.getInstance().getConfig().getMaxNumberOfPointsInPage();
    TSFileDescriptor.getInstance().getConfig().setMaxNumberOfPointsInPage(Integer.MAX_VALUE);

    if (!newTsFileResource.getTsFile().getParentFile().exists()) {
      newTsFileResource.getTsFile().getParentFile().mkdirs();
    }
    RestorableTsFileIOWriter writer = new RestorableTsFileIOWriter(newTsFileResource.getTsFile());
    Map<String, List<String>> deviceMeasurementMap = new HashMap<>();
    for (String fullPath : fullPaths) {
      PartialPath partialPath = new PartialPath(fullPath);
      List<String> sensors =
          deviceMeasurementMap.computeIfAbsent(partialPath.getDevice(), (s) -> new ArrayList<>());
      sensors.add(partialPath.getMeasurement());
    }
    for (Entry<String, List<String>> deviceMeasurementEntry : deviceMeasurementMap.entrySet()) {
      String device = deviceMeasurementEntry.getKey();
      writer.startChunkGroup(device);
      for (String sensor : deviceMeasurementEntry.getValue()) {
        long currTime = startTime;
        for (List<Long> chunk : chunkPagePointsNum) {
          ChunkWriterImpl chunkWriter =
              new ChunkWriterImpl(new MeasurementSchema(sensor, TSDataType.INT64), true);
          for (Long page : chunk) {
            for (long i = 0; i < page; i++) {
              chunkWriter.write(currTime, currTime);
              newTsFileResource.updateStartTime(device, currTime);
              newTsFileResource.updateEndTime(device, currTime);
              currTime++;
            }
            chunkWriter.sealCurrentPage();
          }
          chunkWriter.writeToFileWriter(writer);
        }
      }
      writer.endChunkGroup();
    }
    newTsFileResource.serialize();
    writer.endFile();
    newTsFileResource.close();

    TSFileDescriptor.getInstance()
        .getConfig()
        .setMaxNumberOfPointsInPage(prevMaxNumberOfPointsInPage);
  }

  /**
   * Generate a new file. For each time series, insert a point (time +1 for each point, time =
   * value) into the file from the start time to the end time, the value is also equal to the time
   *
   * @param fullPaths Set(fullPath)
   * @param chunkPagePointsNum chunk->page->points([startTime, endTime),[startTime, endTime),...)
   * @param newTsFileResource The tsfile to write
   */
  public static void writeChunkToTsFileWithTimeRange(
      Set<String> fullPaths,
      List<List<long[][]>> chunkPagePointsNum,
      TsFileResource newTsFileResource)
      throws IOException, IllegalPathException {
    // disable auto page seal and seal page manually
    int prevMaxNumberOfPointsInPage =
        TSFileDescriptor.getInstance().getConfig().getMaxNumberOfPointsInPage();
    TSFileDescriptor.getInstance().getConfig().setMaxNumberOfPointsInPage(Integer.MAX_VALUE);

    RestorableTsFileIOWriter writer = new RestorableTsFileIOWriter(newTsFileResource.getTsFile());
    Map<String, List<String>> deviceMeasurementMap = new HashMap<>();
    for (String fullPath : fullPaths) {
      PartialPath partialPath = new PartialPath(fullPath);
      List<String> sensors =
          deviceMeasurementMap.computeIfAbsent(partialPath.getDevice(), (s) -> new ArrayList<>());
      sensors.add(partialPath.getMeasurement());
    }
    int currChunksIndex = 0;
    for (Entry<String, List<String>> deviceMeasurementEntry : deviceMeasurementMap.entrySet()) {
      String device = deviceMeasurementEntry.getKey();
      writer.startChunkGroup(device);
      for (String sensor : deviceMeasurementEntry.getValue()) {
        List<long[][]> chunks = chunkPagePointsNum.get(currChunksIndex);
        ChunkWriterImpl chunkWriter =
            new ChunkWriterImpl(new MeasurementSchema(sensor, TSDataType.INT64), true);
        for (long[][] pages : chunks) {
          for (long[] starEndTime : pages) {
            for (long i = starEndTime[0]; i < starEndTime[1]; i++) {
              chunkWriter.write(i, i);
              newTsFileResource.updateStartTime(device, i);
              newTsFileResource.updateEndTime(device, i);
            }
          }
          chunkWriter.sealCurrentPage();
        }
        chunkWriter.writeToFileWriter(writer);
        currChunksIndex++;
      }
      writer.endChunkGroup();
    }
    newTsFileResource.serialize();
    writer.endFile();
    newTsFileResource.close();

    TSFileDescriptor.getInstance()
        .getConfig()
        .setMaxNumberOfPointsInPage(prevMaxNumberOfPointsInPage);
  }

  /**
   * Generate mods files according to toDeleteTimeseriesAndTime for corresponding
   * targetTsFileResource
   *
   * @param toDeleteTimeseriesAndTime The timeseries and time to be deleted, Map(fullPath,
   *     (startTime, endTime))
   * @param targetTsFileResource The tsfile to be deleted
   * @param isCompactionMods Generate *.compaction. or generate .compaction.mods
   */
  public static void generateMods(
      Map<String, Pair<Long, Long>> toDeleteTimeseriesAndTime,
      TsFileResource targetTsFileResource,
      boolean isCompactionMods)
      throws IllegalPathException, IOException {
    ModificationFile modificationFile;
    if (isCompactionMods) {
      modificationFile = ModificationFile.getCompactionMods(targetTsFileResource);
    } else {
      modificationFile = ModificationFile.getNormalMods(targetTsFileResource);
    }
    for (Entry<String, Pair<Long, Long>> toDeleteTimeseriesAndTimeEntry :
        toDeleteTimeseriesAndTime.entrySet()) {
      String fullPath = toDeleteTimeseriesAndTimeEntry.getKey();
      Pair<Long, Long> startTimeEndTime = toDeleteTimeseriesAndTimeEntry.getValue();
      Deletion deletion =
          new Deletion(
              new PartialPath(fullPath),
              Long.MAX_VALUE,
              startTimeEndTime.left,
              startTimeEndTime.right);
      modificationFile.write(deletion);
    }
    modificationFile.close();
  }
}
