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

import org.apache.iotdb.db.engine.modification.Deletion;
import org.apache.iotdb.db.engine.modification.Modification;
import org.apache.iotdb.db.engine.modification.ModificationFile;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;
import org.apache.iotdb.tsfile.encoding.decoder.Decoder;
import org.apache.iotdb.tsfile.file.MetaMarker;
import org.apache.iotdb.tsfile.file.header.ChunkGroupHeader;
import org.apache.iotdb.tsfile.file.header.ChunkHeader;
import org.apache.iotdb.tsfile.file.header.PageHeader;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.Chunk;
import org.apache.iotdb.tsfile.read.common.IBatchDataIterator;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.reader.chunk.ChunkReader;
import org.apache.iotdb.tsfile.read.reader.page.PageReader;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import static org.apache.iotdb.db.utils.QueryUtils.modifyChunkMetaData;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class CompactionCheckerUtils {

  public static void putOnePageChunks(
      Map<String, List<List<Long>>> chunkPagePointsNum, String fullPath, long[] pagePointNums) {
    for (long pagePointNum : pagePointNums) {
      putChunk(chunkPagePointsNum, fullPath, new long[] {pagePointNum});
    }
  }

  public static void putOnePageChunk(
      Map<String, List<List<Long>>> chunkPagePointsNum, String fullPath, long pagePointNum) {
    putChunk(chunkPagePointsNum, fullPath, new long[] {pagePointNum});
  }

  public static void putChunk(
      Map<String, List<List<Long>>> chunkPagePointsNum, String fullPath, long[] pagePointNums) {
    List<Long> pagePointsNum = new ArrayList<>();
    for (long pagePointNum : pagePointNums) {
      pagePointsNum.add(pagePointNum);
    }
    List<List<Long>> chunkPointsNum =
        chunkPagePointsNum.computeIfAbsent(fullPath, k -> new ArrayList<>());
    chunkPointsNum.add(pagePointsNum);
  }

  /**
   * Use TsFileSequenceReader to read the file list from back to front
   *
   * @param tsFileResources The tsFileResources to be read
   * @return All time series and their corresponding data points
   */
  public static Map<String, List<TimeValuePair>> readFiles(List<TsFileResource> tsFileResources)
      throws IOException, IllegalPathException {
    Map<String, Map<Long, TimeValuePair>> mapResult = new HashMap<>();
    for (TsFileResource tsFileResource : tsFileResources) {
      try (TsFileSequenceReader reader = new TsFileSequenceReader(tsFileResource.getTsFilePath())) {
        List<Path> allPaths = reader.getAllPaths();
        for (Path path : allPaths) {
          Map<Long, TimeValuePair> timeValuePairMap =
              mapResult.computeIfAbsent(path.getFullPath(), k -> new TreeMap<>());
          List<ChunkMetadata> chunkMetadataList = reader.getChunkMetadataList(path);
          List<Modification> seriesModifications = new LinkedList<>();

          for (Modification modification : tsFileResource.getModFile().getModifications()) {
            if (modification.getPath().matchFullPath(new PartialPath(path.getFullPath()))) {
              seriesModifications.add(modification);
            }
          }
          modifyChunkMetaData(chunkMetadataList, seriesModifications);
          for (ChunkMetadata chunkMetadata : chunkMetadataList) {
            Chunk chunk = reader.readMemChunk(chunkMetadata);
            ChunkReader chunkReader = new ChunkReader(chunk, null);
            while (chunkReader.hasNextSatisfiedPage()) {
              BatchData batchData = chunkReader.nextPageData();
              IBatchDataIterator batchDataIterator = batchData.getBatchDataIterator();
              while (batchDataIterator.hasNext()) {
                timeValuePairMap.put(
                    batchDataIterator.currentTime(),
                    new TimeValuePair(
                        batchDataIterator.currentTime(),
                        TsPrimitiveType.getByType(
                            chunkMetadata.getDataType(), batchDataIterator.currentValue())));
                batchDataIterator.next();
              }
            }
          }
        }
      }
    }

    Map<String, List<TimeValuePair>> result = new HashMap<>();
    for (Entry<String, Map<Long, TimeValuePair>> mapResultEntry : mapResult.entrySet()) {
      String fullPath = mapResultEntry.getKey();
      Map<Long, TimeValuePair> timeValuePairMap = mapResultEntry.getValue();
      List<TimeValuePair> timeValuePairs = new ArrayList<>(timeValuePairMap.values());
      result.put(fullPath, timeValuePairs);
    }
    return result;
  }

  /**
   * 1. Use TsFileSequenceReader to read the merged file from back to front, and compare it with the
   * data before the merge to ensure complete consistency. 2. Use TsFileSequenceReader to read the
   * merged file from the front to the back, and compare it with the data before the merge to ensure
   * complete consistency (including pointsInPage statistics in pageHeader). 3. Check whether the
   * statistical information of TsFileResource corresponds to the original data point one-to-one,
   * including startTime, endTime (device level)
   *
   * @param sourceData All time series before merging and their corresponding data points, fullPath
   *     -> pointList
   * @param mergedFiles The merged Files to be checked
   */
  public static void checkDataAndResource(
      Map<String, List<TimeValuePair>> sourceData, List<TsFileResource> mergedFiles)
      throws IOException, IllegalPathException {
    // read from back to front
    Map<String, List<TimeValuePair>> mergedData = readFiles(mergedFiles);
    compareSensorAndData(sourceData, mergedData);

    // check from front to back
    checkDataFromFrontToEnd(sourceData, mergedFiles);

    // check TsFileResource
    checkTsFileResource(sourceData, mergedFiles);
  }

  private static void checkDataFromFrontToEnd(
      Map<String, List<TimeValuePair>> sourceData, List<TsFileResource> mergedFiles)
      throws IOException, IllegalPathException {
    Map<String, List<TimeValuePair>> mergedData = new HashMap<>();
    Map<String, Long> fullPathPointNum = new HashMap<>();
    for (TsFileResource mergedFile : mergedFiles) {
      try (TsFileSequenceReader reader = new TsFileSequenceReader(mergedFile.getTsFilePath())) {
        // Sequential reading of one ChunkGroup now follows this order:
        // first the CHUNK_GROUP_HEADER, then SeriesChunks (headers and data) in one ChunkGroup
        // Because we do not know how many chunks a ChunkGroup may have, we should read one byte
        // (the
        // marker) ahead and judge accordingly.
        String device = "";
        String measurementID = "";
        List<TimeValuePair> currTimeValuePairs = new ArrayList<>();
        reader.position((long) TSFileConfig.MAGIC_STRING.getBytes().length + 1);
        byte marker;
        while ((marker = reader.readMarker()) != MetaMarker.SEPARATOR) {
          switch (marker) {
            case MetaMarker.CHUNK_HEADER:
            case (byte) (MetaMarker.CHUNK_HEADER | TsFileConstant.TIME_COLUMN_MASK):
            case (byte) (MetaMarker.CHUNK_HEADER | TsFileConstant.VALUE_COLUMN_MASK):
            case MetaMarker.ONLY_ONE_PAGE_CHUNK_HEADER:
            case (byte) (MetaMarker.ONLY_ONE_PAGE_CHUNK_HEADER | TsFileConstant.TIME_COLUMN_MASK):
            case (byte) (MetaMarker.ONLY_ONE_PAGE_CHUNK_HEADER | TsFileConstant.VALUE_COLUMN_MASK):
              ChunkHeader header = reader.readChunkHeader(marker);
              // read the next measurement and pack data of last measurement
              if (currTimeValuePairs.size() > 0) {
                PartialPath path = new PartialPath(device, measurementID);
                List<TimeValuePair> timeValuePairs =
                    mergedData.computeIfAbsent(path.getFullPath(), k -> new ArrayList<>());
                timeValuePairs.addAll(currTimeValuePairs);
                currTimeValuePairs.clear();
              }
              measurementID = header.getMeasurementID();
              Decoder defaultTimeDecoder =
                  Decoder.getDecoderByType(
                      TSEncoding.valueOf(
                          TSFileDescriptor.getInstance().getConfig().getTimeEncoder()),
                      TSDataType.INT64);
              Decoder valueDecoder =
                  Decoder.getDecoderByType(header.getEncodingType(), header.getDataType());
              int dataSize = header.getDataSize();
              while (dataSize > 0) {
                valueDecoder.reset();
                PageHeader pageHeader =
                    reader.readPageHeader(
                        header.getDataType(), header.getChunkType() == MetaMarker.CHUNK_HEADER);
                PartialPath path = new PartialPath(device, measurementID);
                ByteBuffer pageData = reader.readPage(pageHeader, header.getCompressionType());
                PageReader reader1 =
                    new PageReader(
                        pageData, header.getDataType(), valueDecoder, defaultTimeDecoder, null);
                BatchData batchData = reader1.getAllSatisfiedPageData();
                long count = fullPathPointNum.getOrDefault(path.getFullPath(), 0L);
                if (header.getChunkType() == MetaMarker.CHUNK_HEADER) {
                  count += pageHeader.getNumOfValues();
                } else {
                  count += batchData.length();
                }
                fullPathPointNum.put(path.getFullPath(), count);
                IBatchDataIterator batchDataIterator = batchData.getBatchDataIterator();
                while (batchDataIterator.hasNext()) {
                  currTimeValuePairs.add(
                      new TimeValuePair(
                          batchDataIterator.currentTime(),
                          TsPrimitiveType.getByType(
                              header.getDataType(), batchDataIterator.currentValue())));
                  batchDataIterator.next();
                }
                dataSize -= pageHeader.getSerializedPageSize();
              }
              break;
            case MetaMarker.CHUNK_GROUP_HEADER:
              ChunkGroupHeader chunkGroupHeader = reader.readChunkGroupHeader();
              // read the next measurement and pack data of last device
              if (currTimeValuePairs.size() > 0) {
                PartialPath path = new PartialPath(device, measurementID);
                List<TimeValuePair> timeValuePairs =
                    mergedData.computeIfAbsent(path.getFullPath(), k -> new ArrayList<>());
                timeValuePairs.addAll(currTimeValuePairs);
                currTimeValuePairs.clear();
              }
              device = chunkGroupHeader.getDeviceID();
              break;
            case MetaMarker.OPERATION_INDEX_RANGE:
              reader.readPlanIndex();
              break;
            default:
              MetaMarker.handleUnexpectedMarker(marker);
          }
        }
        // pack data of last measurement
        if (currTimeValuePairs.size() > 0) {
          PartialPath path = new PartialPath(device, measurementID);
          List<TimeValuePair> timeValuePairs =
              mergedData.computeIfAbsent(path.getFullPath(), k -> new ArrayList<>());
          timeValuePairs.addAll(currTimeValuePairs);
        }
      }

      Collection<Modification> modifications =
          ModificationFile.getNormalMods(mergedFile).getModifications();
      for (Modification modification : modifications) {
        Deletion deletion = (Deletion) modification;
        if (mergedData.containsKey(deletion.getPath().getFullPath())) {
          long deletedCount = 0L;
          Iterator<TimeValuePair> timeValuePairIterator =
              mergedData.get(deletion.getPath().getFullPath()).iterator();
          while (timeValuePairIterator.hasNext()) {
            TimeValuePair timeValuePair = timeValuePairIterator.next();
            if (timeValuePair.getTimestamp() >= deletion.getStartTime()
                && timeValuePair.getTimestamp() <= deletion.getEndTime()) {
              timeValuePairIterator.remove();
              deletedCount++;
            }
          }
          long count = fullPathPointNum.get(deletion.getPath().getFullPath());
          count = count - deletedCount;
          fullPathPointNum.put(deletion.getPath().getFullPath(), count);
        }
      }
    }
    compareSensorAndData(sourceData, mergedData);
    for (Entry<String, List<TimeValuePair>> sourceDataEntry : sourceData.entrySet()) {
      String fullPath = sourceDataEntry.getKey();
      long sourceDataNum = sourceDataEntry.getValue().size();
      long targetDataNum = fullPathPointNum.getOrDefault(fullPath, 0L);
      assertEquals(sourceDataNum, targetDataNum);
    }
  }

  private static void checkTsFileResource(
      Map<String, List<TimeValuePair>> sourceData, List<TsFileResource> mergedFiles)
      throws IllegalPathException {
    Map<String, long[]> devicePointNumMap = new HashMap<>();
    for (Entry<String, List<TimeValuePair>> dataEntry : sourceData.entrySet()) {
      PartialPath partialPath = new PartialPath(dataEntry.getKey());
      String device = partialPath.getDevice();
      long[] statistics =
          devicePointNumMap.computeIfAbsent(
              device, k -> new long[] {Long.MAX_VALUE, Long.MIN_VALUE});
      List<TimeValuePair> timeValuePairs = dataEntry.getValue();
      statistics[0] = Math.min(statistics[0], timeValuePairs.get(0).getTimestamp()); // start time
      statistics[1] =
          Math.max(
              statistics[1],
              timeValuePairs.get(timeValuePairs.size() - 1).getTimestamp()); // end time
    }
    for (Entry<String, long[]> deviceCountEntry : devicePointNumMap.entrySet()) {
      String device = deviceCountEntry.getKey();
      long[] statistics = deviceCountEntry.getValue();
      long startTime = Long.MAX_VALUE;
      for (TsFileResource mergedFile : mergedFiles) {
        startTime = Math.min(startTime, mergedFile.getStartTime(device));
      }
      long endTime = Long.MIN_VALUE;
      for (TsFileResource mergedFile : mergedFiles) {
        endTime = Math.max(endTime, mergedFile.getEndTime(device));
      }
      assertEquals(statistics[0], startTime);
      assertEquals(statistics[1], endTime);
    }
  }

  private static void compareSensorAndData(
      Map<String, List<TimeValuePair>> expectedData, Map<String, List<TimeValuePair>> targetData) {
    for (Entry<String, List<TimeValuePair>> sourceDataEntry : expectedData.entrySet()) {
      String fullPath = sourceDataEntry.getKey();
      List<TimeValuePair> sourceTimeValuePairs = sourceDataEntry.getValue();
      List<TimeValuePair> targetTimeValuePairs = targetData.get(fullPath);
      compareData(sourceTimeValuePairs, targetTimeValuePairs);
    }
  }

  private static void compareData(
      List<TimeValuePair> expectedData, List<TimeValuePair> targetData) {
    if (targetData.size() > expectedData.size()) {
      fail();
    }
    if (targetData.size() < expectedData.size()) {
      fail();
    }
    for (int i = 0; i < expectedData.size(); i++) {
      TimeValuePair expectedTimeValuePair = expectedData.get(i);
      TimeValuePair targetTimeValuePair = targetData.get(i);
      assertEquals(expectedTimeValuePair.getTimestamp(), targetTimeValuePair.getTimestamp());
      assertEquals(expectedTimeValuePair.getValue(), targetTimeValuePair.getValue());
    }
  }

  /**
   * Use TsFileSequenceReader to read the merged file from the front to the back, and compare it
   * with the data before the merge one by one to ensure that the order and size of chunk and page
   * are one-to-one correspondence
   *
   * @param chunkPagePointsNum Target chunk and page size, fullPath ->
   *     ChunkList(PageList(pointNumInEachPage))
   * @param mergedFile The merged File to be checked
   */
  public static void checkChunkAndPage(
      Map<String, List<List<Long>>> chunkPagePointsNum, TsFileResource mergedFile)
      throws IOException, IllegalPathException {
    Map<String, List<List<Long>>> mergedChunkPagePointsNum = new HashMap<>();
    List<Long> pagePointsNum = new ArrayList<>();
    try (TsFileSequenceReader reader = new TsFileSequenceReader(mergedFile.getTsFilePath())) {
      String entity = "";
      String measurement = "";
      // Sequential reading of one ChunkGroup now follows this order:
      // first the CHUNK_GROUP_HEADER, then SeriesChunks (headers and data) in one ChunkGroup
      // Because we do not know how many chunks a ChunkGroup may have, we should read one byte (the
      // marker) ahead and judge accordingly.
      reader.position((long) TSFileConfig.MAGIC_STRING.getBytes().length + 1);
      byte marker;
      while ((marker = reader.readMarker()) != MetaMarker.SEPARATOR) {
        switch (marker) {
          case MetaMarker.CHUNK_HEADER:
          case (byte) (MetaMarker.CHUNK_HEADER | TsFileConstant.TIME_COLUMN_MASK):
          case (byte) (MetaMarker.CHUNK_HEADER | TsFileConstant.VALUE_COLUMN_MASK):
          case MetaMarker.ONLY_ONE_PAGE_CHUNK_HEADER:
          case (byte) (MetaMarker.ONLY_ONE_PAGE_CHUNK_HEADER | TsFileConstant.TIME_COLUMN_MASK):
          case (byte) (MetaMarker.ONLY_ONE_PAGE_CHUNK_HEADER | TsFileConstant.VALUE_COLUMN_MASK):
            ChunkHeader header = reader.readChunkHeader(marker);
            // read the next measurement and pack data of last measurement
            if (pagePointsNum.size() > 0) {
              String fullPath = new PartialPath(entity, measurement).getFullPath();
              List<List<Long>> currChunkPagePointsNum =
                  mergedChunkPagePointsNum.computeIfAbsent(fullPath, k -> new ArrayList<>());
              currChunkPagePointsNum.add(pagePointsNum);
              pagePointsNum = new ArrayList<>();
            }
            measurement = header.getMeasurementID();
            Decoder defaultTimeDecoder =
                Decoder.getDecoderByType(
                    TSEncoding.valueOf(TSFileDescriptor.getInstance().getConfig().getTimeEncoder()),
                    TSDataType.INT64);
            Decoder valueDecoder =
                Decoder.getDecoderByType(header.getEncodingType(), header.getDataType());
            int dataSize = header.getDataSize();
            while (dataSize > 0) {
              valueDecoder.reset();
              PageHeader pageHeader =
                  reader.readPageHeader(
                      header.getDataType(), header.getChunkType() == MetaMarker.CHUNK_HEADER);
              ByteBuffer pageData = reader.readPage(pageHeader, header.getCompressionType());
              PageReader reader1 =
                  new PageReader(
                      pageData, header.getDataType(), valueDecoder, defaultTimeDecoder, null);
              BatchData batchData = reader1.getAllSatisfiedPageData();
              if (header.getChunkType() == MetaMarker.CHUNK_HEADER) {
                pagePointsNum.add(pageHeader.getNumOfValues());
              } else {
                pagePointsNum.add((long) batchData.length());
              }
              IBatchDataIterator batchDataIterator = batchData.getBatchDataIterator();
              while (batchDataIterator.hasNext()) {
                batchDataIterator.next();
              }
              dataSize -= pageHeader.getSerializedPageSize();
            }
            break;
          case MetaMarker.CHUNK_GROUP_HEADER:
            ChunkGroupHeader chunkGroupHeader = reader.readChunkGroupHeader();
            // read the next measurement and pack data of last device
            if (pagePointsNum.size() > 0) {
              String fullPath = new PartialPath(entity, measurement).getFullPath();
              List<List<Long>> currChunkPagePointsNum =
                  mergedChunkPagePointsNum.computeIfAbsent(fullPath, k -> new ArrayList<>());
              currChunkPagePointsNum.add(pagePointsNum);
              pagePointsNum = new ArrayList<>();
            }
            entity = chunkGroupHeader.getDeviceID();
            break;
          case MetaMarker.OPERATION_INDEX_RANGE:
            reader.readPlanIndex();
            break;
          default:
            MetaMarker.handleUnexpectedMarker(marker);
        }
      }
      // pack data of last measurement
      if (pagePointsNum.size() > 0) {
        String fullPath = new PartialPath(entity, measurement).getFullPath();
        List<List<Long>> currChunkPagePointsNum =
            mergedChunkPagePointsNum.computeIfAbsent(fullPath, k -> new ArrayList<>());
        currChunkPagePointsNum.add(pagePointsNum);
      }

      for (Entry<String, List<List<Long>>> chunkPagePointsNumEntry :
          chunkPagePointsNum.entrySet()) {
        String fullPath = chunkPagePointsNumEntry.getKey();
        List<List<Long>> sourceChunkPages = chunkPagePointsNumEntry.getValue();
        List<List<Long>> mergedChunkPages = mergedChunkPagePointsNum.get(fullPath);
        for (int i = 0; i < sourceChunkPages.size(); i++) {
          for (int j = 0; j < sourceChunkPages.get(0).size(); j++) {
            assertEquals(sourceChunkPages.get(i).get(j), mergedChunkPages.get(i).get(j));
          }
        }
      }
    }
  }
}
