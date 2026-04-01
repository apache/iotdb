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
package org.apache.iotdb.db.tools.utils;

import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.encoding.decoder.Decoder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.header.ChunkHeader;
import org.apache.tsfile.file.header.PageHeader;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.file.metadata.statistics.Statistics;
import org.apache.tsfile.read.common.BatchData;
import org.apache.tsfile.read.reader.page.AlignedPageReader;
import org.apache.tsfile.read.reader.page.PageReader;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.utils.TsPrimitiveType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class TsFileStatisticScan extends TsFileSequenceScan {

  private static final Logger LOGGER = LoggerFactory.getLogger(TsFileStatisticScan.class);

  // (deviceId, measurementId) -> data type
  private final Map<Pair<IDeviceID, String>, TSDataType> seriesDataTypeMap = new HashMap<>();
  private final Map<TSDataType, Long> dataTypeSizeMap = new EnumMap<>(TSDataType.class);
  private final Map<TSDataType, Long> dataTypePointMap = new EnumMap<>(TSDataType.class);
  private final Map<TSDataType, Long> dataTypeChunkMap = new EnumMap<>(TSDataType.class);
  private final List<Integer> distinctBinaryValueNumInChunks = new LinkedList<>();
  // the number of Int64 chunks that can be represented by Int32
  private int overPrecisedInt64ChunkNum;
  // the number of Int64 chunks that cannot be represented by Int32
  private int justPrecisedInt64ChunkNum;
  // the number of Int64 chunks whose range can be represented by Int32
  private int smallRangeInt64ChunkNum;
  // the number of Int64 chunks whose range cannot be represented by Int32
  private int largeRangeInt64ChunkNum;
  private boolean currChunkJustPrecised;
  private boolean currChunkLargeRange;
  private Set<Binary> distinctBinarySet = new HashSet<>();
  private PageHeader currTimePageHeader;
  private ByteBuffer currTimePageBuffer;

  public static void main(String[] args) {
    TsFileStatisticScan t = new TsFileStatisticScan();
    t.scanTsFile(new File(args[0]));
  }

  @Override
  protected void onFileEnd() throws IOException {
    super.onFileEnd();
    Map<TSDataType, Integer> seriesTypeCountMap = new EnumMap<>(TSDataType.class);
    for (TSDataType type : seriesDataTypeMap.values()) {
      seriesTypeCountMap.compute(type, (t, v) -> v == null ? 1 : v + 1);
    }
    System.out.println("Series data type count: " + seriesTypeCountMap);
    System.out.println(
        "Int64 series statistics: overPrecised "
            + overPrecisedInt64ChunkNum
            + ", justPrecised "
            + justPrecisedInt64ChunkNum
            + ", smallRange "
            + smallRangeInt64ChunkNum
            + ", largeRange "
            + largeRangeInt64ChunkNum);
    System.out.println("data type -> size: " + dataTypeSizeMap);
    System.out.println("data type -> point count: " + dataTypePointMap);
    System.out.println("data type -> chunk count: " + dataTypeChunkMap);
    System.out.println(
        "average distinct binary value num: "
            + distinctBinaryValueNumInChunks.stream().mapToInt(i -> i).average().orElse(0.0));
  }

  @Override
  protected void onChunk(PageVisitor pageVisitor) throws IOException {
    currChunkJustPrecised = false;
    currChunkLargeRange = false;

    super.onChunk(pageVisitor);
    if (!isTimeChunk) {
      seriesDataTypeMap.computeIfAbsent(currTimeseriesID, cid -> currChunkHeader.getDataType());
      dataTypeSizeMap.compute(
          currChunkHeader.getDataType(),
          (type, size) ->
              size == null
                  ? currChunkHeader.getSerializedSize() + currChunkHeader.getDataSize()
                  : size + currChunkHeader.getSerializedSize() + currChunkHeader.getDataSize());
      dataTypeChunkMap.compute(
          currChunkHeader.getDataType(), (type, size) -> size == null ? 1 : size + 1);
    }
    if (currChunkHeader.getDataType() == TSDataType.INT64) {
      if (currChunkJustPrecised) {
        justPrecisedInt64ChunkNum++;
      } else {
        overPrecisedInt64ChunkNum++;
      }

      if (currChunkLargeRange) {
        largeRangeInt64ChunkNum++;
      } else {
        smallRangeInt64ChunkNum++;
      }
    } else if (currChunkHeader.getDataType() == TSDataType.TEXT
        || currChunkHeader.getDataType() == TSDataType.STRING) {
      distinctBinaryValueNumInChunks.add(distinctBinarySet.size());
      distinctBinarySet.clear();
    }
  }

  @Override
  protected void onTimePage(PageHeader pageHeader, ByteBuffer pageData, ChunkHeader chunkHeader)
      throws IOException {
    currTimePageHeader = pageHeader;
    currTimePageBuffer = pageData;
  }

  @Override
  protected void onValuePage(PageHeader pageHeader, ByteBuffer pageData, ChunkHeader chunkHeader)
      throws IOException {
    TSDataType dataType = chunkHeader.getDataType();
    if (dataType == TSDataType.INT64) {
      Statistics<Long> statistics = (Statistics<Long>) pageHeader.getStatistics();
      Long minValue = statistics.getMinValue();
      Long maxValue = statistics.getMaxValue();
      if (minValue < Integer.MIN_VALUE || maxValue > Integer.MAX_VALUE) {
        currChunkJustPrecised = true;
      }
      if (maxValue - minValue > Integer.MAX_VALUE) {
        currChunkLargeRange = true;
      }
    } else if (dataType == TSDataType.TEXT || dataType == TSDataType.STRING) {
      AlignedPageReader pageReader =
          new AlignedPageReader(
              currTimePageHeader,
              currTimePageBuffer,
              Decoder.getDecoderByType(
                  TSEncoding.valueOf(TSFileDescriptor.getInstance().getConfig().getTimeEncoder()),
                  TSDataType.INT64),
              Collections.singletonList(pageHeader),
              Collections.singletonList(pageData),
              Collections.singletonList(dataType),
              Collections.singletonList(
                  Decoder.getDecoderByType(chunkHeader.getEncodingType(), dataType)),
              null);
      BatchData batchData = pageReader.getAllSatisfiedPageData();
      while (batchData.hasCurrent()) {
        TsPrimitiveType[] vector = batchData.getVector();
        if (vector[0] != null) {
          distinctBinarySet.add(vector[0].getBinary());
        }
        batchData.next();
      }
    }
    dataTypePointMap.compute(
        currChunkHeader.getDataType(),
        (type, size) ->
            size == null
                ? pageHeader.getStatistics().getCount()
                : size + pageHeader.getStatistics().getCount());
  }

  @Override
  protected void onNonAlignedPage(
      PageHeader pageHeader, ByteBuffer pageData, ChunkHeader chunkHeader) throws IOException {
    TSDataType dataType = chunkHeader.getDataType();
    if (pageHeader.getStatistics() != null) {
      if (dataType == TSDataType.INT64) {
        Statistics<Long> statistics = (Statistics<Long>) pageHeader.getStatistics();
        Long minValue = statistics.getMinValue();
        Long maxValue = statistics.getMaxValue();
        if (minValue < Integer.MIN_VALUE || maxValue > Integer.MAX_VALUE) {
          currChunkJustPrecised = true;
        }
        if (maxValue - minValue > Integer.MAX_VALUE) {
          currChunkLargeRange = true;
        }
      } else if (dataType == TSDataType.TEXT || dataType == TSDataType.STRING) {
        PageReader pageReader =
            new PageReader(
                pageHeader,
                pageData,
                chunkHeader.getDataType(),
                Decoder.getDecoderByType(chunkHeader.getEncodingType(), chunkHeader.getDataType()),
                Decoder.getDecoderByType(
                    TSEncoding.valueOf(TSFileDescriptor.getInstance().getConfig().getTimeEncoder()),
                    TSDataType.INT64));
        BatchData allSatisfiedPageData = pageReader.getAllSatisfiedPageData(true);
        while (allSatisfiedPageData.hasCurrent()) {
          distinctBinarySet.add(allSatisfiedPageData.getBinary());
          allSatisfiedPageData.next();
        }
      }
      dataTypePointMap.compute(
          currChunkHeader.getDataType(),
          (type, size) ->
              size == null
                  ? pageHeader.getStatistics().getCount()
                  : size + pageHeader.getStatistics().getCount());
    } else {
      PageReader pageReader =
          new PageReader(
              pageHeader,
              pageData,
              chunkHeader.getDataType(),
              Decoder.getDecoderByType(chunkHeader.getEncodingType(), chunkHeader.getDataType()),
              Decoder.getDecoderByType(
                  TSEncoding.valueOf(TSFileDescriptor.getInstance().getConfig().getTimeEncoder()),
                  TSDataType.INT64));
      BatchData allSatisfiedPageData = pageReader.getAllSatisfiedPageData(true);
      long minLongValue = Long.MAX_VALUE;
      long maxLongValue = Long.MIN_VALUE;
      int cnt = 0;
      while (allSatisfiedPageData.hasCurrent()) {
        if (dataType == TSDataType.INT64) {
          long val = (long) allSatisfiedPageData.currentValue();
          if (val < Integer.MIN_VALUE || val > Integer.MAX_VALUE) {
            currChunkJustPrecised = true;
          }
          minLongValue = Math.min(minLongValue, val);
          maxLongValue = Math.max(maxLongValue, val);
        } else if (dataType == TSDataType.TEXT || dataType == TSDataType.STRING) {
          distinctBinarySet.add(allSatisfiedPageData.getBinary());
        }
        cnt++;
        allSatisfiedPageData.next();
      }
      int finalCnt = cnt;
      dataTypePointMap.compute(
          currChunkHeader.getDataType(), (type, size) -> size == null ? finalCnt : size + finalCnt);
      if (maxLongValue - minLongValue > Integer.MAX_VALUE) {
        currChunkLargeRange = true;
      }
    }
  }

  @Override
  protected void onException(Throwable t) {
    LOGGER.warn("meet error.", t);
  }
}
