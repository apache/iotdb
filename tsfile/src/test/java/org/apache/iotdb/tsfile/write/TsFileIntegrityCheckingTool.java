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

package org.apache.iotdb.tsfile.write;

import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;
import org.apache.iotdb.tsfile.encoding.decoder.Decoder;
import org.apache.iotdb.tsfile.file.MetaMarker;
import org.apache.iotdb.tsfile.file.header.ChunkGroupHeader;
import org.apache.iotdb.tsfile.file.header.ChunkHeader;
import org.apache.iotdb.tsfile.file.header.PageHeader;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.IChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.TimeseriesMetadata;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.Chunk;
import org.apache.iotdb.tsfile.read.reader.IPointReader;
import org.apache.iotdb.tsfile.read.reader.chunk.ChunkReader;
import org.apache.iotdb.tsfile.read.reader.page.PageReader;
import org.apache.iotdb.tsfile.read.reader.page.TimePageReader;
import org.apache.iotdb.tsfile.read.reader.page.ValuePageReader;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType;

import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

/** This class provide some static method to check the integrity of tsfile */
public class TsFileIntegrityCheckingTool {
  private static Logger LOG = LoggerFactory.getLogger(TsFileIntegrityCheckingTool.class);

  public static void checkIntegrityBySequenceRead(String filename) {
    try (TsFileSequenceReader reader = new TsFileSequenceReader(filename)) {
      String headMagicString = reader.readHeadMagic();
      Assert.assertEquals(TSFileConfig.MAGIC_STRING, headMagicString);
      String tailMagicString = reader.readTailMagic();
      Assert.assertEquals(TSFileConfig.MAGIC_STRING, tailMagicString);
      reader.position((long) TSFileConfig.MAGIC_STRING.getBytes().length + 1);
      List<long[]> timeBatch = new ArrayList<>();
      int pageIndex = 0;
      byte marker;
      while ((marker = reader.readMarker()) != MetaMarker.SEPARATOR) {
        switch (marker) {
          case MetaMarker.CHUNK_HEADER:
          case MetaMarker.TIME_CHUNK_HEADER:
          case MetaMarker.VALUE_CHUNK_HEADER:
          case MetaMarker.ONLY_ONE_PAGE_CHUNK_HEADER:
          case MetaMarker.ONLY_ONE_PAGE_TIME_CHUNK_HEADER:
          case MetaMarker.ONLY_ONE_PAGE_VALUE_CHUNK_HEADER:
            ChunkHeader header = reader.readChunkHeader(marker);
            if (header.getDataSize() == 0) {
              // empty value chunk
              break;
            }
            Decoder defaultTimeDecoder =
                Decoder.getDecoderByType(
                    TSEncoding.valueOf(TSFileDescriptor.getInstance().getConfig().getTimeEncoder()),
                    TSDataType.INT64);
            Decoder valueDecoder =
                Decoder.getDecoderByType(header.getEncodingType(), header.getDataType());
            int dataSize = header.getDataSize();
            pageIndex = 0;
            if (header.getDataType() == TSDataType.VECTOR) {
              timeBatch.clear();
            }
            while (dataSize > 0) {
              valueDecoder.reset();
              PageHeader pageHeader =
                  reader.readPageHeader(
                      header.getDataType(),
                      (header.getChunkType() & 0x3F) == MetaMarker.CHUNK_HEADER);
              ByteBuffer pageData = reader.readPage(pageHeader, header.getCompressionType());
              if ((header.getChunkType() & (byte) TsFileConstant.TIME_COLUMN_MASK)
                  == (byte) TsFileConstant.TIME_COLUMN_MASK) { // Time Chunk
                TimePageReader timePageReader =
                    new TimePageReader(pageHeader, pageData, defaultTimeDecoder);
                timeBatch.add(timePageReader.getNextTimeBatch());
              } else if ((header.getChunkType() & (byte) TsFileConstant.VALUE_COLUMN_MASK)
                  == (byte) TsFileConstant.VALUE_COLUMN_MASK) { // Value Chunk
                ValuePageReader valuePageReader =
                    new ValuePageReader(pageHeader, pageData, header.getDataType(), valueDecoder);
                TsPrimitiveType[] valueBatch =
                    valuePageReader.nextValueBatch(timeBatch.get(pageIndex));
              } else { // NonAligned Chunk
                PageReader pageReader =
                    new PageReader(
                        pageData, header.getDataType(), valueDecoder, defaultTimeDecoder, null);
                BatchData batchData = pageReader.getAllSatisfiedPageData();
              }
              pageIndex++;
              dataSize -= pageHeader.getSerializedPageSize();
            }
            break;
          case MetaMarker.CHUNK_GROUP_HEADER:
            ChunkGroupHeader chunkGroupHeader = reader.readChunkGroupHeader();
            break;
          case MetaMarker.OPERATION_INDEX_RANGE:
            reader.readPlanIndex();
            break;
          default:
            MetaMarker.handleUnexpectedMarker(marker);
        }
      }
    } catch (IOException e) {
      LOG.error("Meet exception when checking integrity of tsfile", e);
      Assert.fail();
    }
  }

  public static void checkIntegrityByQuery(
      String filename, Map<String, Map<String, List<List<Long>>>> originData) {
    try (TsFileSequenceReader reader = new TsFileSequenceReader(filename)) {
      Map<String, List<TimeseriesMetadata>> allTimeseriesMetadata =
          reader.getAllTimeseriesMetadata(true);
      Assert.assertEquals(originData.size(), allTimeseriesMetadata.size());
      for (Map.Entry<String, List<TimeseriesMetadata>> entry : allTimeseriesMetadata.entrySet()) {
        String deviceId = entry.getKey();
        List<TimeseriesMetadata> timeseriesMetadataList = entry.getValue();
        Assert.assertEquals(originData.get(deviceId).size(), timeseriesMetadataList.size());
        for (TimeseriesMetadata timeseriesMetadata : timeseriesMetadataList) {
          String measurementId = timeseriesMetadata.getMeasurementId();
          List<List<Long>> originChunks = originData.get(deviceId).get(measurementId);
          List<IChunkMetadata> chunkMetadataList = timeseriesMetadata.getChunkMetadataList();
          Assert.assertEquals(originChunks.size(), chunkMetadataList.size());
          chunkMetadataList.sort(Comparator.comparing(IChunkMetadata::getStartTime));
          for (int i = 0; i < chunkMetadataList.size(); ++i) {
            Chunk chunk = reader.readMemChunk((ChunkMetadata) chunkMetadataList.get(i));
            ChunkReader chunkReader = new ChunkReader(chunk, null);
            List<Long> originValue = originChunks.get(i);
            for (int valIdx = 0; chunkReader.hasNextSatisfiedPage(); ) {
              IPointReader pointReader = chunkReader.nextPageData().getBatchDataIterator();
              while (pointReader.hasNextTimeValuePair()) {
                Assert.assertEquals(
                    originValue.get(valIdx++).longValue(),
                    pointReader.nextTimeValuePair().getTimestamp());
              }
            }
          }
        }
      }

    } catch (IOException e) {
      LOG.error("Meet exception when checking integrity of tsfile", e);
      Assert.fail();
    }
  }
}
