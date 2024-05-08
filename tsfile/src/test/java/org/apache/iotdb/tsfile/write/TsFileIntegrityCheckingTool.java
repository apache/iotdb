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
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.Chunk;
import org.apache.iotdb.tsfile.read.common.IBatchDataIterator;
import org.apache.iotdb.tsfile.read.reader.IChunkReader;
import org.apache.iotdb.tsfile.read.reader.IPointReader;
import org.apache.iotdb.tsfile.read.reader.chunk.AlignedChunkReader;
import org.apache.iotdb.tsfile.read.reader.chunk.ChunkReader;
import org.apache.iotdb.tsfile.read.reader.page.PageReader;
import org.apache.iotdb.tsfile.read.reader.page.TimePageReader;
import org.apache.iotdb.tsfile.read.reader.page.ValuePageReader;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType;

import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

/** This class provide some static method to check the integrity of tsfile */
public class TsFileIntegrityCheckingTool {
  private static Logger LOG = LoggerFactory.getLogger(TsFileIntegrityCheckingTool.class);

  /**
   * This method check the integrity of file by reading it from the start to the end. It mainly
   * checks the integrity of the chunks.
   *
   * @param filename
   */
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

  /**
   * This method checks the integrity of the file by mimicking the process of the query, which reads
   * the metadata index tree first, and get the timeseries metadata list and chunk metadata list.
   * After that, this method acquires single chunk according to chunk metadata, then it deserializes
   * the chunk, and verifies the correctness of the data.
   *
   * @param filename File to be check
   * @param originData The origin data in a map format, Device -> SeriesId -> List<List<Time,Val>>,
   *     each inner list stands for a chunk.
   */
  public static void checkIntegrityByQuery(
      String filename,
      Map<String, Map<String, List<List<Pair<Long, TsPrimitiveType>>>>> originData) {
    try (TsFileSequenceReader reader = new TsFileSequenceReader(filename)) {
      Map<String, List<TimeseriesMetadata>> allTimeseriesMetadata =
          reader.getAllTimeseriesMetadata(true);
      Assert.assertEquals(originData.size(), allTimeseriesMetadata.size());
      // check each series
      for (Map.Entry<String, List<TimeseriesMetadata>> entry : allTimeseriesMetadata.entrySet()) {
        String deviceId = entry.getKey();
        List<TimeseriesMetadata> timeseriesMetadataList = entry.getValue();
        boolean vectorMode = false;
        if (timeseriesMetadataList.size() > 0
            && timeseriesMetadataList.get(0).getTSDataType() != TSDataType.VECTOR) {
          Assert.assertEquals(originData.get(deviceId).size(), timeseriesMetadataList.size());
        } else {
          vectorMode = true;
          Assert.assertEquals(originData.get(deviceId).size(), timeseriesMetadataList.size() - 1);
        }

        if (!vectorMode) {
          // check integrity of not aligned series
          for (TimeseriesMetadata timeseriesMetadata : timeseriesMetadataList) {
            // get its chunk metadata list, and read the chunk
            String measurementId = timeseriesMetadata.getMeasurementId();
            List<List<Pair<Long, TsPrimitiveType>>> originChunks =
                originData.get(deviceId).get(measurementId);
            List<IChunkMetadata> chunkMetadataList = timeseriesMetadata.getChunkMetadataList();
            Assert.assertEquals(originChunks.size(), chunkMetadataList.size());
            chunkMetadataList.sort(Comparator.comparing(IChunkMetadata::getStartTime));
            for (int i = 0; i < chunkMetadataList.size(); ++i) {
              Chunk chunk = reader.readMemChunk((ChunkMetadata) chunkMetadataList.get(i));
              ChunkReader chunkReader = new ChunkReader(chunk, null);
              List<Pair<Long, TsPrimitiveType>> originValue = originChunks.get(i);
              // deserialize the chunk and verify it with origin data
              for (int valIdx = 0; chunkReader.hasNextSatisfiedPage(); ) {
                IPointReader pointReader = chunkReader.nextPageData().getBatchDataIterator();
                while (pointReader.hasNextTimeValuePair()) {
                  TimeValuePair pair = pointReader.nextTimeValuePair();
                  Assert.assertEquals(
                      originValue.get(valIdx).left.longValue(), pair.getTimestamp());
                  Assert.assertEquals(originValue.get(valIdx++).right, pair.getValue());
                }
              }
            }
          }
        } else {
          // check integrity of vector type
          // get the timeseries metadata of the time column
          TimeseriesMetadata timeColumnMetadata = timeseriesMetadataList.get(0);
          List<IChunkMetadata> timeChunkMetadataList = timeColumnMetadata.getChunkMetadataList();
          timeChunkMetadataList.sort(Comparator.comparing(IChunkMetadata::getStartTime));

          for (int i = 1; i < timeseriesMetadataList.size(); ++i) {
            // traverse each value column
            List<IChunkMetadata> valueChunkMetadataList =
                timeseriesMetadataList.get(i).getChunkMetadataList();
            Assert.assertEquals(timeChunkMetadataList.size(), valueChunkMetadataList.size());
            List<List<Pair<Long, TsPrimitiveType>>> originDataChunks =
                originData.get(deviceId).get(timeseriesMetadataList.get(i).getMeasurementId());
            for (int chunkIdx = 0; chunkIdx < timeChunkMetadataList.size(); ++chunkIdx) {
              Chunk timeChunk =
                  reader.readMemChunk((ChunkMetadata) timeChunkMetadataList.get(chunkIdx));
              Chunk valueChunk =
                  reader.readMemChunk((ChunkMetadata) valueChunkMetadataList.get(chunkIdx));
              // construct an aligned chunk reader using time chunk and value chunk
              IChunkReader chunkReader =
                  new AlignedChunkReader(timeChunk, Collections.singletonList(valueChunk), null);
              // verify the values
              List<Pair<Long, TsPrimitiveType>> originValue = originDataChunks.get(chunkIdx);
              for (int valIdx = 0; chunkReader.hasNextSatisfiedPage(); ) {
                IBatchDataIterator pointReader = chunkReader.nextPageData().getBatchDataIterator();
                while (pointReader.hasNext()) {
                  long time = pointReader.currentTime();
                  Assert.assertEquals(originValue.get(valIdx).left.longValue(), time);
                  Assert.assertEquals(
                      originValue.get(valIdx++).right.getValue(),
                      ((TsPrimitiveType[]) pointReader.currentValue())[0].getValue());
                  pointReader.next();
                }
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
