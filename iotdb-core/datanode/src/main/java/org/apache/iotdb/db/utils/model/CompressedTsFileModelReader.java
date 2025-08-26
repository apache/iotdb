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

package org.apache.iotdb.db.utils.model;

import org.apache.tsfile.compress.IUnCompressor;
import org.apache.tsfile.encoding.decoder.Decoder;
import org.apache.tsfile.encrypt.IDecryptor;
import org.apache.tsfile.file.MetaMarker;
import org.apache.tsfile.file.header.ChunkHeader;
import org.apache.tsfile.file.header.PageHeader;
import org.apache.tsfile.file.metadata.ChunkMetadata;
import org.apache.tsfile.file.metadata.IChunkMetadata;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.StringArrayDeviceID;
import org.apache.tsfile.file.metadata.TimeseriesMetadata;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.read.common.Chunk;
import org.apache.tsfile.read.common.TimeRange;
import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import static org.apache.tsfile.read.reader.chunk.ChunkReader.decryptAndUncompressPageData;

public class CompressedTsFileModelReader extends ModelReader {
  private static final Logger LOGGER = LoggerFactory.getLogger(CompressedTsFileModelReader.class);

  @Override
  public List<float[]> penetrate(String filePath, List<List<Integer>> startAndEndTimeArray) {
    int pairSize = startAndEndTimeArray.size();
    Integer[] indexes = new Integer[pairSize];
    for (int i = 0; i < pairSize; i++) {
      indexes[i] = i;
    }
    Arrays.sort(
        indexes,
        Comparator.comparingInt((Integer i) -> startAndEndTimeArray.get(i).get(0))
            .thenComparingInt(i -> startAndEndTimeArray.get(i).get(1)));
    List<List<Integer>> sorted = new ArrayList<>();

    for (int sortedIndex = 0; sortedIndex < pairSize; sortedIndex++) {
      int originalIndex = indexes[sortedIndex];
      sorted.add(startAndEndTimeArray.get(originalIndex));
    }

    try {
      List<float[]> results = new ArrayList<>(pairSize);
      int currentQueryIndex = 0;
      int currentResultSetIndex = 0;
      for (List<Integer> ints : startAndEndTimeArray) {
        results.add(new float[ints.get(1) - ints.get(0) + 1]);
      }
      IDecryptor decryptor = null;
      IDeviceID deviceID = new StringArrayDeviceID("t");
      int index = 0;
      try (TsFileSequenceReader reader = new TsFileSequenceReader(filePath)) {
        TimeseriesMetadata timeseriesMetadata = reader.readTimeseriesMetadata(deviceID, "v", true);
        for (IChunkMetadata chunkMetadata : timeseriesMetadata.getChunkMetadataList()) {
          TimeRange timeRange =
              new TimeRange(chunkMetadata.getStartTime(), chunkMetadata.getEndTime());
          boolean overlap = false;
          for (int i = currentQueryIndex; i < pairSize; i++) {
            List<Integer> startAndEnd = sorted.get(i);
            if (timeRange.overlaps(new TimeRange(startAndEnd.get(0), startAndEnd.get(1)))) {
              overlap = true;
              break;
            }
          }
          if (!overlap) {
            index += chunkMetadata.getStatistics().getCount();
            continue;
          }

          Chunk chunk = reader.readMemChunk((ChunkMetadata) chunkMetadata);
          ChunkHeader chunkHeader = chunk.getHeader();
          ByteBuffer chunkDataBuffer = chunk.getData();
          while (chunkDataBuffer.hasRemaining()) {
            PageHeader pageHeader = null;
            if (((byte) (chunkHeader.getChunkType() & 0x3F))
                == MetaMarker.ONLY_ONE_PAGE_CHUNK_HEADER) {
              pageHeader = PageHeader.deserializeFrom(chunkDataBuffer, chunk.getChunkStatistic());
            } else {
              pageHeader = PageHeader.deserializeFrom(chunkDataBuffer, chunkHeader.getDataType());
            }

            ByteBuffer pageData = readCompressedPageData(pageHeader, chunkDataBuffer);
            TimeRange pageTimeRange =
                new TimeRange(pageHeader.getStartTime(), pageHeader.getEndTime());
            boolean pageOverlap = false;
            for (int i = currentQueryIndex; i < pairSize; i++) {
              List<Integer> startAndEnd = sorted.get(i);
              if (pageTimeRange.overlaps(new TimeRange(startAndEnd.get(0), startAndEnd.get(1)))) {
                pageOverlap = true;
                break;
              }
            }
            if (!pageOverlap) {
              index += pageHeader.getStatistics().getCount();
              continue;
            }
            decryptor =
                decryptor == null ? IDecryptor.getDecryptor(chunk.getEncryptParam()) : decryptor;
            ByteBuffer uncompressedPageData =
                decryptAndUncompressPageData(
                    pageHeader,
                    IUnCompressor.getUnCompressor(chunkHeader.getCompressionType()),
                    pageData,
                    decryptor);
            Decoder decoder =
                Decoder.getDecoderByType(chunkHeader.getEncodingType(), chunkHeader.getDataType());

            byte[] bitmap = null;
            int size = 0;
            if (uncompressedPageData.hasRemaining()) {
              size = ReadWriteIOUtils.readInt(uncompressedPageData);
              bitmap = new byte[(size + 7) / 8];
              uncompressedPageData.get(bitmap);
            }
            for (int i = 0; i < size; i++) {
              float[] currentQueryResult = results.get(indexes[currentQueryIndex]);
              if (currentResultSetIndex >= currentQueryResult.length) {
                currentQueryIndex++;
                currentResultSetIndex = 0;
              }
              if (currentQueryIndex == pairSize) {
                return results;
              }
              currentQueryResult = results.get(indexes[currentQueryIndex]);
              float v = decoder.readFloat(uncompressedPageData);

              List<Integer> currentQueryStartAndEnd = sorted.get(currentQueryIndex);
              if (index >= currentQueryStartAndEnd.get(0)
                  && index <= currentQueryStartAndEnd.get(1)) {
                currentQueryResult[currentResultSetIndex++] = v;
              }
              index++;
            }
          }
        }
      }
      return results;
    } catch (Exception e) {
      LOGGER.error("Penetrate TS file failed", e);
      return new ArrayList<>();
    }
  }

  public static ByteBuffer readCompressedPageData(PageHeader pageHeader, ByteBuffer chunkBuffer)
      throws IOException {
    int compressedPageBodyLength = pageHeader.getCompressedSize();
    if (compressedPageBodyLength > chunkBuffer.remaining()) {
      throw new IOException(
          "do not have a complete page body. Expected:"
              + compressedPageBodyLength
              + ". Actual:"
              + chunkBuffer.remaining());
    }
    ByteBuffer pageBodyBuffer = chunkBuffer.slice();
    pageBodyBuffer.limit(compressedPageBodyLength);
    chunkBuffer.position(chunkBuffer.position() + compressedPageBodyLength);
    return pageBodyBuffer;
  }
}
