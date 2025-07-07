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

import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.compress.IUnCompressor;
import org.apache.tsfile.encoding.decoder.Decoder;
import org.apache.tsfile.encrypt.EncryptUtils;
import org.apache.tsfile.encrypt.IDecryptor;
import org.apache.tsfile.file.MetaMarker;
import org.apache.tsfile.file.header.ChunkHeader;
import org.apache.tsfile.file.header.PageHeader;
import org.apache.tsfile.file.metadata.ChunkMetadata;
import org.apache.tsfile.file.metadata.IChunkMetadata;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.StringArrayDeviceID;
import org.apache.tsfile.file.metadata.TimeseriesMetadata;
import org.apache.tsfile.file.metadata.statistics.FloatStatistics;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.read.common.Chunk;
import org.apache.tsfile.read.common.TimeRange;
import org.apache.tsfile.read.reader.TsFileInput;
import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.tsfile.read.reader.chunk.ChunkReader.decryptAndUncompressPageData;

public class CompressedTsFileModelReader extends ModelReader {
  private static final Logger LOGGER = LoggerFactory.getLogger(CompressedTsFileModelReader.class);

  @Override
  float[] readAll(String filePath) {
    try {
      float[] values = null;
      int i = 0;
      IDeviceID deviceID = new StringArrayDeviceID("t");
      byte[] fileBytes = Files.readAllBytes(new File(filePath).toPath());
      TsFileInput tsFileInput = new ByteBufferTsFileInput(ByteBuffer.wrap(fileBytes));
      try (TsFileSequenceReader reader = new TsFileSequenceReader(tsFileInput)) {
        TimeseriesMetadata timeseriesMetadata =
            reader.getAllTimeseriesMetadata(false).get(deviceID).stream()
                .filter(t -> t.getMeasurementId().equals("v"))
                .collect(Collectors.toList())
                .get(0);
        values = new float[timeseriesMetadata.getStatistics().getCount()];

        byte marker;
        ChunkHeader chunkHeader;
        reader.position((long) TSFileConfig.MAGIC_STRING.getBytes().length + 1);
        while ((marker = reader.readMarker()) != MetaMarker.SEPARATOR) {
          switch (marker) {
            case MetaMarker.CHUNK_HEADER:
            case MetaMarker.ONLY_ONE_PAGE_CHUNK_HEADER:
            case MetaMarker.TIME_CHUNK_HEADER:
            case MetaMarker.ONLY_ONE_PAGE_TIME_CHUNK_HEADER:
              chunkHeader = reader.readChunkHeader(marker);
              reader.position(reader.position() + chunkHeader.getDataSize());
              break;
            case MetaMarker.VALUE_CHUNK_HEADER:
            case MetaMarker.ONLY_ONE_PAGE_VALUE_CHUNK_HEADER:
              chunkHeader = reader.readChunkHeader(marker);
              ByteBuffer chunkDataBuffer = reader.readChunk(-1, chunkHeader.getDataSize());

              while (chunkDataBuffer.hasRemaining()) {
                PageHeader pageHeader = null;
                if (((byte) (chunkHeader.getChunkType() & 0x3F))
                    == MetaMarker.ONLY_ONE_PAGE_CHUNK_HEADER) {
                  pageHeader = PageHeader.deserializeFrom(chunkDataBuffer, new FloatStatistics());
                } else {
                  pageHeader =
                      PageHeader.deserializeFrom(chunkDataBuffer, chunkHeader.getDataType());
                }

                ByteBuffer pageData = readCompressedPageData(pageHeader, chunkDataBuffer);
                IDecryptor decryptor = IDecryptor.getDecryptor(EncryptUtils.getEncryptParameter());
                ByteBuffer uncompressedPageData =
                    decryptAndUncompressPageData(
                        pageHeader,
                        IUnCompressor.getUnCompressor(chunkHeader.getCompressionType()),
                        pageData,
                        decryptor);
                Decoder decoder =
                    Decoder.getDecoderByType(
                        chunkHeader.getEncodingType(), chunkHeader.getDataType());

                byte[] bitmap = null;
                if (uncompressedPageData.hasRemaining()) {
                  int size = ReadWriteIOUtils.readInt(uncompressedPageData);
                  bitmap = new byte[(size + 7) / 8];
                  uncompressedPageData.get(bitmap);
                }
                while (decoder.hasNext(uncompressedPageData)) {
                  values[i++] = decoder.readFloat(uncompressedPageData);
                }
              }
              break;
            case MetaMarker.CHUNK_GROUP_HEADER:
              reader.readChunkGroupHeader();
              break;
            default:
              return values;
          }
        }
      }
      return values;
    } catch (Exception e) {
      LOGGER.error("Read TS file failed", e);
      return new float[] {0};
    }
  }

  public static class ByteBufferTsFileInput implements TsFileInput {

    public ByteBuffer buffer;

    public ByteBufferTsFileInput(ByteBuffer buffer) {
      this.buffer = buffer;
    }

    @Override
    public long size() throws IOException {
      return buffer.limit();
    }

    @Override
    public long position() throws IOException {
      return buffer.position();
    }

    @Override
    public TsFileInput position(long newPosition) throws IOException {
      buffer.position((int) newPosition);
      return this;
    }

    @Override
    public int read(ByteBuffer dst) {
      int bytesToRead = Math.min(dst.remaining(), buffer.remaining());
      if (bytesToRead == 0) {
        return 0;
      }
      ByteBuffer slice = buffer.slice();
      slice.limit(bytesToRead);
      dst.put(slice);
      buffer.position(buffer.position() + bytesToRead);
      return bytesToRead;
    }

    @Override
    public int read(ByteBuffer dst, long position) {
      ByteBuffer readBuffer = buffer.slice();
      readBuffer.position((int) position);
      int bytesToRead = Math.min(dst.remaining(), readBuffer.remaining());
      if (bytesToRead > 0) {
        readBuffer.limit(readBuffer.position() + bytesToRead);
        dst.put(readBuffer);
      }
      return bytesToRead;
    }

    @Override
    public InputStream wrapAsInputStream() throws IOException {
      return new InputStream() {
        @Override
        public int read() throws IOException {
          if (!buffer.hasRemaining()) {
            return -1;
          }
          return buffer.get() & 0xFF;
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
          if (!buffer.hasRemaining()) {
            return -1;
          }
          int toRead = Math.min(len, buffer.remaining());
          buffer.get(b, off, toRead);
          return toRead;
        }

        @Override
        public int available() throws IOException {
          return buffer.remaining();
        }
      };
    }

    @Override
    public void close() throws IOException {}

    @Override
    public String getFilePath() {
      return "memory tsfile data buffer";
    }
  }

  @Override
  List<float[]> penetrate(String filePath, List<List<Integer>> startAndEndTimeArray) {
    try {
      List<float[]> results = new ArrayList<>(startAndEndTimeArray.size());
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
          for (int i = currentQueryIndex; i < startAndEndTimeArray.size(); i++) {
            List<Integer> startAndEnd = startAndEndTimeArray.get(i);
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
            for (int i = currentQueryIndex; i < startAndEndTimeArray.size(); i++) {
              List<Integer> startAndEnd = startAndEndTimeArray.get(i);
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
            if (uncompressedPageData.hasRemaining()) {
              int size = ReadWriteIOUtils.readInt(uncompressedPageData);
              bitmap = new byte[(size + 7) / 8];
              uncompressedPageData.get(bitmap);
            }
            while (decoder.hasNext(uncompressedPageData)) {
              float[] currentQueryResult = results.get(currentQueryIndex);
              if (currentResultSetIndex >= currentQueryResult.length) {
                currentQueryIndex++;
                currentResultSetIndex = 0;
              }
              if (currentQueryIndex == startAndEndTimeArray.size()) {
                return results;
              }
              currentQueryResult = results.get(currentQueryIndex);
              float v = decoder.readFloat(uncompressedPageData);

              List<Integer> currentQueryStartAndEnd = startAndEndTimeArray.get(currentQueryIndex);
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
