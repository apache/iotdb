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

package org.apache.tsfile;

import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.common.constant.TsFileConstant;
import org.apache.tsfile.encoding.decoder.Decoder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.MetaMarker;
import org.apache.tsfile.file.header.ChunkGroupHeader;
import org.apache.tsfile.file.header.ChunkHeader;
import org.apache.tsfile.file.header.PageHeader;
import org.apache.tsfile.file.metadata.ChunkMetadata;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.fileSystem.FSFactoryProducer;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.read.common.BatchData;
import org.apache.tsfile.read.reader.page.PageReader;
import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.apache.tsfile.write.UnSupportedDataTypeException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/** This tool is used to read TsFile sequentially, including nonAligned or aligned timeseries. */
public class TsFileSequenceRead {
  // if you wanna print detailed datas in pages, then turn it true.
  private static boolean printDetail = false;
  public static final String POINT_IN_PAGE = "\t\tpoints in the page: ";
  private static int MASK = 0x80;

  @SuppressWarnings({
    "squid:S3776",
    "squid:S106"
  }) // Suppress high Cognitive Complexity and Standard outputs warning
  public static void main(String[] args) throws IOException {
    String filename = "test.tsfile";
    if (args.length >= 1) {
      filename = args[0];
    }
    try (TsFileSequenceReader reader = new TsFileSequenceReader(filename)) {
      System.out.println(
          "file length: " + FSFactoryProducer.getFSFactory().getFile(filename).length());
      System.out.println("file magic head: " + reader.readHeadMagic());
      System.out.println("file magic tail: " + reader.readTailMagic());
      System.out.println("Level 1 metadata position: " + reader.getFileMetadataPos());
      System.out.println("Level 1 metadata size: " + reader.getTsFileMetadataSize());
      // Sequential reading of one ChunkGroup now follows this order:
      // first the CHUNK_GROUP_HEADER, then SeriesChunks (headers and data) in one ChunkGroup
      // Because we do not know how many chunks a ChunkGroup may have, we should read one byte (the
      // marker) ahead and judge accordingly.
      reader.position((long) TSFileConfig.MAGIC_STRING.getBytes().length + 1);
      System.out.println("position: " + reader.position());
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
            System.out.println("\t[Chunk]");
            System.out.println("\tchunk type: " + marker);
            System.out.println("\tposition: " + reader.position());
            ChunkHeader header = reader.readChunkHeader(marker);
            System.out.println("\tMeasurement: " + header.getMeasurementID());
            if (header.getDataSize() == 0) {
              // empty value chunk
              System.out.println("\t-- Empty Chunk ");
              break;
            }
            System.out.println(
                "\tChunk Size: " + (header.getDataSize() + header.getSerializedSize()));
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
              System.out.println(
                  "\t\t[Page" + pageIndex + "]\n \t\tPage head position: " + reader.position());
              PageHeader pageHeader =
                  reader.readPageHeader(
                      header.getDataType(),
                      (header.getChunkType() & 0x3F) == MetaMarker.CHUNK_HEADER);
              System.out.println("\t\tPage data position: " + reader.position());
              ByteBuffer pageData = reader.readPage(pageHeader, header.getCompressionType());
              System.out.println(
                  "\t\tUncompressed page data size: " + pageHeader.getUncompressedSize());
              System.out.println(
                  "\t\tCompressed page data size: " + pageHeader.getCompressedSize());
              if ((header.getChunkType() & TsFileConstant.TIME_COLUMN_MASK)
                  == TsFileConstant.TIME_COLUMN_MASK) { // Time Chunk
                Decoder decoder =
                    Decoder.getDecoderByType(header.getEncodingType(), header.getDataType());
                while (decoder.hasNext(pageData)) {
                  long currentTime = decoder.readLong(pageData);
                  if (printDetail) {
                    System.out.println("\t\t\ttime: " + currentTime);
                  }
                }
              } else if ((header.getChunkType() & TsFileConstant.VALUE_COLUMN_MASK)
                  == TsFileConstant.VALUE_COLUMN_MASK) { // Value Chunk
                int pointNum = 0;
                byte[] bitmap = null;
                if (pageData.hasRemaining()) {
                  int size = ReadWriteIOUtils.readInt(pageData);
                  bitmap = new byte[(size + 7) / 8];
                  pageData.get(bitmap);
                }
                while (valueDecoder.hasNext(pageData)) {
                  pointNum++;
                  int idx = pointNum - 1;
                  if (((bitmap[idx / 8] & 0xFF) & (MASK >>> (idx % 8))) == 0) {
                    if (printDetail) {
                      System.out.println("\t\t\tvalue: " + null);
                    }
                    continue;
                  }
                  Object value;
                  switch (header.getDataType()) {
                    case BOOLEAN:
                      value = valueDecoder.readBoolean(pageData);
                      break;
                    case INT32:
                      value = valueDecoder.readInt(pageData);
                      break;
                    case INT64:
                      value = valueDecoder.readLong(pageData);
                      break;
                    case FLOAT:
                      value = valueDecoder.readFloat(pageData);
                      break;
                    case DOUBLE:
                      value = valueDecoder.readDouble(pageData);
                      break;
                    case TEXT:
                      value = valueDecoder.readBinary(pageData);
                      break;
                    default:
                      throw new UnSupportedDataTypeException(String.valueOf(header.getDataType()));
                  }
                  if (printDetail) {
                    System.out.println("\t\t\tvalue: " + value);
                  }
                }
                pageData.flip();
                if (pointNum == 0) {
                  System.out.println("\t\t-- Empty Page ");
                } else {
                  System.out.println(POINT_IN_PAGE + pointNum);
                }
              } else { // NonAligned Chunk
                PageReader pageReader =
                    new PageReader(
                        pageData, header.getDataType(), valueDecoder, defaultTimeDecoder);
                BatchData batchData = pageReader.getAllSatisfiedPageData();
                if (header.getChunkType() == MetaMarker.CHUNK_HEADER) {
                  System.out.println(POINT_IN_PAGE + pageHeader.getNumOfValues());
                } else {
                  System.out.println(POINT_IN_PAGE + batchData.length());
                }
                if (printDetail) {
                  while (batchData.hasCurrent()) {
                    System.out.println(
                        "\t\t\ttime, value: "
                            + batchData.currentTime()
                            + ", "
                            + batchData.currentValue());
                    batchData.next();
                  }
                }
              }
              pageIndex++;
              dataSize -= pageHeader.getSerializedPageSize();
            }
            break;
          case MetaMarker.CHUNK_GROUP_HEADER:
            System.out.println("[Chunk Group]");
            System.out.println("Chunk Group Header position: " + reader.position());
            ChunkGroupHeader chunkGroupHeader = reader.readChunkGroupHeader();
            System.out.println("device: " + chunkGroupHeader.getDeviceID());
            break;
          case MetaMarker.OPERATION_INDEX_RANGE:
            reader.readPlanIndex();
            System.out.println("minPlanIndex: " + reader.getMinPlanIndex());
            System.out.println("maxPlanIndex: " + reader.getMaxPlanIndex());
            break;
          default:
            MetaMarker.handleUnexpectedMarker(marker);
        }
      }
      System.out.println("[Metadata]");
      for (IDeviceID device : reader.getAllDevices()) {
        Map<String, List<ChunkMetadata>> seriesMetaData = reader.readChunkMetadataInDevice(device);
        System.out.printf(
            "\t[Device]Device %s, Number of Measurements %d%n", device, seriesMetaData.size());
        for (Map.Entry<String, List<ChunkMetadata>> serie : seriesMetaData.entrySet()) {
          System.out.println("\t\tMeasurement:" + serie.getKey());
          for (ChunkMetadata chunkMetadata : serie.getValue()) {
            System.out.println("\t\tFile offset:" + chunkMetadata.getOffsetOfChunkHeader());
          }
        }
      }
    }
  }
}
