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
package org.apache.iotdb.tsfile;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.iotdb.tsfile.exception.filter.UnSupportFilterDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.ChunkGroupMetaData;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetaData;
import org.apache.iotdb.tsfile.file.metadata.TsDeviceMetadata;
import org.apache.iotdb.tsfile.file.metadata.TsDeviceMetadataIndex;
import org.apache.iotdb.tsfile.file.metadata.TsDigest;
import org.apache.iotdb.tsfile.file.metadata.TsDigest.StatisticType;
import org.apache.iotdb.tsfile.file.metadata.TsFileMetaData;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.fileSystem.TSFileFactory;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.Chunk;
import org.apache.iotdb.tsfile.utils.BytesUtils;

public class TsFileSketchTool {

  public static void main(String[] args) throws IOException {
    String filename = "test.tsfile";
    String outFile = "TsFile_sketch_view.txt";
    if (args.length == 1) {
      filename = args[0];
    } else if (args.length == 2) {
      filename = args[0];
      outFile = args[1];
    }
    PrintWriter pw = new PrintWriter(new FileWriter(outFile));
    long length = TSFileFactory.INSTANCE.getFile(filename).length();
    printlnBoth(pw,
        "-------------------------------- TsFile Sketch --------------------------------");
    printlnBoth(pw, "file path: " + filename);
    printlnBoth(pw, "file length: " + length);

    // get metadata information
    TsFileSequenceReader reader = new TsFileSequenceReader(filename);
    TsFileMetaData tsFileMetaData = reader.readFileMetadata();
    List<TsDeviceMetadataIndex> tsDeviceMetadataIndexSortedList = tsFileMetaData.getDeviceMap()
        .values()
        .stream()
        .sorted((x, y) -> (int) (x.getOffset() - y.getOffset())).collect(Collectors.toList());
    List<ChunkGroupMetaData> chunkGroupMetaDataTmpList = new ArrayList<>();
    List<TsDeviceMetadata> tsDeviceMetadataSortedList = new ArrayList<>();
    for (TsDeviceMetadataIndex index : tsDeviceMetadataIndexSortedList) {
      TsDeviceMetadata deviceMetadata = reader.readTsDeviceMetaData(index);
      tsDeviceMetadataSortedList.add(deviceMetadata);
      chunkGroupMetaDataTmpList.addAll(deviceMetadata.getChunkGroupMetaDataList());
    }
    List<ChunkGroupMetaData> chunkGroupMetaDataSortedList = chunkGroupMetaDataTmpList.stream()
        .sorted((x, y) -> (int) (x.getStartOffsetOfChunkGroup() - y.getStartOffsetOfChunkGroup()))
        .collect(Collectors.toList());

    // begin print
    StringBuilder str1 = new StringBuilder();
    StringBuilder str2 = new StringBuilder();
    for (int i = 0; i < 21; i++) {
      str1.append(">");
      str2.append("<");
    }

    printlnBoth(pw, "");
    printlnBoth(pw, String.format("%20s", "POSITION") + "|\tCONTENT");
    printlnBoth(pw, String.format("%20s", "--------") + " \t-------");
    printlnBoth(pw, String.format("%20d", 0) + "|\t[magic head] " + reader.readHeadMagic());
    for (ChunkGroupMetaData chunkGroupMetaData : chunkGroupMetaDataSortedList) {
      printlnBoth(pw, str1.toString() + "\t[Chunk Group] of "
          + chunkGroupMetaData.getDeviceID() + " begins at pos " + chunkGroupMetaData
          .getStartOffsetOfChunkGroup() + ", ends at pos " + chunkGroupMetaData
          .getEndOffsetOfChunkGroup() + ", version:" + chunkGroupMetaData.getVersion()
          + ", num of Chunks:" + chunkGroupMetaData.getChunkMetaDataList().size());
      for (ChunkMetaData chunkMetaData : chunkGroupMetaData.getChunkMetaDataList()) {
        printlnBoth(pw,
            String.format("%20d", chunkMetaData.getOffsetOfChunkHeader()) + "|\t[Chunk] of "
                + chunkMetaData.getMeasurementUid() + ", numOfPoints:" + chunkMetaData
                .getNumOfPoints() + ", time range:[" + chunkMetaData.getStartTime() + ","
                + chunkMetaData.getEndTime() + "], tsDataType:" + chunkMetaData.getTsDataType()
                + ", \n" + String.format("%20s", "") + " \t" + statisticByteBufferToString(
                chunkMetaData.getTsDataType(),
                chunkMetaData.getDigest()));
        printlnBoth(pw, String.format("%20s", "") + "|\t\t[marker] 1");
        printlnBoth(pw, String.format("%20s", "") + "|\t\t[ChunkHeader]");
        Chunk chunk = reader.readMemChunk(chunkMetaData);
        printlnBoth(pw,
            String.format("%20s", "") + "|\t\tnum of pages: " + chunk.getHeader()
                .getNumOfPages());
      }
      printlnBoth(pw, String.format("%20s", "") + "|\t[marker] 0");
      printlnBoth(pw, String.format("%20s", "") + "|\t[Chunk Group Footer]");
      printlnBoth(pw, str2.toString() + "\t[Chunk Group] of "
          + chunkGroupMetaData.getDeviceID() + " ends");
    }

    printlnBoth(pw, String.format("%20s", tsDeviceMetadataIndexSortedList.get(0).getOffset() - 1)
        + "|\t[marker] 2");
    for (
        int i = 0; i < tsDeviceMetadataSortedList.size(); i++) {
      TsDeviceMetadata tsDeviceMetadata = tsDeviceMetadataSortedList.get(i);
      TsDeviceMetadataIndex tsDeviceMetadataIndex = tsDeviceMetadataIndexSortedList.get(i);
      printlnBoth(pw,
          String.format("%20s", tsDeviceMetadataIndex.getOffset())
              + "|\t[TsDeviceMetadata] of " + tsDeviceMetadata.getChunkGroupMetaDataList().get(0)
              .getDeviceID() + ", startTime:" + tsDeviceMetadataIndex.getStartTime() + ", endTime:"
              + tsDeviceMetadataIndex.getEndTime());
      printlnBoth(pw, String.format("%20s", "") +
          "|\t\tnum of ChunkGroupMetaData: " + tsDeviceMetadata.getChunkGroupMetaDataList()
          .size());
    }

    printlnBoth(pw, String.format("%20s", reader.getFileMetadataPos()) + "|\t[TsFileMetaData]");

    printlnBoth(pw,
        String.format("%20s", "") + "|\t\tnum of TsDeviceMetadataIndex: " + tsFileMetaData
            .getDeviceMap().size());

    printlnBoth(pw, String.format("%20s", "") + "|\t\tnum of measurementSchema: " + tsFileMetaData
        .getMeasurementSchema().size());

    printlnBoth(pw,
        String.format("%20s", (reader.getFileMetadataPos() + reader.getFileMetadataSize()))
            + "|\t[TsFileMetaDataSize] " + reader.getFileMetadataSize());

    printlnBoth(pw,
        String.format("%20s", reader.getFileMetadataPos() + reader.getFileMetadataSize() + 4)
            + "|\t[magic tail] " + reader.readTailMagic());

    printlnBoth(pw,
        String.format("%20s", length) + "|\tEND of TsFile");

    printlnBoth(pw, "");

    printlnBoth(pw,
        "---------------------------------- TsFile Sketch End ----------------------------------");
    pw.close();
  }

  private static void printlnBoth(PrintWriter pw, String str) {
    System.out.println(str);
    pw.println(str);
  }

  private static String statisticByteBufferToString(TSDataType tsDataType, TsDigest tsDigest) {
    ByteBuffer[] statistics = tsDigest.getStatistics();
    if (statistics == null) {
      return "TsDigest:[]";
    }
    StringBuilder str = new StringBuilder();
    str.append("TsDigest:[");
    for (int i = 0; i < statistics.length - 1; i++) {
      ByteBuffer value = statistics[i];
      str.append(StatisticType.values()[i]);
      str.append(":");
      if (value == null) {
        str.append("null");
      } else {
        switch (tsDataType) {
          case INT32:
            str.append(BytesUtils.bytesToInt(value.array()));
            break;
          case INT64:
            str.append(BytesUtils.bytesToLong(value.array()));
            break;
          case FLOAT:
            str.append(BytesUtils.bytesToFloat(value.array()));
            break;
          case DOUBLE:
            str.append(BytesUtils.bytesToDouble(value.array()));
            break;
          case TEXT:
            str.append(BytesUtils.bytesToString(value.array()));
            break;
          case BOOLEAN:
            str.append(BytesUtils.bytesToBool(value.array()));
            break;
          default:
            throw new UnSupportFilterDataTypeException(
                "DigestForFilter unsupported datatype : " + tsDataType.toString());
        }
      }
      str.append(",");
    }
    // Note that the last statistic of StatisticType is sum_value, which is double.
    str.append(StatisticType.values()[statistics.length - 1]);
    str.append(":");
    ByteBuffer value = statistics[statistics.length - 1];
    if (value == null) {
      str.append("null");
    } else {
      str.append(BytesUtils.bytesToDouble(value.array()));
    }
    str.append("]");

    return str.toString();
  }

}