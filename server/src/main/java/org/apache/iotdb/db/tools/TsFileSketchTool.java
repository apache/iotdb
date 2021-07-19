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

package org.apache.iotdb.db.tools;

import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.file.MetaMarker;
import org.apache.iotdb.tsfile.file.header.ChunkGroupHeader;
import org.apache.iotdb.tsfile.file.metadata.*;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.Chunk;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.utils.BloomFilter;
import org.apache.iotdb.tsfile.utils.Pair;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class TsFileSketchTool {

  public static void main(String[] args) throws IOException {
//    Pair<String, String> fileNames = checkArgs(args);
//    String filename = fileNames.left;
    String filename = "D:\\JavaSpace\\iotdb\\iotdb\\data\\data\\unsequence\\root.sg_1\\0\\0\\1626691990335-41-0-0.tsfile";
//    String outFile = fileNames.right;
    String outFile = "D:\\JavaSpace\\iotdb\\iotdb\\data\\data\\unsequence\\root.sg_1\\0\\0\\temp";
    System.out.println("TsFile path:" + filename);
    System.out.println("Sketch save path:" + outFile);
    try (PrintWriter pw = new PrintWriter(new FileWriter(outFile))) {
      long length = FSFactoryProducer.getFSFactory().getFile(filename).length();
      printlnBoth(
          pw, "-------------------------------- TsFile Sketch --------------------------------");
      printlnBoth(pw, "file path: " + filename);
      printlnBoth(pw, "file length: " + length);

      // get metadata information
      try (TsFileSequenceReader reader = new TsFileSequenceReader(filename)) {
        TsFileMetadata tsFileMetaData = reader.readFileMetadata();
        List<ChunkGroupMetadata> allChunkGroupMetadata = new ArrayList<>();
        reader.selfCheck(null, allChunkGroupMetadata, false);

        // begin print
        StringBuilder str1 = new StringBuilder();
        for (int i = 0; i < 21; i++) {
          str1.append("|");
        }

        printlnBoth(pw, "");
        printlnBoth(pw, String.format("%20s", "POSITION") + "|\tCONTENT");
        printlnBoth(pw, String.format("%20s", "--------") + " \t-------");
        printlnBoth(pw, String.format("%20d", 0) + "|\t[magic head] " + reader.readHeadMagic());
        printlnBoth(
            pw,
            String.format("%20d", TSFileConfig.MAGIC_STRING.getBytes().length)
                + "|\t[version number] "
                + reader.readVersionNumber());
        long nextChunkGroupHeaderPos =
            (long) TSFileConfig.MAGIC_STRING.getBytes().length + Byte.BYTES;
        // ChunkGroup begins
        for (ChunkGroupMetadata chunkGroupMetadata : allChunkGroupMetadata) {
          printlnBoth(
              pw,
              str1
                  + "\t[Chunk Group] of "
                  + chunkGroupMetadata.getDevice()
                  + ", num of Chunks:"
                  + chunkGroupMetadata.getChunkMetadataList().size());
          // chunkGroupHeader begins
          printlnBoth(
              pw, String.format("%20s", nextChunkGroupHeaderPos) + "|\t[Chunk Group Header]");
          ChunkGroupHeader chunkGroupHeader =
              reader.readChunkGroupHeader(nextChunkGroupHeaderPos, false);
          printlnBoth(pw, String.format("%20s", "") + "|\t\t[marker] 0");
          printlnBoth(
              pw, String.format("%20s", "") + "|\t\t[deviceID] " + chunkGroupHeader.getDeviceID());
          // chunk begins
          for (ChunkMetadata chunkMetadata : chunkGroupMetadata.getChunkMetadataList()) {
            Chunk chunk = reader.readMemChunk(chunkMetadata);
            printlnBoth(
                pw,
                String.format("%20d", chunkMetadata.getOffsetOfChunkHeader())
                    + "|\t[Chunk] of "
                    + chunkMetadata.getMeasurementUid()
                    + ", numOfPoints:"
                    + chunkMetadata.getNumOfPoints()
                    + ", time range:["
                    + chunkMetadata.getStartTime()
                    + ","
                    + chunkMetadata.getEndTime()
                    + "], tsDataType:"
                    + chunkMetadata.getDataType()
                    + ", \n"
                    + String.format("%20s", "")
                    + " \t"
                    + chunkMetadata.getStatistics());
            printlnBoth(
                pw,
                String.format("%20s", "") + "|\t\t[marker] " + chunk.getHeader().getChunkType());
            nextChunkGroupHeaderPos =
                chunkMetadata.getOffsetOfChunkHeader()
                    + chunk.getHeader().getSerializedSize()
                    + chunk.getHeader().getDataSize();
          }
          reader.position(nextChunkGroupHeaderPos);
          byte marker = reader.readMarker();
          switch (marker) {
            case MetaMarker.CHUNK_GROUP_HEADER:
              // do nothing
              break;
            case MetaMarker.OPERATION_INDEX_RANGE:
              // skip the PlanIndex
              nextChunkGroupHeaderPos += 16;
              break;
          }

          printlnBoth(pw, str1 + "\t[Chunk Group] of " + chunkGroupMetadata.getDevice() + " ends");
        }

        // metadata begins
        if (tsFileMetaData.getMetadataIndex().getChildren().isEmpty()) {
          printlnBoth(pw, String.format("%20s", reader.getFileMetadataPos() - 1) + "|\t[marker] 2");
        } else {
          printlnBoth(
              pw,
              String.format("%20s", reader.readFileMetadata().getMetaOffset()) + "|\t[marker] 2");
        }

        Map<String, List<TimeseriesMetadata>> allTimeseriesMetadata =
            reader.getAllTimeseriesMetadata();
        Map<String, Pair<Path, TimeseriesMetadata>> timeseriesMetadataMap = new TreeMap<>();

        for (Map.Entry<String, List<TimeseriesMetadata>> entry : allTimeseriesMetadata.entrySet()) {
          String device = entry.getKey();
          List<TimeseriesMetadata> seriesMetadataList = entry.getValue();
          for (TimeseriesMetadata seriesMetadata : seriesMetadataList) {
            timeseriesMetadataMap.put(
                seriesMetadata.getMeasurementId(),
                new Pair<>(new Path(device, seriesMetadata.getMeasurementId()), seriesMetadata));
          }
        }
        for (Map.Entry<String, Pair<Path, TimeseriesMetadata>> entry : timeseriesMetadataMap.entrySet()) {
          printlnBoth(
              pw,
              entry.getKey()
                  + "|\t[ChunkMetadataList] of "
                  + entry.getValue().left
                  + ", tsDataType:"
                  + entry.getValue().right.getTSDataType()
          );
          for(IChunkMetadata chunkMetadata:reader.getChunkMetadataList(entry.getValue().left)){
            printlnBoth(pw,
                    String.format("%20s", "") + "|\t\t[ChunkMetadata] "
                            + chunkMetadata.getMeasurementUid()
                            + ", offset="
                            + chunkMetadata.getOffsetOfChunkHeader());
          }
          printlnBoth(
              pw,
              String.format("%20s", "") + "|\t\t[" + entry.getValue().right.getStatistics() + "] ");
        }

        MetadataIndexNode metadataIndexNode = tsFileMetaData.getMetadataIndex();
        printlnBoth(
                pw,
                String.format("%20s", "")
                        + "|\t[MetadataIndex:" + metadataIndexNode.getNodeType()+"]");
        for (MetadataIndexEntry metadataIndex : tsFileMetaData.getMetadataIndex().getChildren()) {
          metadataIndexNode = reader.getMetadataIndexNode(1513,1563);
          printlnBoth(
              pw,
              String.format("%20s", "")
                      + "|\t\t\t|________<"
                      + metadataIndex.getName()+","
                      + metadataIndex.getOffset()
                      + ">________"
                      + "[MetadataIndex:" + metadataIndexNode.getNodeType()+"]");

        }
        // TODO：怎么递归查出子树啊。。只知道前面的offset，长度/后offset不知道啊。。。
        for (MetadataIndexEntry metadataIndex : metadataIndexNode.getChildren()){
          printlnBoth(
                  pw,
                  String.format("%20s", "")
                          + "|\t\t\t\t\t|________<"
                          + metadataIndex.getName()+","
                          + metadataIndex.getOffset()
                          + ">");
        }

        printlnBoth(pw, String.format("%20s", reader.getFileMetadataPos()) + "|\t[TsFileMetadata]");
        printlnBoth(
            pw,
            String.format("%20s", "")
                + "|\t\t[meta offset] "
                + tsFileMetaData.getMetaOffset());
        printlnBoth(
            pw,
            String.format("%20s", "")
                + "|\t\t[num of devices] "
                + tsFileMetaData.getMetadataIndex().getChildren().size());
        printlnBoth(
            pw,
            String.format("%20s", "")
                + "|\t\t"
                + tsFileMetaData.getMetadataIndex().getChildren().size()
                + " key&TsMetadataIndex");

        // bloom filter
        BloomFilter bloomFilter = tsFileMetaData.getBloomFilter();
        printlnBoth(
            pw,
            String.format("%20s", "")
                + "|\t\t[bloom filter bit vector byte array length] "
                + bloomFilter.serialize().length);
        printlnBoth(pw, String.format("%20s", "") + "|\t\t[bloom filter bit vector byte array] ");
        printlnBoth(
            pw,
            String.format("%20s", "")
                + "|\t\t[bloom filter number of bits] "
                + bloomFilter.getSize());
        printlnBoth(
            pw,
            String.format("%20s", "")
                + "|\t\t[bloom filter number of hash functions] "
                + bloomFilter.getHashFunctionSize());

        printlnBoth(
            pw,
            String.format("%20s", (reader.getFileMetadataPos() + reader.getFileMetadataSize()))
                + "|\t[TsFileMetadataSize] "
                + reader.getFileMetadataSize());

        printlnBoth(
            pw,
            String.format("%20s", reader.getFileMetadataPos() + reader.getFileMetadataSize() + 4)
                + "|\t[magic tail] "
                + reader.readTailMagic());

        printlnBoth(pw, String.format("%20s", length) + "|\tEND of TsFile");

        printlnBoth(pw, "");

        printlnBoth(
            pw,
            "---------------------------------- TsFile Sketch End ----------------------------------");
      }
    }
  }

  private static void printlnBoth(PrintWriter pw, String str) {
    System.out.println(str);
    pw.println(str);
  }

  private static Pair<String, String> checkArgs(String[] args) {
    String filename = "test.tsfile";
    String outFile = "TsFile_sketch_view.txt";
    if (args.length == 1) {
      filename = args[0];
    } else if (args.length == 2) {
      filename = args[0];
      outFile = args[1];
    }
    return new Pair<>(filename, outFile);
  }
}
