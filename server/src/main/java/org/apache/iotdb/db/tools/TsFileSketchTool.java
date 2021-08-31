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
import org.apache.iotdb.tsfile.file.header.PageHeader;
import org.apache.iotdb.tsfile.file.metadata.*;
import org.apache.iotdb.tsfile.file.metadata.enums.MetadataIndexNodeType;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.Chunk;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.utils.BloomFilter;
import org.apache.iotdb.tsfile.utils.Pair;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class TsFileSketchTool {

  private String filename;
  private PrintWriter pw;
  private TsFileSketchToolReader reader;
  private String splitStr; // for split different part of TsFile

  public static void main(String[] args) throws IOException {
    Pair<String, String> fileNames = checkArgs(args);
    String filename = fileNames.left;
    String outFile = fileNames.right;
    System.out.println("TsFile path:" + filename);
    System.out.println("Sketch save path:" + outFile);
    new TsFileSketchTool(filename, outFile).run();
  }

  /**
   * construct TsFileSketchTool
   *
   * @param filename input file path
   * @param outFile output file path
   */
  public TsFileSketchTool(String filename, String outFile) {
    try {
      this.filename = filename;
      pw = new PrintWriter(new FileWriter(outFile));
      reader = new TsFileSketchToolReader(filename);
      StringBuilder str1 = new StringBuilder();
      for (int i = 0; i < 21; i++) {
        str1.append("|");
      }
      splitStr = str1.toString();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  /** entry of tool */
  public void run() throws IOException {
    long length = FSFactoryProducer.getFSFactory().getFile(filename).length();
    printlnBoth(
        pw, "-------------------------------- TsFile Sketch --------------------------------");
    printlnBoth(pw, "file path: " + filename);
    printlnBoth(pw, "file length: " + length);

    // get metadata information
    TsFileMetadata tsFileMetaData = reader.readFileMetadata();
    List<ChunkGroupMetadata> allChunkGroupMetadata = new ArrayList<>();
    reader.selfCheck(null, allChunkGroupMetadata, false);

    // print file information
    printFileInfo();

    // print chunk
    printChunk(allChunkGroupMetadata);

    // metadata begins
    if (tsFileMetaData.getMetadataIndex().getChildren().isEmpty()) {
      printlnBoth(pw, String.format("%20s", reader.getFileMetadataPos() - 1) + "|\t[marker] 2");
    } else {
      printlnBoth(
          pw, String.format("%20s", reader.readFileMetadata().getMetaOffset()) + "|\t[marker] 2");
    }
    // get all timeseries index
    Map<Long, Pair<Path, TimeseriesMetadata>> timeseriesMetadataMap =
        reader.getAllTimeseriesMetadataWithOffset();

    // print timeseries index
    printTimeseriesIndex(timeseriesMetadataMap);

    MetadataIndexNode metadataIndexNode = tsFileMetaData.getMetadataIndex();
    TreeMap<Long, MetadataIndexNode> metadataIndexNodeMap = new TreeMap<>();
    List<String> treeOutputStringBuffer = new ArrayList<>();
    loadIndexTree(metadataIndexNode, metadataIndexNodeMap, treeOutputStringBuffer, 0);

    // print IndexOfTimerseriesIndex
    printIndexOfTimerseriesIndex(metadataIndexNodeMap);

    // print TsFile Metadata
    printTsFileMetadata(tsFileMetaData);

    printlnBoth(pw, String.format("%20s", length) + "|\tEND of TsFile");
    printlnBoth(
        pw,
        "---------------------------- IndexOfTimerseriesIndex Tree -----------------------------");
    // print index tree
    for (String str : treeOutputStringBuffer) {
      printlnBoth(pw, str);
    }
    printlnBoth(
        pw,
        "---------------------------------- TsFile Sketch End ----------------------------------");
    pw.close();
  }

  private void printTsFileMetadata(TsFileMetadata tsFileMetaData) {
    try {
      printlnBoth(pw, String.format("%20s", reader.getFileMetadataPos()) + "|\t[TsFileMetadata]");
      printlnBoth(
          pw, String.format("%20s", "") + "|\t\t[meta offset] " + tsFileMetaData.getMetaOffset());
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
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private void printIndexOfTimerseriesIndex(TreeMap<Long, MetadataIndexNode> metadataIndexNodeMap) {
    for (Map.Entry<Long, MetadataIndexNode> entry : metadataIndexNodeMap.entrySet()) {
      printlnBoth(
          pw,
          String.format("%20s", entry.getKey())
              + "|\t[IndexOfTimerseriesIndex Node] type="
              + entry.getValue().getNodeType());
      for (MetadataIndexEntry metadataIndexEntry : entry.getValue().getChildren()) {
        printlnBoth(
            pw,
            String.format("%20s", "")
                + "|\t\t<"
                + metadataIndexEntry.getName()
                + ", "
                + metadataIndexEntry.getOffset()
                + ">");
      }
      printlnBoth(
          pw,
          String.format("%20s", "") + "|\t\t<endOffset, " + entry.getValue().getEndOffset() + ">");
    }
  }

  private void printFileInfo() {
    try {
      printlnBoth(pw, "");
      printlnBoth(pw, String.format("%20s", "POSITION") + "|\tCONTENT");
      printlnBoth(pw, String.format("%20s", "--------") + " \t-------");
      printlnBoth(pw, String.format("%20d", 0) + "|\t[magic head] " + reader.readHeadMagic());
      printlnBoth(
          pw,
          String.format("%20d", TSFileConfig.MAGIC_STRING.getBytes().length)
              + "|\t[version number] "
              + reader.readVersionNumber());
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private void printChunk(List<ChunkGroupMetadata> allChunkGroupMetadata) {
    try {
      long nextChunkGroupHeaderPos =
          (long) TSFileConfig.MAGIC_STRING.getBytes().length + Byte.BYTES;
      // ChunkGroup begins
      for (ChunkGroupMetadata chunkGroupMetadata : allChunkGroupMetadata) {
        printlnBoth(
            pw,
            splitStr
                + "\t[Chunk Group] of "
                + chunkGroupMetadata.getDevice()
                + ", num of Chunks:"
                + chunkGroupMetadata.getChunkMetadataList().size());
        // chunkGroupHeader begins
        printlnBoth(pw, String.format("%20s", nextChunkGroupHeaderPos) + "|\t[Chunk Group Header]");
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
              String.format("%20s", "")
                  + "|\t\t[chunk header] "
                  + "marker="
                  + chunk.getHeader().getChunkType()
                  + ", measurementId="
                  + chunk.getHeader().getMeasurementID()
                  + ", dataSize="
                  + chunk.getHeader().getDataSize()
                  + ", serializedSize="
                  + chunk.getHeader().getSerializedSize());

          printlnBoth(pw, String.format("%20s", "") + "|\t\t[chunk] " + chunk.getData());
          PageHeader pageHeader;
          if (((byte) (chunk.getHeader().getChunkType() & 0x3F))
              == MetaMarker.ONLY_ONE_PAGE_CHUNK_HEADER) {
            pageHeader = PageHeader.deserializeFrom(chunk.getData(), chunkMetadata.getStatistics());
          } else {
            pageHeader =
                PageHeader.deserializeFrom(chunk.getData(), chunk.getHeader().getDataType());
          }
          printlnBoth(
              pw,
              String.format("%20s", "")
                  + "|\t\t[page] "
                  + " CompressedSize:"
                  + pageHeader.getCompressedSize()
                  + ", UncompressedSize:"
                  + pageHeader.getUncompressedSize());
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

        printlnBoth(
            pw, splitStr + "\t[Chunk Group] of " + chunkGroupMetadata.getDevice() + " ends");
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private void printTimeseriesIndex(
      Map<Long, Pair<Path, TimeseriesMetadata>> timeseriesMetadataMap) {
    try {
      for (Map.Entry<Long, Pair<Path, TimeseriesMetadata>> entry :
          timeseriesMetadataMap.entrySet()) {
        printlnBoth(
            pw,
            String.format("%20s", entry.getKey())
                + "|\t[TimeseriesIndex] of "
                + entry.getValue().left
                + ", tsDataType:"
                + entry.getValue().right.getTSDataType());
        for (IChunkMetadata chunkMetadata : reader.getChunkMetadataList(entry.getValue().left)) {
          printlnBoth(
              pw,
              String.format("%20s", "")
                  + "|\t\t[ChunkIndex] "
                  + chunkMetadata.getMeasurementUid()
                  + ", offset="
                  + chunkMetadata.getOffsetOfChunkHeader());
        }
        printlnBoth(
            pw,
            String.format("%20s", "") + "|\t\t[" + entry.getValue().right.getStatistics() + "] ");
      }
      printlnBoth(pw, splitStr);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  /**
   * load by dfs, and sort by TreeMap
   *
   * @param metadataIndexNode current node
   * @param metadataIndexNodeMap result map, key is offset
   * @param treeOutputStringBuffer result list, string is index tree
   * @param deep current deep
   */
  private void loadIndexTree(
      MetadataIndexNode metadataIndexNode,
      TreeMap<Long, MetadataIndexNode> metadataIndexNodeMap,
      List<String> treeOutputStringBuffer,
      int deep)
      throws IOException {
    StringBuilder tableWriter = new StringBuilder("\t");
    for (int i = 0; i < deep; i++) {
      tableWriter.append("\t\t");
    }
    treeOutputStringBuffer.add(
        tableWriter.toString() + "[MetadataIndex:" + metadataIndexNode.getNodeType() + "]");
    for (int i = 0; i < metadataIndexNode.getChildren().size(); i++) {
      MetadataIndexEntry metadataIndexEntry = metadataIndexNode.getChildren().get(i);

      treeOutputStringBuffer.add(
          tableWriter.toString()
              + "└──────["
              + metadataIndexEntry.getName()
              + ","
              + metadataIndexEntry.getOffset()
              + "]");
      if (!metadataIndexNode.getNodeType().equals(MetadataIndexNodeType.LEAF_MEASUREMENT)) {
        long endOffset = metadataIndexNode.getEndOffset();
        if (i != metadataIndexNode.getChildren().size() - 1) {
          endOffset = metadataIndexNode.getChildren().get(i + 1).getOffset();
        }
        MetadataIndexNode subNode =
            reader.getMetadataIndexNode(metadataIndexEntry.getOffset(), endOffset);
        metadataIndexNodeMap.put(metadataIndexEntry.getOffset(), subNode);
        loadIndexTree(subNode, metadataIndexNodeMap, treeOutputStringBuffer, deep + 1);
      }
    }
  }

  private void printlnBoth(PrintWriter pw, String str) {
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

  private class TsFileSketchToolReader extends TsFileSequenceReader {
    public TsFileSketchToolReader(String file) throws IOException {
      super(file);
    }
    /**
     * Traverse the metadata index from MetadataIndexEntry to get TimeseriesMetadatas
     *
     * @param metadataIndex MetadataIndexEntry
     * @param buffer byte buffer
     * @param deviceId String
     * @param timeseriesMetadataMap map: deviceId -> timeseriesMetadata list
     * @param needChunkMetadata deserialize chunk metadata list or not
     */
    private void generateMetadataIndexWithOffset(
        long startOffset,
        MetadataIndexEntry metadataIndex,
        ByteBuffer buffer,
        String deviceId,
        MetadataIndexNodeType type,
        Map<Long, Pair<Path, TimeseriesMetadata>> timeseriesMetadataMap,
        boolean needChunkMetadata)
        throws IOException {
      try {
        if (type.equals(MetadataIndexNodeType.LEAF_MEASUREMENT)) {
          while (buffer.hasRemaining()) {
            long pos = startOffset + buffer.position();
            TimeseriesMetadata timeseriesMetadata =
                TimeseriesMetadata.deserializeFrom(buffer, needChunkMetadata);
            timeseriesMetadataMap.put(
                pos,
                new Pair<>(
                    new Path(deviceId, timeseriesMetadata.getMeasurementId()), timeseriesMetadata));
          }
        } else {
          // deviceId should be determined by LEAF_DEVICE node
          if (type.equals(MetadataIndexNodeType.LEAF_DEVICE)) {
            deviceId = metadataIndex.getName();
          }
          MetadataIndexNode metadataIndexNode = MetadataIndexNode.deserializeFrom(buffer);
          int metadataIndexListSize = metadataIndexNode.getChildren().size();
          for (int i = 0; i < metadataIndexListSize; i++) {
            long endOffset = metadataIndexNode.getEndOffset();
            if (i != metadataIndexListSize - 1) {
              endOffset = metadataIndexNode.getChildren().get(i + 1).getOffset();
            }
            ByteBuffer nextBuffer =
                readData(metadataIndexNode.getChildren().get(i).getOffset(), endOffset);
            generateMetadataIndexWithOffset(
                metadataIndexNode.getChildren().get(i).getOffset(),
                metadataIndexNode.getChildren().get(i),
                nextBuffer,
                deviceId,
                metadataIndexNode.getNodeType(),
                timeseriesMetadataMap,
                needChunkMetadata);
          }
        }
      } catch (BufferOverflowException e) {
        throw e;
      }
    }

    public Map<Long, Pair<Path, TimeseriesMetadata>> getAllTimeseriesMetadataWithOffset()
        throws IOException {
      if (tsFileMetaData == null) {
        readFileMetadata();
      }
      MetadataIndexNode metadataIndexNode = tsFileMetaData.getMetadataIndex();
      Map<Long, Pair<Path, TimeseriesMetadata>> timeseriesMetadataMap = new TreeMap<>();
      List<MetadataIndexEntry> metadataIndexEntryList = metadataIndexNode.getChildren();
      for (int i = 0; i < metadataIndexEntryList.size(); i++) {
        MetadataIndexEntry metadataIndexEntry = metadataIndexEntryList.get(i);
        long endOffset = tsFileMetaData.getMetadataIndex().getEndOffset();
        if (i != metadataIndexEntryList.size() - 1) {
          endOffset = metadataIndexEntryList.get(i + 1).getOffset();
        }
        ByteBuffer buffer = readData(metadataIndexEntry.getOffset(), endOffset);
        generateMetadataIndexWithOffset(
            metadataIndexEntry.getOffset(),
            metadataIndexEntry,
            buffer,
            null,
            metadataIndexNode.getNodeType(),
            timeseriesMetadataMap,
            false);
      }
      return timeseriesMetadataMap;
    }
  }
}
