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

package org.apache.iotdb.tsfile.test1835;

import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.file.MetaMarker;
import org.apache.iotdb.tsfile.file.header.ChunkGroupHeader;
import org.apache.iotdb.tsfile.file.header.PageHeader;
import org.apache.iotdb.tsfile.file.metadata.ChunkGroupMetadata;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.IChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.MetadataIndexEntry;
import org.apache.iotdb.tsfile.file.metadata.MetadataIndexNode;
import org.apache.iotdb.tsfile.file.metadata.TimeseriesMetadata;
import org.apache.iotdb.tsfile.file.metadata.TsFileMetadata;
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
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class TsFileSketchToolV2 {

  private String filename;
  private String indexFileName;
  private PrintWriter pw;
  private TsFileSketchToolReader reader;
  private TsFileSketchToolReader indexReader;
  private String splitStr; // for split different part of TsFile

  public static void main(String[] args) throws IOException {
    String filename = "/Users/samperson1997/git/iotdb/data/data/sequence/root.sg/1/1/test0.tsfile";
    String outFile = "/Users/samperson1997/git/iotdb/data/data/sequence/root.sg/1/1/test0.txt";
    String indexFileName =
        "/Users/samperson1997/git/iotdb/data/data/sequence/root.sg/1/1/test0.tsfile.index";

    new TsFileSketchToolV2(filename, indexFileName, outFile).run();
  }

  /**
   * construct TsFileSketchTool
   *
   * @param filename input file path
   * @param indexFileName index file path
   * @param outFile output file path
   */
  public TsFileSketchToolV2(String filename, String indexFileName, String outFile) {
    try {
      this.filename = filename;
      this.indexFileName = indexFileName;
      pw = new PrintWriter(new FileWriter(outFile));
      reader = new TsFileSketchToolReader(filename);
      indexReader = new TsFileSketchToolReader(indexFileName);
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
    TsFileMetadata tsFileMetaData = reader.readFileMetadataV2();
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

    // print TsFile Metadata
    printTsFileMetadata(tsFileMetaData);

    printlnBoth(pw, String.format("%20s", length) + "|\tEND of TsFile");
    printlnBoth(
        pw,
        "---------------------------- IndexOfTimerseriesIndex Tree -----------------------------");
    // print index tree
    MetadataIndexNode metadataIndexNode = tsFileMetaData.getMetadataIndex();
    TreeMap<Long, MetadataIndexNode> metadataIndexNodeMap = new TreeMap<>();
    List<String> treeOutputStringBuffer = new ArrayList<>();
    loadIndexTree(metadataIndexNode, metadataIndexNodeMap, treeOutputStringBuffer, 0);

    // print IndexOfTimerseriesIndex
    printIndexOfTimerseriesIndex(metadataIndexNodeMap);

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
        for (IChunkMetadata chunkMetadata :
            reader.getChunkMetadataListV3(entry.getValue().left, false)) {
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
            indexReader.getMetadataIndexNode(metadataIndexEntry.getOffset(), endOffset);
        metadataIndexNodeMap.put(metadataIndexEntry.getOffset(), subNode);
        loadIndexTree(subNode, metadataIndexNodeMap, treeOutputStringBuffer, deep + 1);
      }
    }
  }

  private void printlnBoth(PrintWriter pw, String str) {
    System.out.println(str);
    pw.println(str);
  }

  private class TsFileSketchToolReader extends TsFileSequenceReader {
    public TsFileSketchToolReader(String file) throws IOException {
      super(file);
    }

    public Map<Long, Pair<Path, TimeseriesMetadata>> getAllTimeseriesMetadataWithOffset()
        throws IOException {
      Map<Long, Pair<Path, TimeseriesMetadata>> timeseriesMetadataMap = new TreeMap<>();

      // FIXME
      ByteBuffer buffer = readData(0, 0);
      while (buffer.hasRemaining()) {
        int bufferPos = buffer.position();
        TimeseriesMetadata timeseriesMetaData = TimeseriesMetadata.deserializeFrom(buffer, false);
        timeseriesMetadataMap.put(
            reader.position() + bufferPos,
            new Pair<>(new Path("d1", timeseriesMetaData.getMeasurementId()), timeseriesMetaData));
      }
      return timeseriesMetadataMap;
    }
  }
}
