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

import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.file.IMetadataIndexEntry;
import org.apache.tsfile.file.MetaMarker;
import org.apache.tsfile.file.header.ChunkGroupHeader;
import org.apache.tsfile.file.header.PageHeader;
import org.apache.tsfile.file.metadata.ChunkGroupMetadata;
import org.apache.tsfile.file.metadata.ChunkMetadata;
import org.apache.tsfile.file.metadata.DeviceMetadataIndexEntry;
import org.apache.tsfile.file.metadata.IChunkMetadata;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.MetadataIndexNode;
import org.apache.tsfile.file.metadata.TimeseriesMetadata;
import org.apache.tsfile.file.metadata.TsFileMetadata;
import org.apache.tsfile.file.metadata.enums.MetadataIndexNodeType;
import org.apache.tsfile.fileSystem.FSFactoryProducer;
import org.apache.tsfile.read.TsFileCheckStatus;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.read.common.Chunk;
import org.apache.tsfile.read.common.Path;
import org.apache.tsfile.utils.BloomFilter;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.write.schema.Schema;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

public class TsFileSketchTool {

  private String filename;
  private PrintWriter pw;
  private TsFileSketchToolReader reader;
  private String splitStr; // for split different part of TsFile
  TsFileMetadata tsFileMetaData;
  List<ChunkGroupMetadata> allChunkGroupMetadata;

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
      // get metadata information
      tsFileMetaData = reader.readFileMetadata();
      allChunkGroupMetadata = new ArrayList<>();
      if (reader.selfCheck(new Schema(), allChunkGroupMetadata, false)
          != TsFileCheckStatus.COMPLETE_FILE) {
        throw new IOException(
            String.format("Cannot load file %s because the file has crashed.", filename));
      }
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

    // print file information
    printFileInfo();

    // print chunk
    printChunk(allChunkGroupMetadata);

    // metadata begins
    if (tsFileMetaData.getTableMetadataIndexNodeMap().isEmpty()) {
      printlnBoth(pw, String.format("%20s", reader.getFileMetadataPos() - 1) + "|\t[marker] 2");
    } else {
      printlnBoth(
          pw, String.format("%20s", reader.readFileMetadata().getMetaOffset()) + "|\t[marker] 2");
    }

    // get all TimerseriesIndex
    Map<Long, Pair<Path, TimeseriesMetadata>> timeseriesMetadataMap =
        reader.getAllTimeseriesMetadataWithOffset();

    // get all IndexOfTimerseriesIndex (excluding the root node in TsFileMetadata
    TreeMap<Long, MetadataIndexNode> metadataIndexNodeMap = new TreeMap<>();
    List<String> treeOutputStringBuffer = new ArrayList<>();
    for (MetadataIndexNode metadataIndexNode :
        tsFileMetaData.getTableMetadataIndexNodeMap().values()) {
      loadIndexTree(metadataIndexNode, metadataIndexNodeMap, treeOutputStringBuffer, 0);

      // iterate timeseriesMetadataMap and metadataIndexNodeMap to print info in increasing order of
      // position
      Iterator<Entry<Long, Pair<Path, TimeseriesMetadata>>> ite1 =
          timeseriesMetadataMap.entrySet().iterator();
      Iterator<Entry<Long, MetadataIndexNode>> ite2 = metadataIndexNodeMap.entrySet().iterator();
      Entry<Long, Pair<Path, TimeseriesMetadata>> value1 = (ite1.hasNext() ? ite1.next() : null);
      Entry<Long, MetadataIndexNode> value2 = (ite2.hasNext() ? ite2.next() : null);
      while (value1 != null || value2 != null) {
        if (value2 == null || (value1 != null && value1.getKey().compareTo(value2.getKey()) <= 0)) {
          printTimeseriesIndex(value1.getKey(), value1.getValue());
          value1 = (ite1.hasNext() ? ite1.next() : null);
        } else {
          printIndexOfTimerseriesIndex(value2.getKey(), value2.getValue());
          value2 = (ite2.hasNext() ? ite2.next() : null);
        }
      }
      metadataIndexNodeMap.clear();
    }
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
    reader.close();
    pw.close();
  }

  public void close() throws IOException {
    reader.close();
    pw.close();
  }

  private void printTsFileMetadata(TsFileMetadata tsFileMetaData) {
    try {
      printlnBoth(pw, splitStr + " [TsFileMetadata] begins");

      // metadataIndex
      for (MetadataIndexNode rootNode : tsFileMetaData.getTableMetadataIndexNodeMap().values()) {
        printIndexOfTimerseriesIndex(reader.getFileMetadataPos(), rootNode);
      }

      // metaOffset
      printlnBoth(
          pw, String.format("%20s", "") + "|\t[meta offset] " + tsFileMetaData.getMetaOffset());

      // bloom filter
      BloomFilter bloomFilter = tsFileMetaData.getBloomFilter();
      printlnBoth(
          pw,
          String.format("%20s", "")
              + "|\t[bloom filter] "
              + "bit vector byte array length="
              + bloomFilter.serialize().length
              + ", filterSize="
              + bloomFilter.getSize()
              + ", hashFunctionSize="
              + bloomFilter.getHashFunctionSize());
      printlnBoth(pw, splitStr + " [TsFileMetadata] ends");

      printlnBoth(
          pw,
          String.format("%20s", (reader.getFileMetadataPos() + reader.getTsFileMetadataSize()))
              + "|\t[TsFileMetadataSize] "
              + reader.getTsFileMetadataSize());

      printlnBoth(
          pw,
          String.format("%20s", reader.getFileMetadataPos() + reader.getTsFileMetadataSize() + 4)
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
      for (IMetadataIndexEntry metadataIndexEntry : entry.getValue().getChildren()) {
        printlnBoth(
            pw,
            String.format("%20s", "")
                + "|\t\t<"
                + metadataIndexEntry.getCompareKey()
                + ", "
                + metadataIndexEntry.getOffset()
                + ">");
      }
      printlnBoth(
          pw,
          String.format("%20s", "") + "|\t\t<endOffset, " + entry.getValue().getEndOffset() + ">");
    }
  }

  private void printIndexOfTimerseriesIndex(long pos, MetadataIndexNode metadataIndexNode) {
    printlnBoth(
        pw,
        String.format("%20s", pos)
            + "|\t[IndexOfTimerseriesIndex Node] type="
            + metadataIndexNode.getNodeType());
    for (IMetadataIndexEntry metadataIndexEntry : metadataIndexNode.getChildren()) {
      printlnBoth(
          pw,
          String.format("%20s", "")
              + "|\t\t<"
              + metadataIndexEntry.getCompareKey()
              + ", "
              + metadataIndexEntry.getOffset()
              + ">");
    }
    printlnBoth(
        pw,
        String.format("%20s", "") + "|\t\t<endOffset, " + metadataIndexNode.getEndOffset() + ">");
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
      reader.position(0);
      long nextChunkGroupHeaderPos =
          (long) TSFileConfig.MAGIC_STRING.getBytes().length + Byte.BYTES;
      // ChunkGroup begins
      for (ChunkGroupMetadata chunkGroupMetadata : allChunkGroupMetadata) {
        printlnBoth(
            pw,
            splitStr
                + " [Chunk Group] of "
                + chunkGroupMetadata.getDevice()
                + ", num of Chunks:"
                + chunkGroupMetadata.getChunkMetadataList().size());
        // chunkGroupHeader begins
        printlnBoth(pw, String.format("%20s", nextChunkGroupHeaderPos) + "|\t[Chunk Group Header]");
        ChunkGroupHeader chunkGroupHeader =
            reader.readChunkGroupHeader(nextChunkGroupHeaderPos, false);
        printlnBoth(pw, String.format("%20s", "") + "|\t\t[marker] 0");
        printlnBoth(
            pw,
            String.format("%20s", "")
                + "|\t\t[deviceID] "
                + chunkGroupHeader.getDeviceID().toString());
        // chunk begins
        for (ChunkMetadata chunkMetadata : chunkGroupMetadata.getChunkMetadataList()) {
          Chunk chunk = reader.readMemChunk(chunkMetadata);
          printlnBoth(
              pw,
              String.format("%20d", chunkMetadata.getOffsetOfChunkHeader())
                  + "|\t[Chunk] of "
                  + new Path(
                      chunkGroupHeader.getDeviceID(), chunkMetadata.getMeasurementUid(), false)
                  + ", "
                  + chunkMetadata.getStatistics());
          printlnBoth(
              pw,
              String.format("%20s", "")
                  + "|\t\t[chunk header] "
                  + "marker="
                  + chunk.getHeader().getChunkType()
                  + ", measurementID="
                  + chunk.getHeader().getMeasurementID()
                  + ", dataSize="
                  + chunk.getHeader().getDataSize()
                  + ", dataType="
                  + chunk.getHeader().getDataType()
                  + ", compressionType="
                  + chunk.getHeader().getCompressionType()
                  + ", encodingType="
                  + chunk.getHeader().getEncodingType());
          PageHeader pageHeader;
          if (((byte) (chunk.getHeader().getChunkType() & 0x3F))
              == MetaMarker.ONLY_ONE_PAGE_CHUNK_HEADER) {
            pageHeader = PageHeader.deserializeFrom(chunk.getData(), chunkMetadata.getStatistics());
            printlnBoth(
                pw,
                String.format("%20s", "")
                    + "|\t\t[page] "
                    + " UncompressedSize:"
                    + pageHeader.getUncompressedSize()
                    + ", CompressedSize:"
                    + pageHeader.getCompressedSize());
          } else { // more than one page in this chunk
            ByteBuffer chunkDataBuffer = chunk.getData();
            int pageID = 0;
            while (chunkDataBuffer.remaining() > 0) {
              pageID++;
              // deserialize a PageHeader from chunkDataBuffer
              pageHeader =
                  PageHeader.deserializeFrom(chunkDataBuffer, chunk.getHeader().getDataType());
              // skip the compressed bytes
              chunkDataBuffer.position(chunkDataBuffer.position() + pageHeader.getCompressedSize());
              // print page info
              printlnBoth(
                  pw,
                  String.format("%20s", "")
                      + String.format("|\t\t[page-%s] ", pageID)
                      + " UncompressedSize:"
                      + pageHeader.getUncompressedSize()
                      + ", CompressedSize:"
                      + pageHeader.getCompressedSize()
                      + ", "
                      + pageHeader.getStatistics());
            }
          }
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

        printlnBoth(pw, splitStr + " [Chunk Group] of " + chunkGroupMetadata.getDevice() + " ends");
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
                + entry.getValue().right.getTsDataType()
                + ", "
                + entry.getValue().right.getStatistics());
        for (IChunkMetadata chunkMetadata : reader.getChunkMetadataList(entry.getValue().left)) {
          printlnBoth(
              pw,
              String.format("%20s", "")
                  + "|\t\t[ChunkIndex] "
                  + chunkMetadata.getMeasurementUid()
                  + ", offset="
                  + chunkMetadata.getOffsetOfChunkHeader());
        }
      }
      printlnBoth(pw, splitStr);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private void printTimeseriesIndex(long pos, Pair<Path, TimeseriesMetadata> timeseriesMetadata) {
    try {
      printlnBoth(
          pw,
          String.format("%20s", pos)
              + "|\t[TimeseriesIndex] of "
              + timeseriesMetadata.left
              + ", tsDataType:"
              + timeseriesMetadata.right.getTsDataType()
              + ", "
              + timeseriesMetadata.right.getStatistics());
      for (IChunkMetadata chunkMetadata : reader.getChunkMetadataList(timeseriesMetadata.left)) {
        printlnBoth(
            pw,
            String.format("%20s", "")
                + "|\t\t[ChunkIndex] "
                + "offset="
                + chunkMetadata.getOffsetOfChunkHeader());
      }
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
      IMetadataIndexEntry metadataIndexEntry = metadataIndexNode.getChildren().get(i);

      treeOutputStringBuffer.add(
          tableWriter.toString()
              + "└──────["
              + metadataIndexEntry.getCompareKey()
              + ","
              + metadataIndexEntry.getOffset()
              + "]");
      if (!metadataIndexNode.getNodeType().equals(MetadataIndexNodeType.LEAF_MEASUREMENT)) {
        long endOffset = metadataIndexNode.getEndOffset();
        if (i != metadataIndexNode.getChildren().size() - 1) {
          endOffset = metadataIndexNode.getChildren().get(i + 1).getOffset();
        }
        boolean currentChildLevelIsDevice =
            MetadataIndexNodeType.INTERNAL_DEVICE.equals(metadataIndexNode.getNodeType());
        MetadataIndexNode subNode =
            reader.readMetadataIndexNode(
                metadataIndexEntry.getOffset(), endOffset, currentChildLevelIsDevice);
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
        IMetadataIndexEntry metadataIndex,
        ByteBuffer buffer,
        IDeviceID deviceId,
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
                    new Path(deviceId, timeseriesMetadata.getMeasurementId(), true),
                    timeseriesMetadata));
          }
        } else {
          // deviceId should be determined by LEAF_DEVICE node
          if (type.equals(MetadataIndexNodeType.LEAF_DEVICE)) {
            deviceId = ((DeviceMetadataIndexEntry) metadataIndex).getDeviceID();
          }
          boolean currentChildLevelIsDevice = MetadataIndexNodeType.INTERNAL_DEVICE.equals(type);
          MetadataIndexNode metadataIndexNode =
              getDeserializeContext()
                  .deserializeMetadataIndexNode(buffer, currentChildLevelIsDevice);
          int metadataIndexListSize = metadataIndexNode.getChildren().size();
          for (int i = 0; i < metadataIndexListSize; i++) {
            long endOffset = metadataIndexNode.getEndOffset();
            if (i != metadataIndexListSize - 1) {
              endOffset = metadataIndexNode.getChildren().get(i + 1).getOffset();
            }
            if (endOffset - metadataIndexNode.getChildren().get(i).getOffset()
                < Integer.MAX_VALUE) {
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
            } else {
              // when the buffer length is over than Integer.MAX_VALUE,
              // using tsFileInput to get timeseriesMetadataList
              generateMetadataIndexWithOffsetUsingTsFileInput(
                  metadataIndexNode.getChildren().get(i).getOffset(),
                  endOffset,
                  metadataIndexNode.getChildren().get(i),
                  deviceId,
                  metadataIndexNode.getNodeType(),
                  timeseriesMetadataMap,
                  needChunkMetadata);
            }
          }
        }
      } catch (BufferOverflowException e) {
        throw e;
      }
    }

    /**
     * Traverse the metadata index from MetadataIndexEntry to get TimeseriesMetadatas
     *
     * @param metadataIndex MetadataIndexEntry
     * @param deviceId String
     * @param timeseriesMetadataMap map: deviceId -> timeseriesMetadata list
     * @param needChunkMetadata deserialize chunk metadata list or not
     */
    private void generateMetadataIndexWithOffsetUsingTsFileInput(
        long start,
        long end,
        IMetadataIndexEntry metadataIndex,
        IDeviceID deviceId,
        MetadataIndexNodeType type,
        Map<Long, Pair<Path, TimeseriesMetadata>> timeseriesMetadataMap,
        boolean needChunkMetadata)
        throws IOException {
      try {
        tsFileInput.position(start);
        if (type.equals(MetadataIndexNodeType.LEAF_MEASUREMENT)) {
          while (tsFileInput.position() < end) {
            long pos = tsFileInput.position();
            TimeseriesMetadata timeseriesMetadata =
                TimeseriesMetadata.deserializeFrom(tsFileInput, needChunkMetadata);
            timeseriesMetadataMap.put(
                pos,
                new Pair<>(
                    new Path(deviceId, timeseriesMetadata.getMeasurementId(), true),
                    timeseriesMetadata));
          }
        } else {
          // deviceId should be determined by LEAF_DEVICE node
          if (type.equals(MetadataIndexNodeType.LEAF_DEVICE)) {
            deviceId = ((DeviceMetadataIndexEntry) metadataIndex).getDeviceID();
          }
          boolean isDeviceLevel = MetadataIndexNodeType.INTERNAL_DEVICE.equals(type);
          MetadataIndexNode metadataIndexNode =
              getDeserializeContext()
                  .deserializeMetadataIndexNode(tsFileInput.wrapAsInputStream(), isDeviceLevel);
          int metadataIndexListSize = metadataIndexNode.getChildren().size();
          for (int i = 0; i < metadataIndexListSize; i++) {
            long endOffset = metadataIndexNode.getEndOffset();
            if (i != metadataIndexListSize - 1) {
              endOffset = metadataIndexNode.getChildren().get(i + 1).getOffset();
            }
            generateMetadataIndexWithOffsetUsingTsFileInput(
                metadataIndexNode.getChildren().get(i).getOffset(),
                endOffset,
                metadataIndexNode.getChildren().get(i),
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

      Map<Long, Pair<Path, TimeseriesMetadata>> timeseriesMetadataMap = new TreeMap<>();
      for (MetadataIndexNode metadataIndexNode :
          tsFileMetaData.getTableMetadataIndexNodeMap().values()) {
        List<IMetadataIndexEntry> metadataIndexEntryList = metadataIndexNode.getChildren();
        for (int i = 0; i < metadataIndexEntryList.size(); i++) {
          IMetadataIndexEntry metadataIndexEntry = metadataIndexEntryList.get(i);
          long endOffset = metadataIndexNode.getEndOffset();
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
      }

      return timeseriesMetadataMap;
    }
  }

  // for test
  protected List<ChunkGroupMetadata> getAllChunkGroupMetadata() {
    return allChunkGroupMetadata;
  }
}
