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

package org.apache.tsfile.utils;

import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.file.IMetadataIndexEntry;
import org.apache.tsfile.file.MetaMarker;
import org.apache.tsfile.file.header.ChunkGroupHeader;
import org.apache.tsfile.file.header.PageHeader;
import org.apache.tsfile.file.metadata.ChunkGroupMetadata;
import org.apache.tsfile.file.metadata.ChunkMetadata;
import org.apache.tsfile.file.metadata.DeviceMetadataIndexEntry;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.MetadataIndexNode;
import org.apache.tsfile.file.metadata.TableSchema;
import org.apache.tsfile.file.metadata.TimeseriesMetadata;
import org.apache.tsfile.file.metadata.TsFileMetadata;
import org.apache.tsfile.file.metadata.enums.MetadataIndexNodeType;
import org.apache.tsfile.fileSystem.FSFactoryProducer;
import org.apache.tsfile.read.TsFileCheckStatus;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.read.common.Chunk;
import org.apache.tsfile.read.common.Path;
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
      printlnBoth(
          pw, String.format("%20s", reader.getFileMetadataPos() - 1) + "|\t" + "[marker] 2");
    } else {
      printlnBoth(
          pw,
          String.format("%20s", reader.readFileMetadata().getMetaOffset()) + "|\t" + "[marker] 2");
    }

    // get all TimerseriesIndex
    Map<Long, Pair<Path, TimeseriesMetadata>> timeseriesMetadataMap =
        reader.getAllTimeseriesMetadataWithOffset();

    // get all IndexOfTimerseriesIndex (excluding the root node in TsFileMetadata)
    TreeMap<Long, MetadataIndexNode> metadataIndexNodeMap = new TreeMap<>();
    List<String> treeOutputStringBuffer = new ArrayList<>();
    for (Entry<String, MetadataIndexNode> entry :
        tsFileMetaData.getTableMetadataIndexNodeMap().entrySet()) {
      treeOutputStringBuffer.add(entry.getKey());
      loadIndexTree(entry.getValue(), metadataIndexNodeMap, treeOutputStringBuffer, 0);
    }

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
      long pos = reader.getFileMetadataPos();
      printlnBoth(pw, splitStr + " [TsFileMetadata] begins");

      // metadataIndex
      printlnBoth(
          pw,
          String.format("%20s", pos)
              + "|\tTableIndexRootCnt="
              + tsFileMetaData.getTableMetadataIndexNodeMap().size());
      pos += Integer.BYTES;
      for (Entry<String, MetadataIndexNode> entry :
          tsFileMetaData.getTableMetadataIndexNodeMap().entrySet()) {
        printlnBoth(
            pw,
            String.format("%20s", pos)
                + "|\t[Table Name] "
                + entry.getKey()
                + ", size="
                + ReadWriteForEncodingUtils.intStringSize(entry.getKey()));
        pos += ReadWriteForEncodingUtils.intStringSize(entry.getKey());
        pos = printIndexOfTimerseriesIndex(pos, entry.getValue());
      }

      // table schema
      printlnBoth(
          pw,
          String.format("%20s", pos)
              + "|\tTableSchemaCnt="
              + tsFileMetaData.getTableSchemaMap().size());
      pos += Integer.BYTES;
      for (Entry<String, TableSchema> entry : tsFileMetaData.getTableSchemaMap().entrySet()) {
        final String tableName = entry.getKey();
        final TableSchema tableSchema = entry.getValue();

        final int serializedSize = tableSchema.serializedSize();
        printlnBoth(
            pw,
            String.format("%20s", pos)
                + "|\t[TableSchema] "
                + tableSchema
                + ", size="
                + serializedSize);
        pos += ReadWriteForEncodingUtils.intStringSize(tableName) + serializedSize;
      }

      // metaOffset
      printlnBoth(
          pw, String.format("%20s", pos) + "|\t[Meta Offset] " + tsFileMetaData.getMetaOffset());
      pos += Long.BYTES;

      // bloom filter

      BloomFilter bloomFilter = tsFileMetaData.getBloomFilter();
      if (bloomFilter != null) {
        final int length = bloomFilter.serialize().length;
        printlnBoth(
            pw,
            String.format("%20s", pos)
                + "|\t[Bloom Filter Size] "
                + "bit vector byte array length="
                + length
                + bloomFilter.getHashFunctionSize());
        pos += Integer.BYTES;
        printlnBoth(
            pw,
            String.format("%20s", pos)
                + "|\t[Bloom Filter] "
                + ", filterCapacity="
                + bloomFilter.getSize()
                + ", hashFunctionSize="
                + bloomFilter.getHashFunctionSize());
        pos += length;
      }

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

  private long printIndexOfTimerseriesIndex(long pos, MetadataIndexNode metadataIndexNode) {
    printlnBoth(pw, String.format("%20s", pos) + "|\t[MetadataIndexNode]");
    printlnBoth(
        pw,
        String.format("%20s", pos) + "|\t\t childrenCnt=" + metadataIndexNode.getChildren().size());
    pos += ReadWriteForEncodingUtils.uVarIntSize(metadataIndexNode.getChildren().size());
    for (IMetadataIndexEntry metadataIndexEntry : metadataIndexNode.getChildren()) {
      printlnBoth(
          pw,
          String.format("%20s", pos)
              + "|\t\t<"
              + metadataIndexEntry.getCompareKey()
              + ", "
              + metadataIndexEntry.getOffset()
              + ">");
      pos += metadataIndexEntry.serializedSize();
    }
    printlnBoth(
        pw, String.format("%20s", pos) + "|\t\tendOffset=" + metadataIndexNode.getEndOffset());
    pos += Long.BYTES;
    printlnBoth(
        pw, String.format("%20s", pos) + "|\t\tnodeType=" + metadataIndexNode.getNodeType());
    pos += Byte.BYTES;
    return pos;
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
                + " [ChunkGroup] of "
                + chunkGroupMetadata.getDevice()
                + ", num of Chunks:"
                + chunkGroupMetadata.getChunkMetadataList().size());
        // chunkGroupHeader begins
        long offset = nextChunkGroupHeaderPos;
        printlnBoth(pw, String.format("%20s", offset) + "|\t[ChunkGroup Header]");
        ChunkGroupHeader chunkGroupHeader =
            reader.readChunkGroupHeader(nextChunkGroupHeaderPos, false);
        printlnBoth(pw, String.format("%20s", offset) + "|\t\t[marker] 0");
        printlnBoth(
            pw,
            String.format("%20s", offset + 1)
                + "|\t\t[deviceID] "
                + chunkGroupHeader.getDeviceID()
                + " size="
                + chunkGroupHeader.getDeviceID().serializedSize());
        // chunk begins
        for (ChunkMetadata chunkMetadata : chunkGroupMetadata.getChunkMetadataList()) {
          offset = chunkMetadata.getOffsetOfChunkHeader();
          Chunk chunk = reader.readMemChunk(chunkMetadata);
          printlnBoth(
              pw,
              String.format("%20d", offset)
                  + "|\t[Chunk] of "
                  + new Path(
                      chunkGroupHeader.getDeviceID(), chunkMetadata.getMeasurementUid(), false)
                  + ", "
                  + chunkMetadata.getStatistics());
          printlnBoth(
              pw,
              String.format("%20s", offset)
                  + "|\t\t[Chunk Header] "
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
                  + chunk.getHeader().getEncodingType()
                  + ", size="
                  + chunk.getHeader().getSerializedSize());
          offset += chunk.getHeader().getSerializedSize();
          PageHeader pageHeader;
          if (((byte) (chunk.getHeader().getChunkType() & 0x3F))
              == MetaMarker.ONLY_ONE_PAGE_CHUNK_HEADER) {
            pageHeader = PageHeader.deserializeFrom(chunk.getData(), chunkMetadata.getStatistics());
            printlnBoth(
                pw,
                String.format("%20s", offset)
                    + "|\t\t\t[Page Header] "
                    + " HeaderSize:"
                    + (pageHeader.getSerializedSize()
                        - pageHeader.getStatistics().getSerializedSize())
                    + ", UncompressedSize:"
                    + pageHeader.getUncompressedSize()
                    + ", CompressedSize:"
                    + pageHeader.getCompressedSize());
            offset +=
                pageHeader.getSerializedSize() - pageHeader.getStatistics().getSerializedSize();
            printlnBoth(
                pw,
                String.format("%20s", offset)
                    + "|\t\t\t[Page Data] "
                    + " Size:"
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
                  String.format("%20s", offset)
                      + String.format("|\t\t\t[PageHeader-%s] ", pageID)
                      + " HeaderSize:"
                      + pageHeader.getSerializedSize()
                      + ", UncompressedSize:"
                      + pageHeader.getUncompressedSize()
                      + ", CompressedSize:"
                      + pageHeader.getCompressedSize()
                      + ", "
                      + pageHeader.getStatistics());
              offset += pageHeader.getSerializedSize();
              printlnBoth(
                  pw,
                  String.format("%20s", offset)
                      + String.format("|\t\t\t[Page-%s] ", pageID)
                      + ", CompressedSize:"
                      + pageHeader.getCompressedSize()
                      + ", "
                      + pageHeader.getStatistics());
              offset += pageHeader.getCompressedSize();
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

        printlnBoth(pw, splitStr + " [ChunkGroup] of " + chunkGroupMetadata.getDevice() + " ends");
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private void printTimeseriesIndex(long pos, Pair<Path, TimeseriesMetadata> timeseriesMetadata) {
    try {
      printlnBoth(
          pw,
          String.format("%20s", pos)
              + "|\t[TimeseriesMetadata] of "
              + timeseriesMetadata.left
              + ", tsDataType:"
              + timeseriesMetadata.right.getTsDataType()
              + ", sizeWithoutChunkMetadata:"
              + timeseriesMetadata.right.serializedSizeWithoutMetadata()
              + ", "
              + timeseriesMetadata.right.getStatistics());
      pos += timeseriesMetadata.getRight().serializedSizeWithoutMetadata();
      List<ChunkMetadata> chunkMetadataList = reader.getChunkMetadataList(timeseriesMetadata.left);
      for (int i = 0; i < chunkMetadataList.size(); i++) {
        ChunkMetadata chunkMetadata = chunkMetadataList.get(i);
        final int serializedSize = chunkMetadata.serializedSize(i != 0);
        printlnBoth(
            pw,
            String.format("%20s", pos)
                + "|\t\t[ChunkMetadata] "
                + "offset="
                + chunkMetadata.getOffsetOfChunkHeader()
                + ", size="
                + serializedSize);
        pos += serializedSize;
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
        tableWriter + "[MetadataIndex:" + metadataIndexNode.getNodeType() + "]");
    for (int i = 0; i < metadataIndexNode.getChildren().size(); i++) {
      IMetadataIndexEntry metadataIndexEntry = metadataIndexNode.getChildren().get(i);

      treeOutputStringBuffer.add(
          tableWriter
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
              reader
                  .getDeserializeContext()
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
              reader
                  .getDeserializeContext()
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
      for (Entry<String, MetadataIndexNode> entry :
          tsFileMetaData.getTableMetadataIndexNodeMap().entrySet()) {
        List<IMetadataIndexEntry> metadataIndexEntryList = entry.getValue().getChildren();
        for (int i = 0; i < metadataIndexEntryList.size(); i++) {
          IMetadataIndexEntry metadataIndexEntry = metadataIndexEntryList.get(i);
          long endOffset = entry.getValue().getEndOffset();
          if (i != metadataIndexEntryList.size() - 1) {
            endOffset = metadataIndexEntryList.get(i + 1).getOffset();
          }
          ByteBuffer buffer = readData(metadataIndexEntry.getOffset(), endOffset);
          generateMetadataIndexWithOffset(
              metadataIndexEntry.getOffset(),
              metadataIndexEntry,
              buffer,
              null,
              entry.getValue().getNodeType(),
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
