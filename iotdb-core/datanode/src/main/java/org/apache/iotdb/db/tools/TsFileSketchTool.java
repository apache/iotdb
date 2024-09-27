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

import java.io.File;
import java.io.Serializable;
import java.util.HashMap;
import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.common.constant.TsFileConstant;
import org.apache.tsfile.encoding.decoder.Decoder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.IMetadataIndexEntry;
import org.apache.tsfile.file.MetaMarker;
import org.apache.tsfile.file.header.ChunkGroupHeader;
import org.apache.tsfile.file.header.ChunkHeader;
import org.apache.tsfile.file.header.PageHeader;
import org.apache.tsfile.file.metadata.ChunkGroupMetadata;
import org.apache.tsfile.file.metadata.ChunkMetadata;
import org.apache.tsfile.file.metadata.DeviceMetadataIndexEntry;
import org.apache.tsfile.file.metadata.IChunkMetadata;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.MetadataIndexNode;
import org.apache.tsfile.file.metadata.PlainDeviceID;
import org.apache.tsfile.file.metadata.TimeseriesMetadata;
import org.apache.tsfile.file.metadata.TsFileMetadata;
import org.apache.tsfile.file.metadata.enums.MetadataIndexNodeType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.file.metadata.statistics.Statistics;
import org.apache.tsfile.fileSystem.FSFactoryProducer;
import org.apache.tsfile.read.TsFileCheckStatus;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.read.common.BatchData;
import org.apache.tsfile.read.common.Chunk;
import org.apache.tsfile.read.common.Path;
import org.apache.tsfile.read.reader.page.PageReader;
import org.apache.tsfile.read.reader.page.TimePageReader;
import org.apache.tsfile.read.reader.page.ValuePageReader;
import org.apache.tsfile.utils.BloomFilter;
import org.apache.tsfile.utils.Pair;

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
import org.apache.tsfile.utils.TsPrimitiveType;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TsFileSketchTool {

  private static final Logger LOGGER = LoggerFactory.getLogger(TsFileSketchTool.class);
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
      reader = new TsFileSketchToolReader(filename) {
        @Override
        public long selfCheck(Map<Path, IMeasurementSchema> newSchema,
            List<ChunkGroupMetadata> chunkGroupMetadataList, boolean fastFinish)
            throws IOException {
          File checkFile = FSFactoryProducer.getFSFactory().getFile(this.file);
          long fileSize;
          if (!checkFile.exists()) {
            return TsFileCheckStatus.FILE_NOT_FOUND;
          } else {
            fileSize = checkFile.length();
          }
          ChunkMetadata currentChunk;
          String measurementID;
          TSDataType dataType;
          long fileOffsetOfChunk;

          // ChunkMetadata of current ChunkGroup
          List<ChunkMetadata> chunkMetadataList = new ArrayList<>();

          int headerLength = TSFileConfig.MAGIC_STRING.getBytes().length + Byte.BYTES;
          if (fileSize < headerLength) {
            return TsFileCheckStatus.INCOMPATIBLE_FILE;
          }
          if (!TSFileConfig.MAGIC_STRING.equals(readHeadMagic())
              || (TSFileConfig.VERSION_NUMBER != readVersionNumber())) {
            return TsFileCheckStatus.INCOMPATIBLE_FILE;
          }

          tsFileInput.position(headerLength);
          boolean isComplete = isComplete();
          if (fileSize == headerLength) {
            return headerLength;
          } else if (isComplete) {
            loadMetadataSize();
            if (fastFinish) {
              return TsFileCheckStatus.COMPLETE_FILE;
            }
          }
          // if not a complete file, we will recover it...
          long truncatedSize = headerLength;
          byte marker;
          List<long[]> timeBatch = new ArrayList<>();
          IDeviceID lastDeviceId = null;
          List<IMeasurementSchema> measurementSchemaList = new ArrayList<>();
          Map<String, Integer> valueColumn2TimeBatchIndex = new HashMap<>();
          try {
            while ((marker = this.readMarker()) != MetaMarker.SEPARATOR) {
              switch (marker) {
                case MetaMarker.CHUNK_HEADER:
                case MetaMarker.TIME_CHUNK_HEADER:
                case MetaMarker.VALUE_CHUNK_HEADER:
                case MetaMarker.ONLY_ONE_PAGE_CHUNK_HEADER:
                case MetaMarker.ONLY_ONE_PAGE_TIME_CHUNK_HEADER:
                case MetaMarker.ONLY_ONE_PAGE_VALUE_CHUNK_HEADER:
                  fileOffsetOfChunk = this.position() - 1;
                  if (fileOffsetOfChunk == 350064120) {
                    tsFileInput.position(350249561);
                    break;
                  }
                  // if there is something wrong with a chunk, we will drop the whole ChunkGroup
                  // as different chunks may be created by the same insertions(sqls), and partial
                  // insertion is not tolerable
                  ChunkHeader chunkHeader = this.readChunkHeader(marker);
                  measurementID = chunkHeader.getMeasurementID();
                  IMeasurementSchema measurementSchema =
                      new MeasurementSchema(
                          measurementID,
                          chunkHeader.getDataType(),
                          chunkHeader.getEncodingType(),
                          chunkHeader.getCompressionType());
                  measurementSchemaList.add(measurementSchema);
                  dataType = chunkHeader.getDataType();

                  Statistics<? extends Serializable> chunkStatistics =
                      Statistics.getStatsByType(dataType);
                  int dataSize = chunkHeader.getDataSize();

                  if (dataSize > 0) {
                    if (marker == MetaMarker.TIME_CHUNK_HEADER) {
                      timeBatch.add(null);
                    }
                    if (((byte) (chunkHeader.getChunkType() & 0x3F))
                        == MetaMarker
                        .CHUNK_HEADER) { // more than one page, we could use page statistics to
                      if (marker == MetaMarker.VALUE_CHUNK_HEADER) {
                        int timeBatchIndex =
                            valueColumn2TimeBatchIndex.getOrDefault(chunkHeader.getMeasurementID(), 0);
                        valueColumn2TimeBatchIndex.put(
                            chunkHeader.getMeasurementID(), timeBatchIndex + 1);
                      }
                      // generate chunk statistic
                      while (dataSize > 0) {
                        // a new Page
                        PageHeader pageHeader = this.readPageHeader(chunkHeader.getDataType(), true);
                        if (pageHeader.getUncompressedSize() != 0) {
                          // not empty page
                          chunkStatistics.mergeStatistics(pageHeader.getStatistics());
                        }
                        this.skipPageData(pageHeader);
                        dataSize -= pageHeader.getSerializedPageSize();
                        chunkHeader.increasePageNums(1);
                      }
                    } else { // only one page without statistic, we need to iterate each point to generate
                      // chunk statistic
                      PageHeader pageHeader = this.readPageHeader(chunkHeader.getDataType(), false);
                      Decoder valueDecoder =
                          Decoder.getDecoderByType(
                              chunkHeader.getEncodingType(), chunkHeader.getDataType());
                      ByteBuffer pageData = readPage(pageHeader, chunkHeader.getCompressionType());
                      Decoder timeDecoder =
                          Decoder.getDecoderByType(
                              TSEncoding.valueOf(
                                  TSFileDescriptor.getInstance().getConfig().getTimeEncoder()),
                              TSDataType.INT64);

                      if ((chunkHeader.getChunkType() & TsFileConstant.TIME_COLUMN_MASK)
                          == TsFileConstant.TIME_COLUMN_MASK) { // Time Chunk with only one page

                        TimePageReader timePageReader =
                            new TimePageReader(pageHeader, pageData, timeDecoder);
                        long[] currentTimeBatch = timePageReader.getNextTimeBatch();
                        timeBatch.add(currentTimeBatch);
                        for (long currentTime : currentTimeBatch) {
                          chunkStatistics.update(currentTime);
                        }
                      } else if ((chunkHeader.getChunkType() & TsFileConstant.VALUE_COLUMN_MASK)
                          == TsFileConstant.VALUE_COLUMN_MASK) { // Value Chunk with only one page

                        ValuePageReader valuePageReader =
                            new ValuePageReader(
                                pageHeader, pageData, chunkHeader.getDataType(), valueDecoder);
                        int timeBatchIndex =
                            valueColumn2TimeBatchIndex.getOrDefault(chunkHeader.getMeasurementID(), 0);
                        valueColumn2TimeBatchIndex.put(
                            chunkHeader.getMeasurementID(), timeBatchIndex + 1);
                        TsPrimitiveType[] valueBatch =
                            valuePageReader.nextValueBatch(timeBatch.get(timeBatchIndex));

                        if (valueBatch != null && valueBatch.length != 0) {
                          for (int i = 0; i < valueBatch.length; i++) {
                            TsPrimitiveType value = valueBatch[i];
                            if (value == null) {
                              continue;
                            }
                            long timeStamp = timeBatch.get(timeBatchIndex)[i];
                            switch (dataType) {
                              case INT32:
                              case DATE:
                                chunkStatistics.update(timeStamp, value.getInt());
                                break;
                              case INT64:
                              case TIMESTAMP:
                                chunkStatistics.update(timeStamp, value.getLong());
                                break;
                              case FLOAT:
                                chunkStatistics.update(timeStamp, value.getFloat());
                                break;
                              case DOUBLE:
                                chunkStatistics.update(timeStamp, value.getDouble());
                                break;
                              case BOOLEAN:
                                chunkStatistics.update(timeStamp, value.getBoolean());
                                break;
                              case TEXT:
                              case BLOB:
                              case STRING:
                                chunkStatistics.update(timeStamp, value.getBinary());
                                break;
                              default:
                                throw new IOException("Unexpected type " + dataType);
                            }
                          }
                        }

                      } else { // NonAligned Chunk with only one page
                        PageReader reader =
                            new PageReader(
                                pageHeader,
                                pageData,
                                chunkHeader.getDataType(),
                                valueDecoder,
                                timeDecoder);
                        BatchData batchData = reader.getAllSatisfiedPageData();
                        while (batchData.hasCurrent()) {
                          switch (dataType) {
                            case INT32:
                            case DATE:
                              chunkStatistics.update(batchData.currentTime(), batchData.getInt());
                              break;
                            case INT64:
                            case TIMESTAMP:
                              chunkStatistics.update(batchData.currentTime(), batchData.getLong());
                              break;
                            case FLOAT:
                              chunkStatistics.update(batchData.currentTime(), batchData.getFloat());
                              break;
                            case DOUBLE:
                              chunkStatistics.update(batchData.currentTime(), batchData.getDouble());
                              break;
                            case BOOLEAN:
                              chunkStatistics.update(batchData.currentTime(), batchData.getBoolean());
                              break;
                            case TEXT:
                            case BLOB:
                            case STRING:
                              chunkStatistics.update(batchData.currentTime(), batchData.getBinary());
                              break;
                            default:
                              throw new IOException("Unexpected type " + dataType);
                          }
                          batchData.next();
                        }
                      }
                      chunkHeader.increasePageNums(1);
                    }
                  } else if (marker == MetaMarker.ONLY_ONE_PAGE_VALUE_CHUNK_HEADER
                      || marker == MetaMarker.VALUE_CHUNK_HEADER) {
                    int timeBatchIndex =
                        valueColumn2TimeBatchIndex.getOrDefault(chunkHeader.getMeasurementID(), 0);
                    valueColumn2TimeBatchIndex.put(chunkHeader.getMeasurementID(), timeBatchIndex + 1);
                  }
                  currentChunk =
                      new ChunkMetadata(measurementID, dataType, fileOffsetOfChunk, chunkStatistics);
                  chunkMetadataList.add(currentChunk);
                  break;
                case MetaMarker.CHUNK_GROUP_HEADER:
                  // if there is something wrong with the ChunkGroup Header, we will drop this ChunkGroup
                  // because we can not guarantee the correctness of the deviceId.
                  truncatedSize = this.position() - 1;
                  if (lastDeviceId != null) {
                    // schema of last chunk group
                    if (newSchema != null) {
                      for (IMeasurementSchema tsSchema : measurementSchemaList) {
                        newSchema.putIfAbsent(
                            new Path(lastDeviceId, tsSchema.getMeasurementId(), true), tsSchema);
                      }
                    }
                    measurementSchemaList = new ArrayList<>();
                    // last chunk group Metadata
                    chunkGroupMetadataList.add(new ChunkGroupMetadata(lastDeviceId, chunkMetadataList));
                  }
                  // this is a chunk group
                  chunkMetadataList = new ArrayList<>();
                  ChunkGroupHeader chunkGroupHeader = this.readChunkGroupHeader();
                  lastDeviceId = chunkGroupHeader.getDeviceID();
                  timeBatch.clear();
                  valueColumn2TimeBatchIndex.clear();
                  break;
                case MetaMarker.OPERATION_INDEX_RANGE:
                  truncatedSize = this.position() - 1;
                  if (lastDeviceId != null) {
                    // schema of last chunk group
                    if (newSchema != null) {
                      for (IMeasurementSchema tsSchema : measurementSchemaList) {
                        newSchema.putIfAbsent(
                            new Path(lastDeviceId, tsSchema.getMeasurementId(), true), tsSchema);
                      }
                    }
                    measurementSchemaList = new ArrayList<>();
                    // last chunk group Metadata
                    chunkGroupMetadataList.add(new ChunkGroupMetadata(lastDeviceId, chunkMetadataList));
                    lastDeviceId = null;
                  }
                  readPlanIndex();
                  truncatedSize = this.position();
                  break;
                default:
                  // the disk file is corrupted, using this file may be dangerous
                  throw new IOException("Unexpected marker " + marker);
              }
            }
            // now we read the tail of the data section, so we are sure that the last
            // ChunkGroupFooter is complete.
            if (lastDeviceId != null) {
              // schema of last chunk group
              if (newSchema != null) {
                for (IMeasurementSchema tsSchema : measurementSchemaList) {
                  newSchema.putIfAbsent(
                      new Path(lastDeviceId, tsSchema.getMeasurementId(), true), tsSchema);
                }
              }
              // last chunk group Metadata
              chunkGroupMetadataList.add(new ChunkGroupMetadata(lastDeviceId, chunkMetadataList));
            }
            if (isComplete) {
              truncatedSize = TsFileCheckStatus.COMPLETE_FILE;
            } else {
              truncatedSize = this.position() - 1;
            }
          } catch (Exception e) {
            LOGGER.warn(
                "TsFile {} self-check cannot proceed at position {} " + "recovered, because : {}",
                file,
                this.position(),
                e.getMessage());
          }
          // Despite the completeness of the data section, we will discard current FileMetadata
          // so that we can continue to write data into this tsfile.
          return truncatedSize;
        }
      };
      StringBuilder str1 = new StringBuilder();
      for (int i = 0; i < 21; i++) {
        str1.append("|");
      }
      splitStr = str1.toString();
      // get metadata information
      tsFileMetaData = reader.readFileMetadata();
      allChunkGroupMetadata = new ArrayList<>();
      if (reader.selfCheck(null, allChunkGroupMetadata, false) != TsFileCheckStatus.COMPLETE_FILE) {
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
    if (tsFileMetaData.getMetadataIndex().getChildren().isEmpty()) {
      printlnBoth(pw, String.format("%20s", reader.getFileMetadataPos() - 1) + "|\t[marker] 2");
    } else {
      printlnBoth(
          pw, String.format("%20s", reader.readFileMetadata().getMetaOffset()) + "|\t[marker] 2");
    }

    // get all TimerseriesIndex
    Map<Long, Pair<Path, TimeseriesMetadata>> timeseriesMetadataMap =
        reader.getAllTimeseriesMetadataWithOffset();

    // get all IndexOfTimerseriesIndex (excluding the root node in TsFileMetadata)
    MetadataIndexNode metadataIndexNode = tsFileMetaData.getMetadataIndex();
    TreeMap<Long, MetadataIndexNode> metadataIndexNodeMap = new TreeMap<>();
    List<String> treeOutputStringBuffer = new ArrayList<>();
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
      MetadataIndexNode rootNode = tsFileMetaData.getMetadataIndex();
      printIndexOfTimerseriesIndex(reader.getFileMetadataPos(), rootNode);

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
            pw, String.format("%20s", "") + "|\t\t[deviceID] " + chunkGroupHeader.getDeviceID());
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

        printlnBoth(
            pw,
            splitStr
                + " [Chunk Group] of "
                + ((PlainDeviceID) chunkGroupMetadata.getDevice()).toStringID()
                + " ends");
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
              MetadataIndexNode.deserializeFrom(buffer, currentChildLevelIsDevice);
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
              MetadataIndexNode.deserializeFrom(tsFileInput.wrapAsInputStream(), isDeviceLevel);
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
      MetadataIndexNode metadataIndexNode = tsFileMetaData.getMetadataIndex();
      Map<Long, Pair<Path, TimeseriesMetadata>> timeseriesMetadataMap = new TreeMap<>();
      List<IMetadataIndexEntry> metadataIndexEntryList = metadataIndexNode.getChildren();
      for (int i = 0; i < metadataIndexEntryList.size(); i++) {
        IMetadataIndexEntry metadataIndexEntry = metadataIndexEntryList.get(i);
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

  // for test
  protected List<ChunkGroupMetadata> getAllChunkGroupMetadata() {
    return allChunkGroupMetadata;
  }
}
