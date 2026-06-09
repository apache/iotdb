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

package org.apache.iotdb.db.storageengine.load.splitter;

import org.apache.iotdb.calc.utils.IObjectPath;
import org.apache.iotdb.calc.utils.ObjectTypeUtils;
import org.apache.iotdb.common.rpc.thrift.TTimePartitionSlot;
import org.apache.iotdb.commons.path.PatternTreeMap;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.commons.utils.TimePartitionUtils;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.load.LoadFileException;
import org.apache.iotdb.db.exception.load.ObjectFileCorruptedException;
import org.apache.iotdb.db.i18n.StorageEngineMessages;
import org.apache.iotdb.db.pipe.event.common.tsfile.parser.scan.SinglePageWholeChunkReader;
import org.apache.iotdb.db.pipe.event.common.tsfile.parser.util.ModsOperationUtil;
import org.apache.iotdb.db.pipe.event.common.tsfile.parser.util.ModsOperationUtil.ModsInfo;
import org.apache.iotdb.db.storageengine.dataregion.modification.ModEntry;
import org.apache.iotdb.db.storageengine.dataregion.modification.ModificationFile;
import org.apache.iotdb.db.storageengine.dataregion.modification.TableDeletionEntry;
import org.apache.iotdb.db.utils.datastructure.PatternTreeMapFactory;

import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.common.constant.TsFileConstant;
import org.apache.tsfile.compress.IUnCompressor;
import org.apache.tsfile.encoding.decoder.Decoder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.exception.TsFileRuntimeException;
import org.apache.tsfile.file.MetaMarker;
import org.apache.tsfile.file.header.ChunkGroupHeader;
import org.apache.tsfile.file.header.ChunkHeader;
import org.apache.tsfile.file.header.PageHeader;
import org.apache.tsfile.file.metadata.IChunkMetadata;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.StringArrayDeviceID;
import org.apache.tsfile.file.metadata.TimeseriesMetadata;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.read.common.BatchData;
import org.apache.tsfile.read.reader.page.PageReader;
import org.apache.tsfile.read.reader.page.TimePageReader;
import org.apache.tsfile.read.reader.page.ValuePageReader;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.utils.TsPrimitiveType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class TsFileSplitter {
  private static final Logger logger = LoggerFactory.getLogger(TsFileSplitter.class);

  private static final IoTDBConfig CONFIG = IoTDBDescriptor.getInstance().getConfig();

  private final File tsFile;
  private final TsFileDataConsumer consumer;

  private final boolean fileContainsObjectColumns;

  private final File objectFileSearchRoot;
  private final Map<String, String> tableNameRewriteMap;
  private final Map<String, Map<String, String>> columnNameRewriteMap;

  private Map<Long, IChunkMetadata> offset2ChunkMetadata = new HashMap<>();
  private List<ModEntry> deletions = new ArrayList<>();
  private Map<Integer, List<AlignedChunkData>> pageIndex2ChunkData = new HashMap<>();
  private Map<Integer, long[]> pageIndex2Times = new HashMap<>();
  private boolean isTimeChunkNeedDecode = true;
  private IDeviceID curDevice = null;
  private String currentInputTableName = null;
  private Map<String, String> currentMeasurementRewriteMap = Collections.emptyMap();
  private boolean isCurrentDeviceRewritten = false;
  private boolean isAligned;
  private int timeChunkIndexOfCurrentValueColumn = 0;
  private Set<TTimePartitionSlot> timePartitionSlots = new HashSet<>();

  // Maintain the number of times the chunk of each measurement appears.
  private Map<String, Integer> valueColumn2TimeChunkIndex = new HashMap<>();
  // When encountering a value chunk, find the corresponding time chunk index through
  // valueColumn2TimeChunkIndex,
  // and then restore the corresponding context in the following List through time chunk index
  private List<Map<Integer, List<AlignedChunkData>>> pageIndex2ChunkDataList = new ArrayList<>();
  private List<Map<Integer, long[]>> pageIndex2TimesList = null;
  private List<Boolean> isTimeChunkNeedDecodeList = new ArrayList<>();

  private PatternTreeMap<ModEntry, PatternTreeMapFactory.ModsSerializer> modsPatternTree;

  @TestOnly
  public TsFileSplitter(File tsFile, TsFileDataConsumer consumer) {
    this(tsFile, consumer, false, null);
  }

  public TsFileSplitter(
      File tsFile, TsFileDataConsumer consumer, boolean fileContainsObjectColumns) {
    this(tsFile, consumer, fileContainsObjectColumns, null);
  }

  public TsFileSplitter(
      File tsFile,
      TsFileDataConsumer consumer,
      boolean fileContainsObjectColumns,
      File objectFileSearchRoot) {
    this(
        tsFile,
        consumer,
        fileContainsObjectColumns,
        objectFileSearchRoot,
        Collections.emptyMap(),
        Collections.emptyMap());
  }

  public TsFileSplitter(
      File tsFile,
      TsFileDataConsumer consumer,
      boolean fileContainsObjectColumns,
      File objectFileSearchRoot,
      Map<String, String> tableNameRewriteMap) {
    this(
        tsFile,
        consumer,
        fileContainsObjectColumns,
        objectFileSearchRoot,
        tableNameRewriteMap,
        Collections.emptyMap());
  }

  public TsFileSplitter(
      File tsFile,
      TsFileDataConsumer consumer,
      boolean fileContainsObjectColumns,
      File objectFileSearchRoot,
      Map<String, String> tableNameRewriteMap,
      Map<String, Map<String, String>> columnNameRewriteMap) {
    this.tsFile = tsFile;
    this.consumer = consumer;
    this.fileContainsObjectColumns = fileContainsObjectColumns;
    this.objectFileSearchRoot = objectFileSearchRoot;
    this.tableNameRewriteMap =
        Objects.nonNull(tableNameRewriteMap) ? tableNameRewriteMap : Collections.emptyMap();
    this.columnNameRewriteMap =
        Objects.nonNull(columnNameRewriteMap)
            ? copyColumnNameRewriteMap(columnNameRewriteMap)
            : Collections.emptyMap();
  }

  private static Map<String, Map<String, String>> copyColumnNameRewriteMap(
      final Map<String, Map<String, String>> columnNameRewriteMap) {
    final Map<String, Map<String, String>> copiedMap = new HashMap<>();
    columnNameRewriteMap.forEach(
        (tableName, columnMap) -> copiedMap.put(tableName, new HashMap<>(columnMap)));
    return copiedMap;
  }

  @SuppressWarnings({"squid:S3776", "squid:S6541"})
  public void splitTsFileByDataPartition()
      throws IOException, LoadFileException, IllegalStateException {
    try (TsFileSequenceReader reader = new TsFileSequenceReader(tsFile.getAbsolutePath())) {
      getAllModification(deletions);

      modsPatternTree = PatternTreeMapFactory.getModsPatternTreeMap();
      if (fileContainsObjectColumns) {
        for (ModEntry mod : deletions) {
          modsPatternTree.append(mod.keyOfPatternTree(), mod);
        }
      }

      if (!checkMagic(reader)) {
        throw new TsFileRuntimeException(
            String.format("Magic String check error when parsing TsFile %s.", tsFile.getPath()));
      }

      reader.position((long) TSFileConfig.MAGIC_STRING.getBytes().length + 1);
      getChunkMetadata(reader, offset2ChunkMetadata);
      byte marker;
      // It should be noted that time chunk and its corresponding value chunk are not necessarily
      // consecutive in the file.
      // Therefore, every time after consuming a set of AlignedChunkData, we still need to retain
      // some structural information
      // for the corresponding value chunk that may appear later.
      while ((marker = reader.readMarker()) != MetaMarker.SEPARATOR) {
        switch (marker) {
          case MetaMarker.CHUNK_HEADER:
          case MetaMarker.TIME_CHUNK_HEADER:
          case MetaMarker.ONLY_ONE_PAGE_CHUNK_HEADER:
          case MetaMarker.ONLY_ONE_PAGE_TIME_CHUNK_HEADER:
            processTimeChunkOrNonAlignedChunk(reader, marker);
            if (isAligned) {
              storeTimeChunkContext();
            }
            break;
          case MetaMarker.VALUE_CHUNK_HEADER:
          case MetaMarker.ONLY_ONE_PAGE_VALUE_CHUNK_HEADER:
            processValueChunk(reader, marker);
            break;
          case MetaMarker.CHUNK_GROUP_HEADER:
            ChunkGroupHeader chunkGroupHeader = reader.readChunkGroupHeader();
            final IDeviceID sourceDevice = chunkGroupHeader.getDeviceID();
            currentInputTableName = sourceDevice.getTableName();
            currentMeasurementRewriteMap =
                columnNameRewriteMap.getOrDefault(currentInputTableName, Collections.emptyMap());
            curDevice = rewriteDeviceIfNecessary(sourceDevice);
            isCurrentDeviceRewritten = !curDevice.equals(sourceDevice);
            pageIndex2ChunkDataList = new ArrayList<>();
            pageIndex2TimesList = new ArrayList<>();
            isTimeChunkNeedDecodeList = new ArrayList<>();
            valueColumn2TimeChunkIndex = new HashMap<>();
            timeChunkIndexOfCurrentValueColumn = 0;
            break;
          case MetaMarker.OPERATION_INDEX_RANGE:
            reader.readPlanIndex();
            break;
          default:
            MetaMarker.handleUnexpectedMarker(marker);
        }
      }

      consumeAllAlignedChunkData(reader.position(), pageIndex2ChunkData);
      handleModification(deletions);
    }
  }

  private IDeviceID rewriteDeviceIfNecessary(final IDeviceID device) {
    if (tableNameRewriteMap.isEmpty() || !device.isTableModel()) {
      return device;
    }

    final String rewrittenTableName = tableNameRewriteMap.get(device.getTableName());
    if (Objects.isNull(rewrittenTableName)) {
      return device;
    }

    final Object[] segments = device.getSegments();
    final String[] rewrittenSegments = new String[segments.length];
    rewrittenSegments[0] = rewrittenTableName;
    for (int i = 1; i < segments.length; i++) {
      rewrittenSegments[i] = Objects.toString(segments[i], null);
    }
    return new StringArrayDeviceID(rewrittenSegments);
  }

  private String rewriteMeasurementIfNecessary(final String measurementID) {
    if (measurementID == null
        || measurementID.isEmpty()
        || currentMeasurementRewriteMap.isEmpty()) {
      return measurementID;
    }

    return currentMeasurementRewriteMap.getOrDefault(measurementID, measurementID);
  }

  private ChunkHeader rewriteChunkHeaderIfNecessary(final ChunkHeader header) {
    final String rewrittenMeasurementID = rewriteMeasurementIfNecessary(header.getMeasurementID());
    if (!Objects.equals(rewrittenMeasurementID, header.getMeasurementID())) {
      header.setMeasurementID(rewrittenMeasurementID);
    }
    return header;
  }

  private boolean shouldRewriteObjectBinary(final ChunkHeader header) {
    return fileContainsObjectColumns
        && isCurrentDeviceRewritten
        && header.getDataType() == TSDataType.OBJECT;
  }

  private ModsInfo getModsInfoForMeasurement(String measurementID) {
    if (modsPatternTree == null || modsPatternTree.isEmpty()) {
      return null;
    }
    List<ModsInfo> infos =
        ModsOperationUtil.initializeMeasurementMods(
            curDevice, Collections.singletonList(measurementID), modsPatternTree);
    return infos.isEmpty() ? null : infos.get(0);
  }

  private void processTimeChunkOrNonAlignedChunk(TsFileSequenceReader reader, byte marker)
      throws IOException, LoadFileException {
    long chunkOffset = reader.position();
    timeChunkIndexOfCurrentValueColumn = pageIndex2TimesList.size();
    consumeAllAlignedChunkData(chunkOffset, pageIndex2ChunkData);

    ChunkHeader header = reader.readChunkHeader(marker);
    rewriteChunkHeaderIfNecessary(header);
    String measurementId = header.getMeasurementID();
    if (header.getDataSize() == 0) {
      throw new TsFileRuntimeException(
          String.format(
              "Empty Nonaligned Chunk or Time Chunk with offset %d in TsFile %s.",
              chunkOffset, tsFile.getPath()));
    }

    isAligned =
        ((header.getChunkType() & TsFileConstant.TIME_COLUMN_MASK)
            == TsFileConstant.TIME_COLUMN_MASK);
    if (isAligned) {
      pageIndex2Times = new HashMap<>();
      pageIndex2ChunkData = new HashMap<>();
      isTimeChunkNeedDecode = true;
    }

    IChunkMetadata chunkMetadata = offset2ChunkMetadata.get(chunkOffset - Byte.BYTES);
    // When loading TsFile with Chunk in data zone but no matched ChunkMetadata
    // at the end of file, this Chunk needs to be skipped.
    if (chunkMetadata == null) {
      reader.readChunk(-1, header.getDataSize());
      return;
    }
    TTimePartitionSlot timePartitionSlot =
        TimePartitionUtils.getTimePartitionSlot(chunkMetadata.getStartTime());
    ChunkData chunkData =
        ChunkData.createChunkData(isAligned, curDevice, header, timePartitionSlot);

    final boolean rewriteObjectBinary = shouldRewriteObjectBinary(header);
    if (!needDecodeChunk(chunkMetadata)
        && !rewriteObjectBinary
        && !(isAligned && fileContainsObjectColumns && isCurrentDeviceRewritten)) {
      final long chunkDataStartOffset = reader.position();
      if (fileContainsObjectColumns && isAligned) {
        pageIndex2Times = collectAlignedTimeBatchForObjectColumn(reader, header);
        reader.position(chunkDataStartOffset);
      } else if (fileContainsObjectColumns && header.getDataType() == TSDataType.OBJECT) {
        ModsInfo modsInfo = getModsInfoForMeasurement(measurementId);
        collectNonAlignedObjectFiles(
            reader, header, chunkData.getTimePartitionSlot(), chunkData, modsInfo);
        reader.position(chunkDataStartOffset);
      }
      chunkData.setNotDecode();
      chunkData.writeEntireChunk(reader.readChunk(-1, header.getDataSize()), chunkMetadata);
      if (isAligned) {
        isTimeChunkNeedDecode = false;
        pageIndex2ChunkData
            .computeIfAbsent(1, o -> new ArrayList<>())
            .add((AlignedChunkData) chunkData);
      } else {
        consumeChunkData(measurementId, chunkOffset, chunkData);
      }
      return;
    }

    decodeAndWriteTimeChunkOrNonAlignedChunk(reader, header, chunkMetadata, chunkOffset, chunkData);
  }

  private void decodeAndWriteTimeChunkOrNonAlignedChunk(
      TsFileSequenceReader reader,
      ChunkHeader header,
      IChunkMetadata chunkMetadata,
      long chunkOffset,
      ChunkData chunkData)
      throws IOException, LoadFileException {
    String measurementId = header.getMeasurementID();
    TTimePartitionSlot timePartitionSlot = chunkData.getTimePartitionSlot();
    Decoder defaultTimeDecoder =
        Decoder.getDecoderByType(
            TSEncoding.valueOf(TSFileDescriptor.getInstance().getConfig().getTimeEncoder()),
            TSDataType.INT64);
    Decoder valueDecoder = Decoder.getDecoderByType(header.getEncodingType(), header.getDataType());
    int dataSize = header.getDataSize();
    int pageIndex = 0;
    if (isAligned) {
      isTimeChunkNeedDecode = true;
      pageIndex2Times = new HashMap<>();
    }

    ModsInfo modsInfo = getModsInfoForMeasurement(measurementId);
    final boolean rewriteObjectBinary = shouldRewriteObjectBinary(header);

    while (dataSize > 0) {
      PageHeader pageHeader =
          reader.readPageHeader(
              header.getDataType(), (header.getChunkType() & 0x3F) == MetaMarker.CHUNK_HEADER);
      long pageDataSize = pageHeader.getSerializedPageSize();
      if (!needDecodePage(pageHeader, chunkMetadata)) { // an entire page
        long startTime =
            pageHeader.getStatistics() == null
                ? chunkMetadata.getStartTime()
                : pageHeader.getStartTime();
        TTimePartitionSlot pageTimePartitionSlot =
            TimePartitionUtils.getTimePartitionSlot(startTime);
        if (!timePartitionSlot.equals(pageTimePartitionSlot)) {
          if (!isAligned) {
            consumeChunkData(measurementId, chunkOffset, chunkData);
          }
          timePartitionSlot = pageTimePartitionSlot;
          chunkData = ChunkData.createChunkData(isAligned, curDevice, header, timePartitionSlot);
        }
        if (isAligned) {
          pageIndex2ChunkData
              .computeIfAbsent(pageIndex, o -> new ArrayList<>())
              .add((AlignedChunkData) chunkData);
        }
        final ByteBuffer compressedPage = reader.readCompressedPage(pageHeader);
        if (isAligned && fileContainsObjectColumns && isCurrentDeviceRewritten) {
          final ByteBuffer uncompressedPage =
              SinglePageWholeChunkReader.uncompressPageData(
                  pageHeader,
                  IUnCompressor.getUnCompressor(header.getCompressionType()),
                  compressedPage.duplicate());
          final Pair<long[], Object[]> tvArray =
              decodePage(true, uncompressedPage, pageHeader, defaultTimeDecoder, null, header);
          pageIndex2Times.put(pageIndex, tvArray.left);
          chunkData.writeDecodePage(tvArray.left, tvArray.right, tvArray.left.length);
          pageIndex += 1;
          dataSize -= pageDataSize;
          continue;
        }
        if (fileContainsObjectColumns && header.getDataType() == TSDataType.OBJECT && !isAligned) {
          final ByteBuffer uncompressedPage =
              SinglePageWholeChunkReader.uncompressPageData(
                  pageHeader,
                  IUnCompressor.getUnCompressor(header.getCompressionType()),
                  compressedPage.duplicate());
          final Pair<long[], Object[]> tvArray =
              decodePage(
                  false, uncompressedPage, pageHeader, defaultTimeDecoder, valueDecoder, header);
          processObjectValues(
              chunkData, tvArray.left, tvArray.right, timePartitionSlot, modsInfo, measurementId);
          if (rewriteObjectBinary) {
            chunkData.writeDecodePage(tvArray.left, tvArray.right, tvArray.left.length);
          } else {
            chunkData.writeEntirePage(pageHeader, compressedPage);
          }
        } else {
          chunkData.writeEntirePage(pageHeader, compressedPage);
        }
      } else { // split page
        ByteBuffer pageData = reader.readPage(pageHeader, header.getCompressionType());
        Pair<long[], Object[]> tvArray =
            decodePage(isAligned, pageData, pageHeader, defaultTimeDecoder, valueDecoder, header);
        long[] times = tvArray.left;
        Object[] values = tvArray.right;
        if (isAligned) {
          pageIndex2Times.put(pageIndex, times);
        }

        int satisfiedLength = 0;
        long endTime =
            timePartitionSlot.getStartTime() + TimePartitionUtils.getTimePartitionInterval();
        // beware of overflow
        if (endTime <= timePartitionSlot.getStartTime()) {
          endTime = Long.MAX_VALUE;
        }
        for (int i = 0; i < times.length; i++) {
          if (times[i] >= endTime) {
            if (fileContainsObjectColumns
                && header.getDataType() == TSDataType.OBJECT
                && !isAligned) {
              processObjectValues(
                  chunkData, times, values, timePartitionSlot, modsInfo, measurementId);
            }
            chunkData.writeDecodePage(times, values, satisfiedLength);
            if (isAligned) {
              pageIndex2ChunkData
                  .computeIfAbsent(pageIndex, o -> new ArrayList<>())
                  .add((AlignedChunkData) chunkData);
            } else {
              consumeChunkData(measurementId, chunkOffset, chunkData);
            }

            timePartitionSlot = TimePartitionUtils.getTimePartitionSlot(times[i]);
            satisfiedLength = 0;
            endTime =
                timePartitionSlot.getStartTime() + TimePartitionUtils.getTimePartitionInterval();
            if (endTime <= timePartitionSlot.getStartTime()) {
              endTime = Long.MAX_VALUE;
            }
            chunkData = ChunkData.createChunkData(isAligned, curDevice, header, timePartitionSlot);
          }
          satisfiedLength += 1;
        }
        if (fileContainsObjectColumns && header.getDataType() == TSDataType.OBJECT && !isAligned) {
          processObjectValues(chunkData, times, values, timePartitionSlot, modsInfo, measurementId);
        }
        chunkData.writeDecodePage(times, values, satisfiedLength);
        if (isAligned) {
          pageIndex2ChunkData
              .computeIfAbsent(pageIndex, o -> new ArrayList<>())
              .add((AlignedChunkData) chunkData);
        }
      }

      pageIndex += 1;
      dataSize -= pageDataSize;
    }

    if (!isAligned) {
      consumeChunkData(measurementId, chunkOffset, chunkData);
    }
  }

  private void processValueChunk(TsFileSequenceReader reader, byte marker)
      throws IOException, LoadFileException {
    long chunkOffset = reader.position();
    IChunkMetadata chunkMetadata = offset2ChunkMetadata.get(chunkOffset - Byte.BYTES);
    ChunkHeader header = reader.readChunkHeader(marker);
    rewriteChunkHeaderIfNecessary(header);
    // When loading TsFile with Chunk in data zone but no matched ChunkMetadata
    // at the end of file, this Chunk needs to be skipped.
    if (chunkMetadata == null) {
      reader.readChunk(-1, header.getDataSize());
      return;
    }
    switchToTimeChunkContextOfCurrentMeasurement(reader, header.getMeasurementID());
    if (header.getDataSize() == 0) {
      handleEmptyValueChunk(header, pageIndex2ChunkData, chunkMetadata, isTimeChunkNeedDecode);
      return;
    }

    ModsInfo modsInfo = getModsInfoForMeasurement(header.getMeasurementID());
    final boolean rewriteObjectBinary = shouldRewriteObjectBinary(header);

    if (!isTimeChunkNeedDecode && !rewriteObjectBinary) {
      AlignedChunkData alignedChunkData = pageIndex2ChunkData.get(1).get(0);
      alignedChunkData.addValueChunk(header);
      if (fileContainsObjectColumns && header.getDataType() == TSDataType.OBJECT) {
        final long valueChunkDataStartOffset = reader.position();
        collectAlignedObjectFiles(reader, header, pageIndex2Times, alignedChunkData, modsInfo);
        reader.position(valueChunkDataStartOffset);
      }
      alignedChunkData.writeEntireChunk(reader.readChunk(-1, header.getDataSize()), chunkMetadata);
      return;
    }

    Set<ChunkData> allChunkData = new HashSet<>();
    int dataSize = header.getDataSize();
    int pageIndex = 0;
    Decoder valueDecoder = Decoder.getDecoderByType(header.getEncodingType(), header.getDataType());

    while (dataSize > 0) {
      PageHeader pageHeader =
          reader.readPageHeader(
              header.getDataType(), (header.getChunkType() & 0x3F) == MetaMarker.CHUNK_HEADER);
      List<AlignedChunkData> alignedChunkDataList = pageIndex2ChunkData.get(pageIndex);
      for (AlignedChunkData alignedChunkData : alignedChunkDataList) {
        if (!allChunkData.contains(alignedChunkData)) {
          alignedChunkData.addValueChunk(header);
          allChunkData.add(alignedChunkData);
        }
      }
      if (alignedChunkDataList.size() == 1 && !rewriteObjectBinary) { // write entire page
        // write the entire page if it's not an empty page.
        alignedChunkDataList
            .get(0)
            .writeEntirePage(pageHeader, reader.readCompressedPage(pageHeader));
      } else if (pageHeader.getSerializedPageSize() == 0) {
        TsPrimitiveType[] values = new TsPrimitiveType[pageIndex2Times.get(pageIndex).length];
        for (AlignedChunkData alignedChunkData : alignedChunkDataList) {
          alignedChunkData.writeDecodeValuePage(
              pageIndex2Times.get(pageIndex), values, header.getDataType());
        }
      } else { // decode page
        long[] times = pageIndex2Times.get(pageIndex);
        final ByteBuffer pageData = reader.readPage(pageHeader, header.getCompressionType());
        for (AlignedChunkData alignedChunkData : alignedChunkDataList) {
          final TsPrimitiveType[] values =
              decodeValuePage(header, pageHeader, pageData.duplicate(), times, valueDecoder);
          if (fileContainsObjectColumns && header.getDataType() == TSDataType.OBJECT) {
            processObjectAlignedValues(
                alignedChunkData,
                times,
                values,
                modsInfo,
                alignedChunkData.timePartitionSlot,
                header.getMeasurementID());
          }
          alignedChunkData.writeDecodeValuePage(times, values, header.getDataType());
        }
      }
      long pageDataSize = pageHeader.getSerializedPageSize();
      pageIndex += 1;
      dataSize -= pageDataSize;
    }
  }

  private void storeTimeChunkContext() {
    pageIndex2TimesList.add(pageIndex2Times);
    pageIndex2ChunkDataList.add(pageIndex2ChunkData);
    isTimeChunkNeedDecodeList.add(isTimeChunkNeedDecode);
  }

  private void switchToTimeChunkContextOfCurrentMeasurement(
      TsFileSequenceReader reader, String measurement) throws IOException, LoadFileException {
    int index = valueColumn2TimeChunkIndex.getOrDefault(measurement, 0);
    if (index != timeChunkIndexOfCurrentValueColumn) {
      consumeAllAlignedChunkData(reader.position(), pageIndex2ChunkData);
    }
    timeChunkIndexOfCurrentValueColumn = index;
    valueColumn2TimeChunkIndex.put(measurement, index + 1);
    pageIndex2Times = pageIndex2TimesList.get(index);
    pageIndex2ChunkData = pageIndex2ChunkDataList.get(index);

    isTimeChunkNeedDecode = isTimeChunkNeedDecodeList.get(index);
  }

  private void getAllModification(List<ModEntry> deletions) throws IOException {
    ModificationFile.readAllModifications(tsFile, true).stream()
        .map(this::rewriteDeletionIfNecessary)
        .forEach(deletions::add);
  }

  private ModEntry rewriteDeletionIfNecessary(final ModEntry deletion) {
    if ((tableNameRewriteMap.isEmpty() && columnNameRewriteMap.isEmpty())
        || !(deletion instanceof TableDeletionEntry)) {
      return deletion;
    }
    final ModEntry rewrittenDeletion =
        ((TableDeletionEntry) deletion)
            .rewriteTableNameAndColumns(tableNameRewriteMap, columnNameRewriteMap);
    return rewrittenDeletion == deletion ? deletion.clone() : rewrittenDeletion;
  }

  private boolean checkMagic(TsFileSequenceReader reader) throws IOException {
    String magic = reader.readHeadMagic();
    if (!magic.equals(TSFileConfig.MAGIC_STRING)) {
      logger.error(StorageEngineMessages.FILE_MAGIC_STRING_INCORRECT, reader.getFileName());
      return false;
    }

    byte versionNumber = reader.readVersionNumber();
    if (versionNumber < TSFileConfig.VERSION_NUMBER) {
      if (versionNumber == TSFileConfig.VERSION_NUMBER_V3 && TSFileConfig.VERSION_NUMBER == 4) {
        logger.info(
            "try to load TsFile V3 into current version (V4), file path: {}", reader.getFileName());
      } else {
        logger.error(StorageEngineMessages.FILE_VERSION_TOO_OLD, reader.getFileName());
        return false;
      }
    } else if (versionNumber > TSFileConfig.VERSION_NUMBER) {
      logger.error(
          "the file's Version Number is higher than current, file path: {}", reader.getFileName());
      return false;
    }

    if (!reader.readTailMagic().equals(TSFileConfig.MAGIC_STRING)) {
      logger.error(StorageEngineMessages.FILE_NOT_CLOSED_CORRECTLY, reader.getFileName());
      return false;
    }
    return true;
  }

  private void getChunkMetadata(
      TsFileSequenceReader reader, Map<Long, IChunkMetadata> offset2ChunkMetadata)
      throws IOException {
    Map<IDeviceID, List<TimeseriesMetadata>> device2Metadata =
        reader.getAllTimeseriesMetadata(true);
    for (Map.Entry<IDeviceID, List<TimeseriesMetadata>> entry : device2Metadata.entrySet()) {
      for (TimeseriesMetadata timeseriesMetadata : entry.getValue()) {
        for (IChunkMetadata chunkMetadata : timeseriesMetadata.getChunkMetadataList()) {
          offset2ChunkMetadata.put(chunkMetadata.getOffsetOfChunkHeader(), chunkMetadata);
        }
      }
    }
  }

  private void handleModification(List<ModEntry> deletions) throws LoadFileException {
    for (final ModEntry mod : deletions) {
      consumer.apply(new DeletionData(mod));
    }
  }

  private void consumeAllAlignedChunkData(
      long offset, Map<Integer, List<AlignedChunkData>> pageIndex2ChunkData)
      throws LoadFileException {
    if (pageIndex2ChunkData.isEmpty()) {
      return;
    }

    Map<AlignedChunkData, BatchedAlignedValueChunkData> chunkDataMap = new HashMap<>();
    for (Map.Entry<Integer, List<AlignedChunkData>> entry : pageIndex2ChunkData.entrySet()) {
      List<AlignedChunkData> alignedChunkDataList = entry.getValue();
      for (int i = 0; i < alignedChunkDataList.size(); i++) {
        AlignedChunkData oldChunkData = alignedChunkDataList.get(i);
        BatchedAlignedValueChunkData chunkData =
            chunkDataMap.computeIfAbsent(oldChunkData, BatchedAlignedValueChunkData::new);
        alignedChunkDataList.set(i, chunkData);
      }
    }
    for (AlignedChunkData chunkData : chunkDataMap.keySet()) {
      timePartitionSlots.add(chunkData.getTimePartitionSlot());
      if (deletions.isEmpty()
          && timePartitionSlots.size() > CONFIG.getLoadTsFileSpiltPartitionMaxSize()) {
        throw new LoadFileException(
            String.format(
                "Time partition slots size is greater than %s",
                CONFIG.getLoadTsFileSpiltPartitionMaxSize()));
      }
      if (Boolean.FALSE.equals(consumer.apply(chunkData))) {
        throw new IllegalStateException(
            String.format(
                "Consume aligned chunk data error, next chunk offset: %d, chunkData: %s",
                offset, chunkData));
      }
    }
    this.pageIndex2ChunkData = new HashMap<>();
    this.pageIndex2Times = new HashMap<>();
  }

  private void consumeChunkData(String measurement, long offset, ChunkData chunkData)
      throws LoadFileException {
    timePartitionSlots.add(chunkData.getTimePartitionSlot());
    if (deletions.isEmpty()
        && timePartitionSlots.size() > CONFIG.getLoadTsFileSpiltPartitionMaxSize()) {
      throw new LoadFileException(
          String.format(
              "Time partition slots size is greater than %s",
              CONFIG.getLoadTsFileSpiltPartitionMaxSize()));
    }
    if (Boolean.FALSE.equals(consumer.apply(chunkData))) {
      throw new IllegalStateException(
          String.format(
              "Consume chunkData error, chunk offset: %d, measurement: %s, chunkData: %s",
              offset, measurement, chunkData));
    }
  }

  private boolean needDecodeChunk(IChunkMetadata chunkMetadata) {
    return !TimePartitionUtils.getTimePartitionSlot(chunkMetadata.getStartTime())
        .equals(TimePartitionUtils.getTimePartitionSlot(chunkMetadata.getEndTime()));
  }

  private boolean needDecodePage(PageHeader pageHeader, IChunkMetadata chunkMetadata) {
    if (pageHeader.getStatistics() == null) {
      return !TimePartitionUtils.getTimePartitionSlot(chunkMetadata.getStartTime())
          .equals(TimePartitionUtils.getTimePartitionSlot(chunkMetadata.getEndTime()));
    }
    return !TimePartitionUtils.getTimePartitionSlot(pageHeader.getStartTime())
        .equals(TimePartitionUtils.getTimePartitionSlot(pageHeader.getEndTime()));
  }

  private Pair<long[], Object[]> decodePage(
      boolean isAligned,
      ByteBuffer pageData,
      PageHeader pageHeader,
      Decoder timeDecoder,
      Decoder valueDecoder,
      ChunkHeader chunkHeader)
      throws IOException {
    if (isAligned) {
      TimePageReader timePageReader = new TimePageReader(pageHeader, pageData, timeDecoder);
      long[] times = timePageReader.getNextTimeBatch();
      return new Pair<>(times, new Object[times.length]);
    }

    valueDecoder.reset();
    PageReader pageReader =
        new PageReader(pageData, chunkHeader.getDataType(), valueDecoder, timeDecoder);
    BatchData batchData = pageReader.getAllSatisfiedPageData();
    long[] times = new long[batchData.length()];
    Object[] values = new Object[batchData.length()];
    int index = 0;
    while (batchData.hasCurrent()) {
      times[index] = batchData.currentTime();
      values[index++] = batchData.currentValue();
      batchData.next();
    }
    return new Pair<>(times, values);
  }

  private void handleEmptyValueChunk(
      ChunkHeader header,
      Map<Integer, List<AlignedChunkData>> pageIndex2ChunkData,
      IChunkMetadata chunkMetadata,
      boolean isTimeChunkNeedDecode)
      throws IOException {
    Set<ChunkData> allChunkData = new HashSet<>();
    for (Map.Entry<Integer, List<AlignedChunkData>> entry : pageIndex2ChunkData.entrySet()) {
      for (AlignedChunkData alignedChunkData : entry.getValue()) {
        if (!allChunkData.contains(alignedChunkData)) {
          alignedChunkData.addValueChunk(header);
          if (!isTimeChunkNeedDecode) {
            alignedChunkData.writeEntireChunk(ByteBuffer.allocate(0), chunkMetadata);
          } else {
            alignedChunkData.writeEntirePage(
                new PageHeader(0, 0, chunkMetadata.getStatistics()), ByteBuffer.allocate(0));
          }
          allChunkData.add(alignedChunkData);
        }
      }
    }
  }

  /**
   * handle empty page in aligned chunk, if uncompressedSize and compressedSize are both 0, and the
   * statistics is null, then the page is empty.
   *
   * @param pageHeader page header
   * @return true if the page is empty
   */
  private boolean isEmptyPage(PageHeader pageHeader) {
    return pageHeader.getUncompressedSize() == 0
        && pageHeader.getCompressedSize() == 0
        && pageHeader.getStatistics() == null;
  }

  private TsPrimitiveType[] decodeValuePage(
      TsFileSequenceReader reader,
      ChunkHeader chunkHeader,
      PageHeader pageHeader,
      long[] times,
      Decoder valueDecoder)
      throws IOException {
    if (pageHeader.getSerializedPageSize() == 0) {
      return new TsPrimitiveType[times.length];
    }

    ByteBuffer pageData = reader.readPage(pageHeader, chunkHeader.getCompressionType());
    return decodeValuePage(chunkHeader, pageHeader, pageData, times, valueDecoder);
  }

  private TsPrimitiveType[] decodeValuePage(
      ChunkHeader chunkHeader,
      PageHeader pageHeader,
      ByteBuffer pageData,
      long[] times,
      Decoder valueDecoder)
      throws IOException {
    valueDecoder.reset();
    ValuePageReader valuePageReader =
        new ValuePageReader(pageHeader, pageData, chunkHeader.getDataType(), valueDecoder);
    return valuePageReader.nextValueBatch(times);
  }

  private Binary processObjectValue(
      final ChunkData chunkData,
      final long time,
      final Binary valueBinary,
      final ModsInfo modsInfo,
      final String measurement)
      throws LoadFileException {

    if (ModsOperationUtil.isDelete(time, modsInfo)) {
      return valueBinary;
    }

    final Pair<Long, IObjectPath> lengthAndPath = parseObjectLengthAndPathFromBinary(valueBinary);
    if (lengthAndPath == null || lengthAndPath.getRight() == null) {
      return valueBinary;
    }

    final IObjectPath sourceObjectPath = lengthAndPath.getRight();
    final String sourceRelativePath = sourceObjectPath.toString();
    final IObjectPath targetObjectPath =
        rewriteObjectPathIfNecessary(sourceObjectPath, measurement);
    final String targetRelativePath = targetObjectPath.toString();
    verifyAndAddObjectFile(
        chunkData, sourceRelativePath, targetRelativePath, lengthAndPath.getLeft());
    if (targetObjectPath == sourceObjectPath
        || sourceRelativePath.equals(targetRelativePath)
        || measurement == null) {
      return valueBinary;
    }
    return ObjectTypeUtils.generateObjectBinary(lengthAndPath.getLeft(), targetObjectPath);
  }

  private Pair<Long, IObjectPath> parseObjectLengthAndPathFromBinary(final Binary binary) {
    if (binary == null) {
      return null;
    }
    return ObjectTypeUtils.parseObjectBinaryToSizeIObjectPathPair(binary);
  }

  private IObjectPath rewriteObjectPathIfNecessary(
      final IObjectPath sourceObjectPath, final String measurement) throws LoadFileException {
    if (!isCurrentDeviceRewritten || measurement == null) {
      return sourceObjectPath;
    }
    final Path sourcePath = sourceObjectPath.getPath();
    if (sourcePath.getNameCount() < 1) {
      throw new ObjectFileCorruptedException(
          "Object file relative path resolved to empty in OBJECT column of TsFile: "
              + tsFile.getPath());
    }
    try {
      final int regionId = Integer.parseInt(sourcePath.getName(0).toString());
      return IObjectPath.Factory.FACTORY.create(
          regionId, sourceObjectPath.getTime(), curDevice, measurement);
    } catch (final NumberFormatException e) {
      throw new ObjectFileCorruptedException(
          String.format("Invalid object file region id in path %s.", sourceObjectPath));
    }
  }

  private Map<Integer, long[]> collectAlignedTimeBatchForObjectColumn(
      final TsFileSequenceReader reader, final ChunkHeader chunkHeader) throws IOException {

    final Map<Integer, long[]> timeBatchByPage = new HashMap<>();
    final Decoder defaultTimeDecoder =
        Decoder.getDecoderByType(
            TSEncoding.valueOf(TSFileDescriptor.getInstance().getConfig().getTimeEncoder()),
            TSDataType.INT64);

    int pageIndex = 0;
    int dataSize = chunkHeader.getDataSize();

    while (dataSize > 0) {
      final PageHeader pageHeader =
          reader.readPageHeader(
              chunkHeader.getDataType(),
              (chunkHeader.getChunkType() & 0x3F) == MetaMarker.CHUNK_HEADER);
      final ByteBuffer pageData = reader.readPage(pageHeader, chunkHeader.getCompressionType());

      final Pair<long[], Object[]> tvArray =
          decodePage(true, pageData, pageHeader, defaultTimeDecoder, null, chunkHeader);
      timeBatchByPage.put(pageIndex++, tvArray.left);

      dataSize -= pageHeader.getSerializedPageSize();
    }
    return timeBatchByPage;
  }

  private void collectNonAlignedObjectFiles(
      final TsFileSequenceReader reader,
      final ChunkHeader chunkHeader,
      final TTimePartitionSlot timePartitionSlot,
      final ChunkData chunkData,
      final ModsInfo modsInfo)
      throws IOException, LoadFileException {

    final Decoder defaultTimeDecoder =
        Decoder.getDecoderByType(
            TSEncoding.valueOf(TSFileDescriptor.getInstance().getConfig().getTimeEncoder()),
            TSDataType.INT64);
    final Decoder valueDecoder =
        Decoder.getDecoderByType(chunkHeader.getEncodingType(), chunkHeader.getDataType());

    int dataSize = chunkHeader.getDataSize();

    while (dataSize > 0) {
      final PageHeader pageHeader =
          reader.readPageHeader(
              chunkHeader.getDataType(),
              (chunkHeader.getChunkType() & 0x3F) == MetaMarker.CHUNK_HEADER);
      final ByteBuffer pageData = reader.readPage(pageHeader, chunkHeader.getCompressionType());

      final Pair<long[], Object[]> tvArray =
          decodePage(false, pageData, pageHeader, defaultTimeDecoder, valueDecoder, chunkHeader);
      processObjectValues(
          chunkData,
          tvArray.left,
          tvArray.right,
          timePartitionSlot,
          modsInfo,
          chunkHeader.getMeasurementID());

      dataSize -= pageHeader.getSerializedPageSize();
    }
  }

  private void collectAlignedObjectFiles(
      final TsFileSequenceReader reader,
      final ChunkHeader chunkHeader,
      final Map<Integer, long[]> timeBatchByPage,
      final ChunkData chunkData,
      final ModsInfo modsInfo)
      throws IOException, LoadFileException {

    final Decoder valueDecoder =
        Decoder.getDecoderByType(chunkHeader.getEncodingType(), chunkHeader.getDataType());
    int dataSize = chunkHeader.getDataSize();
    int pageIndex = 0;

    while (dataSize > 0) {
      final PageHeader pageHeader =
          reader.readPageHeader(
              chunkHeader.getDataType(),
              (chunkHeader.getChunkType() & 0x3F) == MetaMarker.CHUNK_HEADER);
      final long[] times = timeBatchByPage.get(pageIndex);

      if (times == null || times.length == 0) {
        reader.readPage(pageHeader, chunkHeader.getCompressionType());
      } else {
        final TsPrimitiveType[] values =
            decodeValuePage(reader, chunkHeader, pageHeader, times, valueDecoder);
        processObjectAlignedValues(
            chunkData,
            times,
            values,
            modsInfo,
            chunkData.getTimePartitionSlot(),
            chunkHeader.getMeasurementID());
      }
      pageIndex++;
      dataSize -= pageHeader.getSerializedPageSize();
    }
  }

  private void processObjectValues(
      final ChunkData chunkData,
      final long[] times,
      final Object[] values,
      final TTimePartitionSlot timePartitionSlot,
      final ModsInfo modsInfo,
      final String measurement)
      throws LoadFileException {

    final long startTime = timePartitionSlot.getStartTime();
    long endTime = startTime + TimePartitionUtils.getTimePartitionInterval();
    endTime = (endTime <= startTime) ? Long.MAX_VALUE : endTime;

    for (int i = 0; i < times.length; i++) {
      final long time = times[i];
      if (time >= endTime) {
        break;
      }
      if (time < startTime || values[i] == null) {
        continue;
      }

      values[i] = processObjectValue(chunkData, time, (Binary) values[i], modsInfo, measurement);
    }
  }

  private void processObjectAlignedValues(
      final ChunkData chunkData,
      final long[] times,
      final TsPrimitiveType[] values,
      final ModsInfo modsInfo,
      final TTimePartitionSlot timePartitionSlot,
      final String measurement)
      throws LoadFileException {

    if (times == null || values == null) {
      return;
    }

    final long startTime = timePartitionSlot.getStartTime();
    long endTime = startTime + TimePartitionUtils.getTimePartitionInterval();
    endTime = (endTime <= startTime) ? Long.MAX_VALUE : endTime;

    final int len = Math.min(times.length, values.length);
    for (int i = 0; i < len; i++) {
      final long time = times[i];
      if (time >= endTime) {
        break;
      }
      if (time < startTime || values[i] == null) {
        continue;
      }
      if (values[i] != null) {
        final Binary rewrittenBinary =
            processObjectValue(chunkData, times[i], values[i].getBinary(), modsInfo, measurement);
        values[i].setBinary(rewrittenBinary);
      }
    }
  }

  private void verifyAndAddObjectFile(
      final ChunkData chunkData,
      final String sourceRelativePath,
      final String targetRelativePath,
      final long expectedLength)
      throws LoadFileException {
    if (sourceRelativePath == null || sourceRelativePath.isEmpty()) {
      throw new ObjectFileCorruptedException(
          "Object file relative path resolved to empty in OBJECT column of TsFile: "
              + tsFile.getPath());
    }

    File resolvedObjectFile = new File(objectFileSearchRoot, sourceRelativePath);

    if (!resolvedObjectFile.exists() || !resolvedObjectFile.isFile()) {
      throw new ObjectFileCorruptedException(
          String.format(
              "Referenced object file does not exist or is not a regular file: %s",
              resolvedObjectFile.getAbsolutePath()));
    }
    if (expectedLength >= 0 && resolvedObjectFile.length() != expectedLength) {
      throw new ObjectFileCorruptedException(
          String.format(
              "Referenced object file size mismatch, expected %d but got %d: %s",
              expectedLength, resolvedObjectFile.length(), resolvedObjectFile.getAbsolutePath()));
    }

    chunkData.addObjectRelativePath(objectFileSearchRoot, sourceRelativePath, targetRelativePath);
  }

  @FunctionalInterface
  public interface TsFileDataConsumer {
    boolean apply(TsFileData tsFileData) throws LoadFileException;
  }
}
