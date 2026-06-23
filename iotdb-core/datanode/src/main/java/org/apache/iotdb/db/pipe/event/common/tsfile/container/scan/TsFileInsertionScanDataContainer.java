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

package org.apache.iotdb.db.pipe.event.common.tsfile.container.scan;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeTaskMeta;
import org.apache.iotdb.commons.pipe.config.PipeConfig;
import org.apache.iotdb.commons.pipe.datastructure.pattern.PipePattern;
import org.apache.iotdb.commons.pipe.event.EnrichedEvent;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeRawTabletInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeTabletUtils;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeTabletUtils.TabletStringInternPool;
import org.apache.iotdb.db.pipe.event.common.tsfile.container.TsFileInsertionDataContainer;
import org.apache.iotdb.db.pipe.event.common.tsfile.parser.util.ModsOperationUtil;
import org.apache.iotdb.db.pipe.resource.PipeDataNodeResourceManager;
import org.apache.iotdb.db.pipe.resource.memory.PipeMemoryBlock;
import org.apache.iotdb.db.pipe.resource.memory.PipeMemoryWeightUtil;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.CompactionPathUtils;
import org.apache.iotdb.db.utils.datastructure.PatternTreeMapFactory;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;
import org.apache.iotdb.pipe.api.exception.PipeException;

import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.common.constant.TsFileConstant;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.MetaMarker;
import org.apache.tsfile.file.header.ChunkHeader;
import org.apache.tsfile.file.metadata.AlignedChunkMetadata;
import org.apache.tsfile.file.metadata.IChunkMetadata;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.PlainDeviceID;
import org.apache.tsfile.file.metadata.statistics.Statistics;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.read.common.BatchData;
import org.apache.tsfile.read.common.Chunk;
import org.apache.tsfile.read.filter.basic.Filter;
import org.apache.tsfile.read.reader.IChunkReader;
import org.apache.tsfile.read.reader.chunk.AlignedChunkReader;
import org.apache.tsfile.read.reader.chunk.ChunkReader;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.DateUtils;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.utils.TsPrimitiveType;
import org.apache.tsfile.write.UnSupportedDataTypeException;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;

public class TsFileInsertionScanDataContainer extends TsFileInsertionDataContainer {

  private static final Logger LOGGER = LoggerFactory.getLogger(ModsOperationUtil.class);

  private static final LocalDate EMPTY_DATE = LocalDate.of(1000, 1, 1);

  private final long startTime;
  private final long endTime;
  private final Filter filter;

  private IChunkReader chunkReader;
  private BatchData data;
  private final PipeMemoryBlock allocatedMemoryBlockForBatchData;
  private final PipeMemoryBlock allocatedMemoryBlockForChunk;

  private boolean currentIsMultiPage;
  private String currentDevice;
  private boolean currentIsAligned;
  private final List<MeasurementSchema> currentMeasurements = new ArrayList<>();
  private final TabletStringInternPool tabletStringInternPool = new TabletStringInternPool();
  private Exception deferredException;

  private final List<ModsOperationUtil.ModsInfo> modsInfos = new ArrayList<>();
  // Cached time chunk
  private final List<Chunk> timeChunkList = new ArrayList<>();
  private final List<Boolean> isMultiPageList = new ArrayList<>();
  private final List<Long> timeChunkPageMemorySizeList = new ArrayList<>();

  private final Map<String, Integer> measurementIndexMap = new HashMap<>();
  private int lastIndex = -1;
  private Chunk firstChunk4NextSequentialValueChunks;

  private byte lastMarker = Byte.MIN_VALUE;

  public TsFileInsertionScanDataContainer(
      final String pipeName,
      final long creationTime,
      final File tsFile,
      final PipePattern pattern,
      final long startTime,
      final long endTime,
      final PipeTaskMeta pipeTaskMeta,
      final EnrichedEvent sourceEvent,
      final boolean isWithMod)
      throws IOException {
    super(
        tsFile,
        pipeName,
        creationTime,
        pattern,
        startTime,
        endTime,
        pipeTaskMeta,
        sourceEvent,
        isWithMod);

    this.startTime = startTime;
    this.endTime = endTime;
    filter = Objects.nonNull(timeFilterExpression) ? timeFilterExpression.getFilter() : null;

    this.allocatedMemoryBlockForBatchData =
        PipeDataNodeResourceManager.memory()
            .forceAllocateForTabletWithRetry(
                IoTDBDescriptor.getInstance().getConfig().getPipeDataStructureTabletSizeInBytes());
    this.allocatedMemoryBlockForChunk =
        PipeDataNodeResourceManager.memory()
            .forceAllocateForTabletWithRetry(PipeConfig.getInstance().getPipeMaxReaderChunkSize());

    try {
      currentModifications =
          isWithMod
              ? ModsOperationUtil.loadModificationsFromTsFile(tsFile)
              : PatternTreeMapFactory.getModsPatternTreeMap();
      allocatedMemoryBlockForModifications =
          PipeDataNodeResourceManager.memory()
              .forceAllocateForTabletWithRetry(currentModifications.ramBytesUsed());

      tsFileSequenceReader =
          new TsFileSequenceReader(
              tsFile.getAbsolutePath(),
              !currentModifications.isEmpty(),
              !currentModifications.isEmpty());
      tsFileSequenceReader.position((long) TSFileConfig.MAGIC_STRING.getBytes().length + 1);

      prepareData();
    } catch (final Exception e) {
      close();
      throw e;
    }
  }

  public TsFileInsertionScanDataContainer(
      final File tsFile,
      final PipePattern pattern,
      final long startTime,
      final long endTime,
      final PipeTaskMeta pipeTaskMeta,
      final EnrichedEvent sourceEvent,
      final boolean isWithMod)
      throws IOException {
    this(null, 0, tsFile, pattern, startTime, endTime, pipeTaskMeta, sourceEvent, isWithMod);
  }

  @Override
  public Iterable<TabletInsertionEvent> toTabletInsertionEvents() {
    if (tabletInsertionIterable == null) {
      tabletInsertionIterable =
          () ->
              new Iterator<TabletInsertionEvent>() {

                @Override
                public boolean hasNext() {
                  throwIfDeferredException();
                  final boolean hasNext = Objects.nonNull(chunkReader);
                  if (hasNext && !parseStartTimeRecorded) {
                    // Record start time on first hasNext() that returns true
                    recordParseStartTime();
                  } else if (!hasNext && parseStartTimeRecorded && !parseEndTimeRecorded) {
                    // Record end time on last hasNext() that returns false
                    recordParseEndTime();
                  }
                  return hasNext;
                }

                @Override
                public TabletInsertionEvent next() {
                  if (!hasNext()) {
                    close();
                    throw new NoSuchElementException();
                  }

                  // currentIsAligned is initialized when TsFileInsertionScanDataContainer is
                  // constructed.
                  // When the getNextTablet function is called, currentIsAligned may be updated,
                  // causing
                  // the currentIsAligned information to be inconsistent with the current Tablet
                  // information.
                  final boolean isAligned = currentIsAligned;
                  final Tablet tablet = getNextTablet();
                  // Record tablet metrics
                  recordTabletMetrics(tablet);
                  final boolean isLast = isLastTabletWithoutDeferredException();
                  try {
                    return new PipeRawTabletInsertionEvent(
                        tablet,
                        isAligned,
                        sourceEvent != null ? sourceEvent.getPipeName() : null,
                        sourceEvent != null ? sourceEvent.getCreationTime() : 0,
                        pipeTaskMeta,
                        sourceEvent,
                        isLast);
                  } finally {
                    if (isLast) {
                      recordParseEndTimeIfNecessary();
                      close();
                    }
                  }
                }
              };
    }
    return tabletInsertionIterable;
  }

  public Iterable<Pair<Tablet, Boolean>> toTabletWithIsAligneds() {
    return () ->
        new Iterator<Pair<Tablet, Boolean>>() {
          @Override
          public boolean hasNext() {
            throwIfDeferredException();
            return Objects.nonNull(chunkReader);
          }

          @Override
          public Pair<Tablet, Boolean> next() {
            if (!hasNext()) {
              close();
              throw new NoSuchElementException();
            }

            // currentIsAligned is initialized when TsFileInsertionScanDataContainer is constructed.
            // When the getNextTablet function is called, currentIsAligned may be updated, causing
            // the currentIsAligned information to be inconsistent with the current Tablet
            // information.
            final boolean isAligned = currentIsAligned;
            final Tablet tablet = getNextTablet();
            try {
              return new Pair<>(tablet, isAligned);
            } finally {
              if (isLastTabletWithoutDeferredException()) {
                close();
              }
            }
          }
        };
  }

  public IDeviceID getCurrentDevice() {
    return Objects.nonNull(currentDevice) ? new PlainDeviceID(currentDevice) : null;
  }

  public boolean isCurrentAligned() {
    return currentIsAligned;
  }

  public List<String> getCurrentMeasurements() {
    final List<String> measurementIds = new ArrayList<>(currentMeasurements.size());
    for (final MeasurementSchema schema : currentMeasurements) {
      measurementIds.add(schema.getMeasurementId());
    }
    return measurementIds;
  }

  private Tablet getNextTablet() {
    try {
      Tablet tablet = null;

      if (!data.hasCurrent()) {
        tablet = new Tablet(currentDevice, currentMeasurements, 1);
        // Ignore the memory cost of tablet
        PipeDataNodeResourceManager.memory().forceResize(allocatedMemoryBlockForTablet, 0);
        return tablet;
      }

      boolean isFirstRow = true;
      while (data.hasCurrent()) {
        if (currentIsMultiPage
            || data.currentTime() >= startTime && data.currentTime() <= endTime) {
          if (isFirstRow) {
            // Calculate row count and memory size of the tablet based on the first row
            Pair<Integer, Integer> rowCountAndMemorySize =
                PipeMemoryWeightUtil.calculateTabletRowCountAndMemory(data);
            tablet =
                new Tablet(currentDevice, currentMeasurements, rowCountAndMemorySize.getLeft());
            if (allocatedMemoryBlockForTablet.getMemoryUsageInBytes()
                < rowCountAndMemorySize.getRight()) {
              PipeDataNodeResourceManager.memory()
                  .forceResize(allocatedMemoryBlockForTablet, rowCountAndMemorySize.getRight());
            }
            isFirstRow = false;
          }

          final int rowIndex = tablet.rowSize;

          if (putValueToColumns(data, tablet, rowIndex)) {
            PipeTabletUtils.putTimestamp(tablet, rowIndex, data.currentTime());
          }
        }

        data.next();
        while (!data.hasCurrent() && chunkReader.hasNextSatisfiedPage()) {
          data = nextPageData();
        }

        if (tablet != null && tablet.rowSize == tablet.getMaxRowNumber()) {
          break;
        }
      }

      if (tablet == null) {
        tablet = new Tablet(currentDevice, currentMeasurements, 1);
      }

      // Switch chunk reader iff current chunk is all consumed
      if (!data.hasCurrent()) {
        try {
          prepareData();
        } catch (final Exception e) {
          deferredException = e;
        }
      }
      PipeTabletUtils.compactBitMaps(tablet);
      return tablet;
    } catch (final Exception e) {
      close();
      throw new PipeException("Failed to get next tablet insertion event.", e);
    }
  }

  private void throwIfDeferredException() {
    if (Objects.isNull(deferredException)) {
      return;
    }

    final Exception exception = deferredException;
    deferredException = null;
    throw new PipeException("Failed to prepare next tablet insertion event.", exception);
  }

  private boolean isLastTabletWithoutDeferredException() {
    return Objects.isNull(deferredException) && Objects.isNull(chunkReader);
  }

  private void recordParseEndTimeIfNecessary() {
    if (parseStartTimeRecorded && !parseEndTimeRecorded) {
      recordParseEndTime();
    }
  }

  private void prepareData() throws IOException {
    do {
      do {
        moveToNextChunkReader();
      } while (Objects.nonNull(chunkReader) && !chunkReader.hasNextSatisfiedPage());

      if (Objects.isNull(chunkReader)) {
        close();
        break;
      }

      do {
        data = nextPageData();
      } while (!data.hasCurrent() && chunkReader.hasNextSatisfiedPage());
    } while (!data.hasCurrent());
  }

  private BatchData nextPageData() throws IOException {
    resizePageDataMemoryForCurrentPageIfNeeded();
    final BatchData nextData = chunkReader.nextPageData();
    resizePageDataMemoryIfNeeded(PipeMemoryWeightUtil.calculateBatchDataRamBytesUsed(nextData));
    return nextData;
  }

  private void resizePageDataMemoryForCurrentPageIfNeeded() {
    if (!(chunkReader instanceof EstimatedMemoryChunkReader)) {
      return;
    }

    final long estimatedMemoryUsageInBytes =
        ((EstimatedMemoryChunkReader) chunkReader).getCurrentPageEstimatedMemoryUsageInBytes();
    resizePageDataMemoryIfNeeded(estimatedMemoryUsageInBytes);
  }

  private void resizePageDataMemoryIfNeeded(final long estimatedMemoryUsageInBytes) {
    if (allocatedMemoryBlockForBatchData.getMemoryUsageInBytes() < estimatedMemoryUsageInBytes) {
      PipeDataNodeResourceManager.memory()
          .forceResize(allocatedMemoryBlockForBatchData, estimatedMemoryUsageInBytes);
    }
  }

  private boolean putValueToColumns(final BatchData data, final Tablet tablet, final int rowIndex) {
    boolean isNeedFillTime = false;
    if (data.getDataType() == TSDataType.VECTOR) {
      for (int i = 0; i < tablet.getSchemas().size(); ++i) {
        final TsPrimitiveType primitiveType = data.getVector()[i];
        final TSDataType type = tablet.getSchemas().get(i).getType();
        if (Objects.isNull(primitiveType)
            || ModsOperationUtil.isDelete(data.currentTime(), modsInfos.get(i))) {
          if (type == TSDataType.TEXT || type == TSDataType.BLOB || type == TSDataType.STRING) {
            PipeTabletUtils.putValue(tablet, rowIndex, i, type, Binary.EMPTY_VALUE);
          }
          PipeTabletUtils.markNullValue(tablet, rowIndex, i);
          continue;
        }

        isNeedFillTime = true;
        switch (type) {
          case BOOLEAN:
            PipeTabletUtils.putValue(tablet, rowIndex, i, type, primitiveType.getBoolean());
            break;
          case INT32:
            PipeTabletUtils.putValue(tablet, rowIndex, i, type, primitiveType.getInt());
            break;
          case DATE:
            PipeTabletUtils.putValue(
                tablet, rowIndex, i, type, DateUtils.parseIntToLocalDate(primitiveType.getInt()));
            break;
          case INT64:
          case TIMESTAMP:
            PipeTabletUtils.putValue(tablet, rowIndex, i, type, primitiveType.getLong());
            break;
          case FLOAT:
            PipeTabletUtils.putValue(tablet, rowIndex, i, type, primitiveType.getFloat());
            break;
          case DOUBLE:
            PipeTabletUtils.putValue(tablet, rowIndex, i, type, primitiveType.getDouble());
            break;
          case TEXT:
          case BLOB:
          case STRING:
            final Binary binary = primitiveType.getBinary();
            PipeTabletUtils.putValue(
                tablet,
                rowIndex,
                i,
                type,
                Objects.isNull(binary) || Objects.isNull(binary.getValues())
                    ? Binary.EMPTY_VALUE
                    : binary);
            break;
          default:
            throw new UnSupportedDataTypeException("UnSupported" + primitiveType.getDataType());
        }
      }
    } else {
      if (!modsInfos.isEmpty()
          && ModsOperationUtil.isDelete(data.currentTime(), modsInfos.get(0))) {
        return false;
      }

      isNeedFillTime = true;
      final TSDataType type = tablet.getSchemas().get(0).getType();
      switch (type) {
        case BOOLEAN:
          PipeTabletUtils.putValue(tablet, rowIndex, 0, type, data.getBoolean());
          break;
        case INT32:
          PipeTabletUtils.putValue(tablet, rowIndex, 0, type, data.getInt());
          break;
        case DATE:
          PipeTabletUtils.putValue(
              tablet, rowIndex, 0, type, DateUtils.parseIntToLocalDate(data.getInt()));
          break;
        case INT64:
        case TIMESTAMP:
          PipeTabletUtils.putValue(tablet, rowIndex, 0, type, data.getLong());
          break;
        case FLOAT:
          PipeTabletUtils.putValue(tablet, rowIndex, 0, type, data.getFloat());
          break;
        case DOUBLE:
          PipeTabletUtils.putValue(tablet, rowIndex, 0, type, data.getDouble());
          break;
        case TEXT:
        case BLOB:
        case STRING:
          final Binary binary = data.getBinary();
          PipeTabletUtils.putValue(
              tablet,
              rowIndex,
              0,
              type,
              Objects.isNull(binary) || Objects.isNull(binary.getValues())
                  ? Binary.EMPTY_VALUE
                  : binary);
          break;
        default:
          throw new UnSupportedDataTypeException("UnSupported" + data.getDataType());
      }
    }
    return isNeedFillTime;
  }

  private void moveToNextChunkReader() throws IOException, IllegalStateException {
    ChunkHeader chunkHeader;
    long valueChunkSize = 0;
    long valueChunkPageMemorySize = 0;
    final List<Chunk> valueChunkList = new ArrayList<>();
    currentMeasurements.clear();
    modsInfos.clear();

    if (lastMarker == MetaMarker.SEPARATOR) {
      chunkReader = null;
      return;
    }

    byte marker;
    while ((marker =
            lastMarker != Byte.MIN_VALUE
                ? lastMarker
                : Objects.nonNull(firstChunk4NextSequentialValueChunks)
                    ? toValueChunkMarker(firstChunk4NextSequentialValueChunks.getHeader())
                    : tsFileSequenceReader.readMarker())
        != MetaMarker.SEPARATOR) {
      lastMarker = Byte.MIN_VALUE;
      switch (marker) {
        case MetaMarker.CHUNK_HEADER:
        case MetaMarker.TIME_CHUNK_HEADER:
        case MetaMarker.ONLY_ONE_PAGE_CHUNK_HEADER:
        case MetaMarker.ONLY_ONE_PAGE_TIME_CHUNK_HEADER:
          {
            // Notice that the data in one chunk group is either aligned or non-aligned
            // There is no need to consider non-aligned chunks when there are value chunks
            currentIsMultiPage = marker == MetaMarker.CHUNK_HEADER;
            long currentChunkHeaderOffset = tsFileSequenceReader.position() - 1;
            chunkHeader = tsFileSequenceReader.readChunkHeader(marker);
            final long nextMarkerOffset =
                tsFileSequenceReader.position() + chunkHeader.getDataSize();

            if (Objects.isNull(currentDevice)) {
              tsFileSequenceReader.position(nextMarkerOffset);
              break;
            }

            if ((chunkHeader.getChunkType() & TsFileConstant.TIME_COLUMN_MASK)
                == TsFileConstant.TIME_COLUMN_MASK) {
              final Chunk timeChunk =
                  new Chunk(
                      chunkHeader, tsFileSequenceReader.readChunk(-1, chunkHeader.getDataSize()));
              final boolean isMultiPage = marker == MetaMarker.TIME_CHUNK_HEADER;
              timeChunkList.add(timeChunk);
              isMultiPageList.add(isMultiPage);
              timeChunkPageMemorySizeList.add(
                  SinglePageWholeChunkReader.calculateMaxPageEstimatedMemoryUsageInBytes(
                      timeChunk));
              break;
            }

            if (!pattern.matchesMeasurement(currentDevice, chunkHeader.getMeasurementID())) {
              tsFileSequenceReader.position(nextMarkerOffset);
              break;
            }

            // Skip the chunk if it is fully deleted by mods
            if (!currentModifications.isEmpty()) {
              Statistics statistics = null;
              try {
                statistics =
                    findNonAlignedChunkStatistics(
                        tsFileSequenceReader.getIChunkMetadataList(
                            CompactionPathUtils.getPath(
                                currentDevice, chunkHeader.getMeasurementID())),
                        currentChunkHeaderOffset);
              } catch (IllegalPathException ignore) {
                LOGGER.warn(
                    "Failed to get chunk metadata for {}.{}",
                    currentDevice,
                    chunkHeader.getMeasurementID());
              }

              if (statistics != null
                  && ModsOperationUtil.isAllDeletedByMods(
                      currentDevice,
                      chunkHeader.getMeasurementID(),
                      statistics.getStartTime(),
                      statistics.getEndTime(),
                      currentModifications)) {
                tsFileSequenceReader.position(nextMarkerOffset);
                break;
              }
            }

            if (chunkHeader.getDataSize() > allocatedMemoryBlockForChunk.getMemoryUsageInBytes()) {
              PipeDataNodeResourceManager.memory()
                  .forceResize(allocatedMemoryBlockForChunk, chunkHeader.getDataSize());
            }

            Chunk chunk =
                new Chunk(
                    chunkHeader, tsFileSequenceReader.readChunk(-1, chunkHeader.getDataSize()));
            final List<Long> pageEstimatedMemoryUsageInBytesList =
                SinglePageWholeChunkReader
                    .calculatePageEstimatedMemoryUsageInBytesWithBatchDataList(chunk);

            chunkReader =
                currentIsMultiPage
                    ? new MemoryControlledChunkReader(
                        new ChunkReader(chunk, filter), pageEstimatedMemoryUsageInBytesList)
                    : new SinglePageWholeChunkReader(chunk);
            currentIsAligned = false;
            final String measurementID =
                tabletStringInternPool.intern(chunkHeader.getMeasurementID());
            currentMeasurements.add(
                new MeasurementSchema(measurementID, chunkHeader.getDataType()));
            modsInfos.addAll(
                ModsOperationUtil.initializeMeasurementMods(
                    currentDevice, Collections.singletonList(measurementID), currentModifications));
            return;
          }
        case MetaMarker.VALUE_CHUNK_HEADER:
        case MetaMarker.ONLY_ONE_PAGE_VALUE_CHUNK_HEADER:
          {
            Chunk chunk;
            long currentValueChunkPageMemorySize = 0;
            if (Objects.isNull(firstChunk4NextSequentialValueChunks)) {
              final long currentChunkHeaderOffset = tsFileSequenceReader.position() - 1;
              chunkHeader = tsFileSequenceReader.readChunkHeader(marker);

              final long nextMarkerOffset =
                  tsFileSequenceReader.position() + chunkHeader.getDataSize();
              if (Objects.isNull(currentDevice)
                  || !pattern.matchesMeasurement(currentDevice, chunkHeader.getMeasurementID())) {
                tsFileSequenceReader.position(nextMarkerOffset);
                break;
              }

              if (!currentModifications.isEmpty()) {
                // Skip the chunk if it is fully deleted by mods
                Statistics statistics = null;
                try {
                  statistics =
                      findAlignedChunkStatistics(
                          tsFileSequenceReader.getIChunkMetadataList(
                              CompactionPathUtils.getPath(
                                  currentDevice, chunkHeader.getMeasurementID())),
                          currentChunkHeaderOffset);
                } catch (IllegalPathException ignore) {
                  LOGGER.warn(
                      "Failed to get chunk metadata for {}.{}",
                      currentDevice,
                      chunkHeader.getMeasurementID());
                }
                if (statistics != null
                    && ModsOperationUtil.isAllDeletedByMods(
                        currentDevice,
                        chunkHeader.getMeasurementID(),
                        statistics.getStartTime(),
                        statistics.getEndTime(),
                        currentModifications)) {
                  tsFileSequenceReader.position(nextMarkerOffset);
                  break;
                }
              }

              // Increase value index
              final String measurementID =
                  tabletStringInternPool.intern(chunkHeader.getMeasurementID());
              final int valueIndex =
                  measurementIndexMap.compute(
                      measurementID,
                      (measurement, index) -> Objects.nonNull(index) ? index + 1 : 0);

              // Emit when encountered non-sequential value chunk, or the chunk size exceeds
              // certain value to avoid OOM
              // Do not record or end current value chunks when there are empty chunks
              if (chunkHeader.getDataSize() == 0) {
                break;
              }
              chunk =
                  new Chunk(
                      chunkHeader, tsFileSequenceReader.readChunk(-1, chunkHeader.getDataSize()));
              currentValueChunkPageMemorySize = calculateMaxPageMemorySize(chunk);
              boolean needReturn = false;
              final long timeChunkSize =
                  lastIndex >= 0
                      ? PipeMemoryWeightUtil.calculateChunkRamBytesUsed(
                          timeChunkList.get(lastIndex))
                      : 0;
              final long timeChunkPageMemorySize =
                  lastIndex >= 0 ? timeChunkPageMemorySizeList.get(lastIndex) : 0;
              if (lastIndex >= 0) {
                if (valueIndex != lastIndex) {
                  needReturn = recordAlignedChunk(valueChunkList, marker);
                } else {
                  final long chunkSize = timeChunkSize + valueChunkSize;
                  final long pageMemorySize = timeChunkPageMemorySize + valueChunkPageMemorySize;
                  if (chunkSize + chunkHeader.getDataSize()
                          > allocatedMemoryBlockForChunk.getMemoryUsageInBytes()
                      || timeChunkPageMemorySize > 0
                          && currentValueChunkPageMemorySize > 0
                          && pageMemorySize + currentValueChunkPageMemorySize
                              > getPageDataMemoryLimitInBytes()) {
                    needReturn = recordAlignedChunk(valueChunkList, marker);
                  }
                }
              }
              lastIndex = valueIndex;
              if (needReturn) {
                firstChunk4NextSequentialValueChunks = chunk;
                return;
              }
              resizeChunkMemoryBlockIfFirstValueChunkExceedsLimit(valueChunkList, chunkHeader);
              resizePageDataMemoryBlockIfFirstValueChunkExceedsLimit(
                  valueChunkList, currentValueChunkPageMemorySize);
            } else {
              chunk = firstChunk4NextSequentialValueChunks;
              chunkHeader = chunk.getHeader();
              firstChunk4NextSequentialValueChunks = null;
              currentValueChunkPageMemorySize = calculateMaxPageMemorySize(chunk);
              resizeChunkMemoryBlockIfFirstValueChunkExceedsLimit(valueChunkList, chunkHeader);
              resizePageDataMemoryBlockIfFirstValueChunkExceedsLimit(
                  valueChunkList, currentValueChunkPageMemorySize);
            }

            valueChunkSize += chunkHeader.getDataSize();
            valueChunkPageMemorySize += currentValueChunkPageMemorySize;
            valueChunkList.add(chunk);
            final String measurementID =
                tabletStringInternPool.intern(chunkHeader.getMeasurementID());
            currentMeasurements.add(
                new MeasurementSchema(measurementID, chunkHeader.getDataType()));
            modsInfos.addAll(
                ModsOperationUtil.initializeMeasurementMods(
                    currentDevice, Collections.singletonList(measurementID), currentModifications));
            break;
          }
        case MetaMarker.CHUNK_GROUP_HEADER:
          {
            // Return before "currentDevice" changes
            if (recordAlignedChunk(valueChunkList, marker)) {
              return;
            }
            final String deviceID =
                ((PlainDeviceID) tsFileSequenceReader.readChunkGroupHeader().getDeviceID())
                    .toStringID();
            // Clear because the cached data will never be used in the next chunk group
            lastIndex = -1;
            timeChunkList.clear();
            isMultiPageList.clear();
            timeChunkPageMemorySizeList.clear();
            measurementIndexMap.clear();

            currentDevice =
                pattern.mayOverlapWithDevice(deviceID)
                    ? tabletStringInternPool.intern(deviceID)
                    : null;
            break;
          }
        case MetaMarker.OPERATION_INDEX_RANGE:
          {
            tsFileSequenceReader.readPlanIndex();
            break;
          }
        default:
          MetaMarker.handleUnexpectedMarker(marker);
      }
    }

    lastMarker = marker;
    if (!recordAlignedChunk(valueChunkList, marker)) {
      chunkReader = null;
    }
  }

  private long getPageDataMemoryLimitInBytes() {
    return PipeConfig.getInstance().getPipeMaxReaderChunkSize();
  }

  private boolean recordAlignedChunk(final List<Chunk> valueChunkList, final byte marker)
      throws IOException {
    if (!valueChunkList.isEmpty()) {
      final Chunk timeChunk = timeChunkList.get(lastIndex);
      timeChunk.getData().rewind();
      currentIsMultiPage = isMultiPageList.get(lastIndex);
      if (!currentIsMultiPage) {
        resizePageDataMemoryIfNeeded(
            AlignedSinglePageWholeChunkReader.calculatePageEstimatedMemoryUsageInBytes(
                timeChunk, valueChunkList));
      }
      final List<Long> pageEstimatedMemoryUsageInBytesList =
          currentIsMultiPage
              ? AlignedSinglePageWholeChunkReader
                  .calculatePageEstimatedMemoryUsageInBytesWithBatchDataList(
                      timeChunk, valueChunkList)
              : Collections.emptyList();
      final long maxPageEstimatedMemoryUsageInBytes =
          pageEstimatedMemoryUsageInBytesList.isEmpty()
              ? 0
              : pageEstimatedMemoryUsageInBytesList.get(0);
      resizePageDataMemoryIfNeeded(maxPageEstimatedMemoryUsageInBytes);
      chunkReader =
          currentIsMultiPage
              ? new MemoryControlledChunkReader(
                  new AlignedChunkReader(timeChunk, valueChunkList, filter),
                  pageEstimatedMemoryUsageInBytesList)
              : new AlignedSinglePageWholeChunkReader(timeChunk, valueChunkList);
      currentIsAligned = true;
      lastMarker = marker;
      return true;
    }
    return false;
  }

  private void resizeChunkMemoryBlockIfFirstValueChunkExceedsLimit(
      final List<Chunk> valueChunkList, final ChunkHeader valueChunkHeader) {
    if (!valueChunkList.isEmpty() || lastIndex < 0) {
      return;
    }

    final long chunkSize =
        PipeMemoryWeightUtil.calculateChunkRamBytesUsed(timeChunkList.get(lastIndex))
            + valueChunkHeader.getDataSize();
    if (chunkSize > allocatedMemoryBlockForChunk.getMemoryUsageInBytes()) {
      PipeDataNodeResourceManager.memory().forceResize(allocatedMemoryBlockForChunk, chunkSize);
    }
  }

  private void resizePageDataMemoryBlockIfFirstValueChunkExceedsLimit(
      final List<Chunk> valueChunkList, final long valueChunkPageMemorySize) {
    if (!valueChunkList.isEmpty() || lastIndex < 0 || valueChunkPageMemorySize <= 0) {
      return;
    }

    final long timeChunkPageMemorySize = timeChunkPageMemorySizeList.get(lastIndex);
    if (timeChunkPageMemorySize <= 0) {
      return;
    }

    final long pageMemorySize = timeChunkPageMemorySize + valueChunkPageMemorySize;
    if (pageMemorySize > getPageDataMemoryLimitInBytes()) {
      PipeDataNodeResourceManager.memory()
          .forceResize(allocatedMemoryBlockForBatchData, pageMemorySize);
    }
  }

  private long calculateMaxPageMemorySize(final Chunk chunk) throws IOException {
    return SinglePageWholeChunkReader.calculateMaxPageEstimatedMemoryUsageInBytes(chunk);
  }

  private boolean isSinglePageValueChunk(final ChunkHeader chunkHeader) {
    return (chunkHeader.getChunkType() & 0x3F) == MetaMarker.ONLY_ONE_PAGE_CHUNK_HEADER;
  }

  private byte toValueChunkMarker(final ChunkHeader chunkHeader) {
    return isSinglePageValueChunk(chunkHeader)
        ? MetaMarker.ONLY_ONE_PAGE_VALUE_CHUNK_HEADER
        : MetaMarker.VALUE_CHUNK_HEADER;
  }

  @Override
  public void close() {
    super.close();

    if (allocatedMemoryBlockForBatchData != null) {
      allocatedMemoryBlockForBatchData.close();
    }

    if (allocatedMemoryBlockForChunk != null) {
      allocatedMemoryBlockForChunk.close();
    }
  }

  private Statistics findAlignedChunkStatistics(
      List<IChunkMetadata> metadataList, long currentChunkHeaderOffset) {
    for (IChunkMetadata metadata : metadataList) {
      if (!(metadata instanceof AlignedChunkMetadata)) {
        continue;
      }
      List<IChunkMetadata> list = ((AlignedChunkMetadata) metadata).getValueChunkMetadataList();
      for (IChunkMetadata m : list) {
        if (m.getOffsetOfChunkHeader() == currentChunkHeaderOffset) {
          return m.getStatistics();
        }
      }
      break;
    }
    return null;
  }

  private Statistics findNonAlignedChunkStatistics(
      List<IChunkMetadata> metadataList, long currentChunkHeaderOffset) {
    for (IChunkMetadata metadata : metadataList) {
      if (metadata.getOffsetOfChunkHeader() == currentChunkHeaderOffset) {
        // found the corresponding chunk metadata
        return metadata.getStatistics();
      }
    }
    return null;
  }
}
