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

import org.apache.iotdb.commons.pipe.config.PipeConfig;
import org.apache.iotdb.commons.pipe.event.EnrichedEvent;
import org.apache.iotdb.commons.pipe.pattern.PipePattern;
import org.apache.iotdb.commons.pipe.task.meta.PipeTaskMeta;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeRawTabletInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.tsfile.container.TsFileInsertionDataContainer;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;
import org.apache.iotdb.pipe.api.exception.PipeException;

import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.common.constant.TsFileConstant;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.MetaMarker;
import org.apache.tsfile.file.header.ChunkHeader;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.read.common.BatchData;
import org.apache.tsfile.read.common.Chunk;
import org.apache.tsfile.read.filter.basic.Filter;
import org.apache.tsfile.read.reader.IChunkReader;
import org.apache.tsfile.read.reader.chunk.AlignedChunkReader;
import org.apache.tsfile.read.reader.chunk.ChunkReader;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.DateUtils;
import org.apache.tsfile.utils.TsPrimitiveType;
import org.apache.tsfile.write.UnSupportedDataTypeException;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;

import java.io.File;
import java.io.IOException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;

public class TsFileInsertionScanDataContainer extends TsFileInsertionDataContainer {

  private final long startTime;
  private final long endTime;
  private final Filter filter;

  private IChunkReader chunkReader;
  private BatchData data;

  private boolean isMultiPage;
  private IDeviceID currentDevice;
  private boolean currentIsAligned;
  private final List<IMeasurementSchema> currentMeasurements = new ArrayList<>();

  private byte lastMarker = Byte.MIN_VALUE;

  public TsFileInsertionScanDataContainer(
      final File tsFile,
      final PipePattern pattern,
      final long startTime,
      final long endTime,
      final PipeTaskMeta pipeTaskMeta,
      final EnrichedEvent sourceEvent)
      throws IOException {
    super(pattern, startTime, endTime, pipeTaskMeta, sourceEvent);

    this.startTime = startTime;
    this.endTime = endTime;
    filter = Objects.nonNull(timeFilterExpression) ? timeFilterExpression.getFilter() : null;

    try {
      tsFileSequenceReader = new TsFileSequenceReader(tsFile.getAbsolutePath(), false, false);
      tsFileSequenceReader.position((long) TSFileConfig.MAGIC_STRING.getBytes().length + 1);

      prepareData();
    } catch (final Exception e) {
      close();
      throw e;
    }
  }

  @Override
  public Iterable<TabletInsertionEvent> toTabletInsertionEvents() {
    return () ->
        new Iterator<TabletInsertionEvent>() {

          @Override
          public boolean hasNext() {
            return Objects.nonNull(chunkReader);
          }

          @Override
          public TabletInsertionEvent next() {
            if (!hasNext()) {
              close();
              throw new NoSuchElementException();
            }

            final Tablet tablet = getNextTablet();
            final boolean hasNext = hasNext();
            try {
              return new PipeRawTabletInsertionEvent(
                  tablet,
                  currentIsAligned,
                  sourceEvent != null ? sourceEvent.getPipeName() : null,
                  sourceEvent != null ? sourceEvent.getCreationTime() : 0,
                  pipeTaskMeta,
                  sourceEvent,
                  !hasNext);
            } finally {
              if (!hasNext) {
                close();
              }
            }
          }
        };
  }

  private Tablet getNextTablet() {
    try {
      final Tablet tablet =
          new Tablet(
              currentDevice.toString(),
              currentMeasurements,
              PipeConfig.getInstance().getPipeDataStructureTabletRowSize());
      tablet.initBitMaps();

      while (data.hasCurrent()) {
        if (isMultiPage || data.currentTime() >= startTime && data.currentTime() <= endTime) {
          final int rowIndex = tablet.rowSize;

          tablet.addTimestamp(rowIndex, data.currentTime());
          putValueToColumns(data, tablet.values, rowIndex);

          tablet.rowSize++;
        }

        data.next();
        while (!data.hasCurrent() && chunkReader.hasNextSatisfiedPage()) {
          data = chunkReader.nextPageData();
        }

        if (tablet.rowSize == tablet.getMaxRowNumber()) {
          break;
        }
      }

      // Switch chunk reader iff current chunk is all consumed
      if (!data.hasCurrent()) {
        prepareData();
      }

      return tablet;
    } catch (final Exception e) {
      close();
      throw new PipeException("Failed to get next tablet insertion event.", e);
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
        data = chunkReader.nextPageData();
      } while (!data.hasCurrent() && chunkReader.hasNextSatisfiedPage());
    } while (!data.hasCurrent());
  }

  private void putValueToColumns(final BatchData data, final Object[] columns, final int rowIndex) {
    final TSDataType type = data.getDataType();
    if (type == TSDataType.VECTOR) {
      for (int i = 0; i < columns.length; ++i) {
        final TsPrimitiveType primitiveType = data.getVector()[i];
        switch (primitiveType.getDataType()) {
          case BOOLEAN:
            ((boolean[]) columns[i])[rowIndex] = primitiveType.getBoolean();
            break;
          case INT32:
            ((int[]) columns[i])[rowIndex] = primitiveType.getInt();
            break;
          case DATE:
            ((LocalDate[]) columns[i])[rowIndex] =
                DateUtils.parseIntToLocalDate(primitiveType.getInt());
            break;
          case INT64:
          case TIMESTAMP:
            ((long[]) columns[i])[rowIndex] = primitiveType.getLong();
            break;
          case FLOAT:
            ((float[]) columns[i])[rowIndex] = primitiveType.getFloat();
            break;
          case DOUBLE:
            ((double[]) columns[i])[rowIndex] = primitiveType.getDouble();
            break;
          case TEXT:
          case BLOB:
          case STRING:
            ((Binary[]) columns[i])[rowIndex] = primitiveType.getBinary();
            break;
          default:
            throw new UnSupportedDataTypeException("UnSupported" + primitiveType.getDataType());
        }
      }
    } else {
      switch (type) {
        case BOOLEAN:
          ((boolean[]) columns[0])[rowIndex] = data.getBoolean();
          break;
        case INT32:
          ((int[]) columns[0])[rowIndex] = data.getInt();
          break;
        case DATE:
          ((LocalDate[]) columns[0])[rowIndex] = DateUtils.parseIntToLocalDate(data.getInt());
          break;
        case INT64:
        case TIMESTAMP:
          ((long[]) columns[0])[rowIndex] = data.getLong();
          break;
        case FLOAT:
          ((float[]) columns[0])[rowIndex] = data.getFloat();
          break;
        case DOUBLE:
          ((double[]) columns[0])[rowIndex] = data.getDouble();
          break;
        case TEXT:
        case BLOB:
        case STRING:
          ((Binary[]) columns[0])[rowIndex] = data.getBinary();
          break;
        default:
          throw new UnSupportedDataTypeException("UnSupported" + data.getDataType());
      }
    }
  }

  private void moveToNextChunkReader() throws IOException, IllegalStateException {
    ChunkHeader chunkHeader;
    Chunk timeChunk = null;
    final List<Chunk> valueChunkList = new ArrayList<>();
    currentMeasurements.clear();

    if (lastMarker == MetaMarker.SEPARATOR) {
      chunkReader = null;
      return;
    }

    byte marker;
    while ((marker = lastMarker != Byte.MIN_VALUE ? lastMarker : tsFileSequenceReader.readMarker())
        != MetaMarker.SEPARATOR) {
      lastMarker = Byte.MIN_VALUE;
      switch (marker) {
        case MetaMarker.CHUNK_HEADER:
        case MetaMarker.TIME_CHUNK_HEADER:
        case MetaMarker.ONLY_ONE_PAGE_CHUNK_HEADER:
        case MetaMarker.ONLY_ONE_PAGE_TIME_CHUNK_HEADER:
          if (Objects.nonNull(timeChunk) && !currentMeasurements.isEmpty()) {
            chunkReader =
                isMultiPage
                    ? new AlignedChunkReader(timeChunk, valueChunkList, filter)
                    : new AlignedSinglePageWholeChunkReader(timeChunk, valueChunkList);
            currentIsAligned = true;
            lastMarker = marker;
            return;
          }

          isMultiPage = marker == MetaMarker.CHUNK_HEADER || marker == MetaMarker.TIME_CHUNK_HEADER;

          chunkHeader = tsFileSequenceReader.readChunkHeader(marker);

          if (Objects.isNull(currentDevice)) {
            tsFileSequenceReader.position(
                tsFileSequenceReader.position() + chunkHeader.getDataSize());
            break;
          }

          if ((chunkHeader.getChunkType() & TsFileConstant.TIME_COLUMN_MASK)
              == TsFileConstant.TIME_COLUMN_MASK) {
            timeChunk =
                new Chunk(
                    chunkHeader, tsFileSequenceReader.readChunk(-1, chunkHeader.getDataSize()));
            break;
          }

          if (!pattern.matchesMeasurement(currentDevice, chunkHeader.getMeasurementID())) {
            tsFileSequenceReader.position(
                tsFileSequenceReader.position() + chunkHeader.getDataSize());
            break;
          }

          chunkReader =
              isMultiPage
                  ? new ChunkReader(
                      new Chunk(
                          chunkHeader,
                          tsFileSequenceReader.readChunk(-1, chunkHeader.getDataSize())),
                      filter)
                  : new SinglePageWholeChunkReader(
                      new Chunk(
                          chunkHeader,
                          tsFileSequenceReader.readChunk(-1, chunkHeader.getDataSize())));
          currentIsAligned = false;
          currentMeasurements.add(
              new MeasurementSchema(chunkHeader.getMeasurementID(), chunkHeader.getDataType()));
          return;
        case MetaMarker.VALUE_CHUNK_HEADER:
        case MetaMarker.ONLY_ONE_PAGE_VALUE_CHUNK_HEADER:
          chunkHeader = tsFileSequenceReader.readChunkHeader(marker);

          if (Objects.isNull(currentDevice)
              || !pattern.matchesMeasurement(currentDevice, chunkHeader.getMeasurementID())) {
            tsFileSequenceReader.position(
                tsFileSequenceReader.position() + chunkHeader.getDataSize());
            break;
          }

          // Do not record empty chunk
          if (chunkHeader.getDataSize() > 0) {
            valueChunkList.add(
                new Chunk(
                    chunkHeader, tsFileSequenceReader.readChunk(-1, chunkHeader.getDataSize())));
            currentMeasurements.add(
                new MeasurementSchema(chunkHeader.getMeasurementID(), chunkHeader.getDataType()));
          }
          break;
        case MetaMarker.CHUNK_GROUP_HEADER:
          // Return before "currentDevice" changes
          if (Objects.nonNull(timeChunk) && !currentMeasurements.isEmpty()) {
            chunkReader =
                isMultiPage
                    ? new AlignedChunkReader(timeChunk, valueChunkList, filter)
                    : new AlignedSinglePageWholeChunkReader(timeChunk, valueChunkList);
            currentIsAligned = true;
            lastMarker = marker;
            return;
          }
          final IDeviceID deviceID = tsFileSequenceReader.readChunkGroupHeader().getDeviceID();
          currentDevice = pattern.mayOverlapWithDevice(deviceID) ? deviceID : null;
          break;
        case MetaMarker.OPERATION_INDEX_RANGE:
          tsFileSequenceReader.readPlanIndex();
          break;
        default:
          MetaMarker.handleUnexpectedMarker(marker);
      }
    }

    lastMarker = marker;
    if (Objects.nonNull(timeChunk) && !currentMeasurements.isEmpty()) {
      chunkReader =
          isMultiPage
              ? new AlignedChunkReader(timeChunk, valueChunkList, filter)
              : new AlignedSinglePageWholeChunkReader(timeChunk, valueChunkList);
      currentIsAligned = true;
    } else {
      chunkReader = null;
    }
  }
}
