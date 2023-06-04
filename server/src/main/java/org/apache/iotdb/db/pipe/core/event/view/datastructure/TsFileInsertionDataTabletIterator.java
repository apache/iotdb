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

package org.apache.iotdb.db.pipe.core.event.view.datastructure;

import org.apache.iotdb.pipe.api.exception.PipeException;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;
import org.apache.iotdb.tsfile.encoding.decoder.Decoder;
import org.apache.iotdb.tsfile.file.MetaMarker;
import org.apache.iotdb.tsfile.file.header.ChunkHeader;
import org.apache.iotdb.tsfile.file.header.PageHeader;
import org.apache.iotdb.tsfile.file.metadata.IChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.TimeseriesMetadata;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.reader.page.PageReader;
import org.apache.iotdb.tsfile.read.reader.page.TimePageReader;
import org.apache.iotdb.tsfile.read.reader.page.ValuePageReader;
import org.apache.iotdb.tsfile.utils.BitMap;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

public class TsFileInsertionDataTabletIterator implements Iterator<Tablet> {

  private static Logger LOGGER = LoggerFactory.getLogger(TsFileInsertionDataTabletIterator.class);
  private final TsFileSequenceReader reader;
  private final String filePath;
  private final Iterator<Map.Entry<String, List<TimeseriesMetadata>>> entriesIterator;
  private Map.Entry<String, List<TimeseriesMetadata>> currentEntry;
  private Iterator<TimeseriesMetadata> timeseriesMetadataIterator;
  private TimeseriesMetadata currentTimeseriesMetadata;
  private List<MeasurementSchema> measurementSchemas;

  private boolean isAligned;
  private final List<long[]> timeBatches;
  private long[] timestampsForAligned;

  public TsFileInsertionDataTabletIterator(
      String filePath, Map<String, List<TimeseriesMetadata>> device2TimeseriesMetadataMap) {
    this.filePath = filePath;
    this.entriesIterator = device2TimeseriesMetadataMap.entrySet().iterator();
    this.timeBatches = new ArrayList<>();
    this.currentEntry = null;
    this.timeseriesMetadataIterator = null;
    this.currentTimeseriesMetadata = null;
    this.measurementSchemas = null;
    this.isAligned = false;
    this.timestampsForAligned = null;
    try {
      this.reader = new TsFileSequenceReader(filePath);
    } catch (IOException e) {
      throw new PipeException("Cannot create TsFileSequenceReader for file " + filePath, e);
    }

    // Initialize timeseriesMetadataIterator if there is a next entry
    if (entriesIterator.hasNext()) {
      currentEntry = entriesIterator.next();
      timeseriesMetadataIterator = currentEntry.getValue().iterator();
    } else {
      timeseriesMetadataIterator =
          new Iterator<TimeseriesMetadata>() {
            @Override
            public boolean hasNext() {
              return false;
            }

            @Override
            public TimeseriesMetadata next() {
              return null;
            }
          };
    }
  }

  @Override
  public boolean hasNext() {
    boolean hasNext = timeseriesMetadataIterator.hasNext() || entriesIterator.hasNext();
    if (!hasNext) {
      try {
        reader.close();
      } catch (IOException e) {
        LOGGER.warn("Cannot close TsFileSequenceReader for file {}", filePath, e);
      }
    }
    return hasNext;
  }

  @Override
  public Tablet next() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }

    if (!timeseriesMetadataIterator.hasNext()) {
      currentEntry = entriesIterator.next();
      timeseriesMetadataIterator = currentEntry.getValue().iterator();
    }
    currentTimeseriesMetadata = timeseriesMetadataIterator.next();
    measurementSchemas = new ArrayList<>();

    try (TsFileSequenceReader reader = new TsFileSequenceReader(filePath)) {
      if (currentTimeseriesMetadata.getTSDataType() == TSDataType.VECTOR) {
        processTimeseriesMetadata(currentTimeseriesMetadata, reader);
        currentTimeseriesMetadata = timeseriesMetadataIterator.next();
      }
      return processTimeseriesMetadata(currentTimeseriesMetadata, reader);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private Tablet createTablet(long[] timestamps, Object[] values, BitMap[] bitMaps) {
    long[] tmp;

    if (isAligned) {
      if (timestampsForAligned == null) {
        timestampsForAligned = timestamps;
        return null;
      }
      tmp = timestampsForAligned;
    } else {
      tmp = timestamps;
    }

    // create tablet
    int rowSize = tmp.length;
    Tablet tablet = new Tablet(currentEntry.getKey(), measurementSchemas, rowSize);
    tablet.timestamps = tmp;
    tablet.values = values;
    tablet.rowSize = rowSize;
    tablet.bitMaps = bitMaps;

    return tablet;
  }

  private Tablet processTimeseriesMetadata(
      TimeseriesMetadata timeseriesMetadata, TsFileSequenceReader reader) {
    int pageIndex = 0;
    if (timeseriesMetadata.getTSDataType() == TSDataType.VECTOR) {
      isAligned = true;
      timeBatches.clear();
    } else {
      MeasurementSchema measurementSchema =
          new MeasurementSchema(
              timeseriesMetadata.getMeasurementId(), timeseriesMetadata.getTSDataType());
      measurementSchemas.add(measurementSchema);
    }

    List<Byte> bitMapBytes = new ArrayList<>();
    List<Object> measurementValues = new ArrayList<>();
    List<Long> measurementTimestamps = new ArrayList<>();

    for (IChunkMetadata chunkMetadata : timeseriesMetadata.getChunkMetadataList()) {
      long offset = chunkMetadata.getOffsetOfChunkHeader();
      try {
        reader.position(offset);
        ChunkHeader header = reader.readChunkHeader(reader.readMarker());
        int dataSize = header.getDataSize();

        Decoder defaultTimeDecoder =
            Decoder.getDecoderByType(
                TSEncoding.valueOf(TSFileDescriptor.getInstance().getConfig().getTimeEncoder()),
                TSDataType.INT64);
        Decoder valueDecoder =
            Decoder.getDecoderByType(header.getEncodingType(), header.getDataType());
        pageIndex = 0;
        if (header.getDataType() == TSDataType.VECTOR) {
          timeBatches.clear();
        }

        while (dataSize > 0) {
          PageHeader pageHeader =
              reader.readPageHeader(
                  header.getDataType(), (header.getChunkType() & 0x3F) == MetaMarker.CHUNK_HEADER);
          ByteBuffer pageData = reader.readPage(pageHeader, header.getCompressionType());

          // Time column chunk
          if ((header.getChunkType() & TsFileConstant.TIME_COLUMN_MASK)
              == TsFileConstant.TIME_COLUMN_MASK) {
            TimePageReader timePageReader =
                new TimePageReader(pageHeader, pageData, defaultTimeDecoder);
            long[] timeBatch = timePageReader.getNextTimeBatch();
            timeBatches.add(timeBatch);

            for (long time : timeBatch) {
              measurementTimestamps.add(time);
            }
          }
          // Value column chunk
          else if ((header.getChunkType() & TsFileConstant.VALUE_COLUMN_MASK)
              == TsFileConstant.VALUE_COLUMN_MASK) {
            ValuePageReader valuePageReader =
                new ValuePageReader(pageHeader, pageData, header.getDataType(), valueDecoder);

            for (byte value : valuePageReader.getBitmap()) {
              bitMapBytes.add(value);
            }

            for (TsPrimitiveType value :
                valuePageReader.nextValueBatch(timeBatches.get(pageIndex))) {
              measurementValues.add(value.getValue());
            }
          }

          // NonAligned Chunk
          else {
            PageReader pageReader =
                new PageReader(
                    pageData, header.getDataType(), valueDecoder, defaultTimeDecoder, null);
            BatchData batchData = pageReader.getAllSatisfiedPageData();
            List<Integer> isNullList = new ArrayList<>();
            int index = 0;
            while (batchData.hasCurrent()) {
              measurementTimestamps.add(batchData.currentTime());
              Object value = batchData.currentValue();

              if (value == null) {
                isNullList.add(index);
              }
              measurementValues.add(value);
              index++;
              batchData.next();
            }

            BitMap bitmap = new BitMap(measurementTimestamps.size());
            for (int isNull : isNullList) {
              bitmap.mark(isNull);
            }
            byte[] bytes = bitmap.getByteArray();
            for (byte value : bytes) {
              bitMapBytes.add(value);
            }
          }
          pageIndex++;
          dataSize -= pageHeader.getSerializedPageSize();
        }
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }

    long[] timestamps = new long[measurementTimestamps.size()];
    for (int i = 0; i < measurementTimestamps.size(); i++) {
      timestamps[i] = measurementTimestamps.get(i);
    }

    byte[] byteArray = new byte[bitMapBytes.size()];
    for (int i = 0; i < bitMapBytes.size(); i++) {
      byteArray[i] = bitMapBytes.get(i);
    }
    BitMap[] bitMaps = new BitMap[] {new BitMap(byteArray.length, byteArray)};

    return createTablet(timestamps, measurementValues.toArray(), bitMaps);
  }
}
