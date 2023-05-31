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
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.reader.page.PageReader;
import org.apache.iotdb.tsfile.read.reader.page.TimePageReader;
import org.apache.iotdb.tsfile.read.reader.page.ValuePageReader;
import org.apache.iotdb.tsfile.utils.BitMap;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

public class TsFileInsertionDataTabletIterator implements Iterator<Tablet> {
  //  private final TsFileSequenceReader reader;
  private final String filePath;
  private final Iterator<Map.Entry<String, List<TimeseriesMetadata>>> entriesIterator;
  private Map.Entry<String, List<TimeseriesMetadata>> currentEntry;
  private Iterator<TimeseriesMetadata> timeseriesMetadataIterator;
  private TimeseriesMetadata currentTimeseriesMetadata;
  private List<MeasurementSchema> measurementSchemas;

  private boolean isAligned;
  private List<long[]> timeBatches;
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

    // Initialize timeseriesMetadataIterator if there is a next entry
    if (entriesIterator.hasNext()) {
      currentEntry = entriesIterator.next();
      timeseriesMetadataIterator = currentEntry.getValue().iterator();
    }
  }

  @Override
  public boolean hasNext() {
    return timeseriesMetadataIterator.hasNext() || entriesIterator.hasNext();
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

    if (currentTimeseriesMetadata.getTSDataType() == TSDataType.VECTOR) {
      processTimeseriesMetadata(currentTimeseriesMetadata);
      currentTimeseriesMetadata = timeseriesMetadataIterator.next();
    }
    return processTimeseriesMetadata(currentTimeseriesMetadata);
  }

  private Tablet createTablet(long[] timestamps, Object[] values, BitMap[] bitMaps) {
    // create tablet
    Tablet tablet = new Tablet(currentEntry.getKey(), measurementSchemas);

    if (isAligned) {
      if (timestampsForAligned == null) {
        timestampsForAligned = timestamps;
      }
      tablet.timestamps = timestampsForAligned;
    } else {
      tablet.timestamps = timestamps;
    }

    tablet.values = values;
    tablet.rowSize = isAligned ? timestampsForAligned.length : timestamps.length;
    tablet.bitMaps = bitMaps;
    return tablet;
  }

  private Tablet processTimeseriesMetadata(TimeseriesMetadata timeseriesMetadata) {
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
    BitMap[] bitMaps = new BitMap[] {new BitMap(bitMapBytes.size() * 8, byteArray)};

    return createTablet(timestamps, measurementValues.toArray(), bitMaps);
  }
}
