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

package org.apache.iotdb.db.pipe.core.event.utils;

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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

public class TabletIterator implements Iterator<Tablet> {
  private final TsFileSequenceReader reader;
  private final Iterator<Map.Entry<String, List<TimeseriesMetadata>>> entriesIterator;
  private Map.Entry<String, List<TimeseriesMetadata>> currentEntry;
  private List<MeasurementSchema> measurementSchemas;
  private List<Object> values;
  private List<BitMap> bitMap;
  private List<Long> timestamps;

  private boolean isSetTimestamp;

  public TabletIterator(
      TsFileSequenceReader reader,
      Map<String, List<TimeseriesMetadata>> device2TimeseriesMetadataMap) {
    this.reader = reader;
    this.entriesIterator = device2TimeseriesMetadataMap.entrySet().iterator();
    this.currentEntry = null;
    this.measurementSchemas = null;
    this.values = null;
    this.bitMap = null;
    this.timestamps = null;
    this.isSetTimestamp = false;
  }

  @Override
  public boolean hasNext() {
    return entriesIterator.hasNext();
  }

  @Override
  public Tablet next() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }

    currentEntry = entriesIterator.next();
    measurementSchemas = new ArrayList<>();
    values = new ArrayList<>();
    bitMap = new ArrayList<>();
    timestamps = new ArrayList<>();
    List<long[]> timeBatches = new ArrayList<>();

    for (TimeseriesMetadata timeseriesMetadata : currentEntry.getValue()) {
      processTimeseriesMetadata(timeseriesMetadata, timeBatches);
    }

    return createTablet();
  }

  private Tablet createTablet() {
    // create tablet
    Tablet tablet = new Tablet(currentEntry.getKey(), measurementSchemas);
    tablet.timestamps = timestamps.stream().mapToLong(Long::longValue).toArray();
    tablet.values = values.toArray();
    tablet.rowSize = tablet.timestamps.length;

    BitMap[] bitMapArray = new BitMap[bitMap.size()];
    for (int i = 0; i < bitMap.size(); i++) {
      bitMapArray[i] = bitMap.get(i);
    }
    tablet.bitMaps = bitMapArray;
    return tablet;
  }

  private void processTimeseriesMetadata(
      TimeseriesMetadata timeseriesMetadata, List<long[]> timeBatches) {
    int pageIndex = 0;
    if (timeseriesMetadata.getTSDataType() == TSDataType.VECTOR) {
      timeBatches.clear();
    } else {
      MeasurementSchema measurementSchema =
          new MeasurementSchema(
              timeseriesMetadata.getMeasurementId(), timeseriesMetadata.getTSDataType());
      measurementSchemas.add(measurementSchema);
    }

    List<Byte> bitMapBytes = new ArrayList<>();
    List<Object> measurementValues = new ArrayList<>();

    List<IChunkMetadata> chunkMetadataList = timeseriesMetadata.getChunkMetadataList();

    for (IChunkMetadata chunkMetadata : chunkMetadataList) {
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
              timestamps.add(batchData.currentTime());
              Object value = batchData.currentValue();

              if (value == null) {
                isNullList.add(index);
              }
              measurementValues.add(value);
              index++;
              batchData.next();
            }

            BitMap bitmap = new BitMap(bitMap.size());
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

    // Fill in the timestamps, values, and bitMap with the information for this measurement.
    if (!isSetTimestamp) {
      for (long[] timeBatch : timeBatches) {
        for (long time : timeBatch) {
          timestamps.add(time);
        }
      }
      isSetTimestamp = true;
    }

    // values
    values.add(measurementValues.toArray());

    // bitMap
    byte[] byteArray = new byte[bitMapBytes.size()];
    for (int i = 0; i < bitMapBytes.size(); i++) {
      byteArray[i] = bitMapBytes.get(i);
    }
    bitMap.add(new BitMap(bitMapBytes.size() * 8, byteArray));
  }
}
