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

    for (TimeseriesMetadata timeseriesMetadata : currentEntry.getValue()) {
      MeasurementSchema measurementSchema =
          new MeasurementSchema(
              timeseriesMetadata.getMeasurementId(), timeseriesMetadata.getTSDataType());
      measurementSchemas.add(measurementSchema);
      List<Byte> bitMapBytes = new ArrayList<>();
      List<long[]> measurementTimestamps = new ArrayList<>();
      List<Object> measurementValues = new ArrayList<>();

      List<IChunkMetadata> chunkMetadataList = timeseriesMetadata.getChunkMetadataList();
      for (IChunkMetadata chunkMetadata : chunkMetadataList) {
        long offset = chunkMetadata.getOffsetOfChunkHeader();
        try {
          reader.position(offset);
          ChunkHeader header = reader.readChunkHeader(reader.readMarker());
          int dataSize = header.getDataSize();
          int pageIndex = 0;

          Decoder defaultTimeDecoder =
              Decoder.getDecoderByType(
                  TSEncoding.valueOf(TSFileDescriptor.getInstance().getConfig().getTimeEncoder()),
                  TSDataType.INT64);
          Decoder valueDecoder =
              Decoder.getDecoderByType(header.getEncodingType(), header.getDataType());

          while (dataSize > 0) {
            PageHeader pageHeader =
                reader.readPageHeader(
                    header.getDataType(),
                    (header.getChunkType() & 0x3F) == MetaMarker.CHUNK_HEADER);
            ByteBuffer pageData = reader.readPage(pageHeader, header.getCompressionType());

            // Time column chunk
            if ((header.getChunkType() & TsFileConstant.TIME_COLUMN_MASK)
                == TsFileConstant.TIME_COLUMN_MASK) {
              TimePageReader timePageReader =
                  new TimePageReader(pageHeader, pageData, defaultTimeDecoder);
              measurementTimestamps.add(timePageReader.nextTimeBatch());
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
                  valuePageReader.nextValueBatch(measurementTimestamps.get(pageIndex))) {
                measurementValues.add(value.getValue());
              }
            }

            dataSize -= pageHeader.getSerializedPageSize();
            pageIndex++;
          }

        } catch (IOException e) {
          throw new UncheckedIOException(e);
        }
      }

      // Fill in the timestamps, values, and bitMap with the information for this measurement.
      // timestamps
      for (long[] time : measurementTimestamps) {
        for (long t : time) {
          timestamps.add(t);
        }
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
    return new Tablet(currentEntry.getKey(), measurementSchemas);
  }
}
