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
package org.apache.iotdb.tsfile.write.chunk;

import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.record.datapoint.DataPoint;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.writer.TsFileIOWriter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** a implementation of IChunkGroupWriter. */
public class NonAlignedChunkGroupWriterImpl implements IChunkGroupWriter {

  private static final Logger LOG = LoggerFactory.getLogger(NonAlignedChunkGroupWriterImpl.class);

  private final String deviceId;

  /** Map(measurementID, ChunkWriterImpl). Aligned measurementId is empty. */
  private Map<String, ChunkWriterImpl> chunkWriters = new LinkedHashMap<>();

  private Map<String, Long> lastTimeMap = new HashMap<>();

  public NonAlignedChunkGroupWriterImpl(String deviceId) {
    this.deviceId = deviceId;
  }

  @Override
  public void tryToAddSeriesWriter(IMeasurementSchema schema) {
    if (!chunkWriters.containsKey(schema.getMeasurementId())) {
      this.chunkWriters.put(schema.getMeasurementId(), new ChunkWriterImpl(schema));
    }
  }

  @Override
  public void tryToAddSeriesWriter(List<IMeasurementSchema> schemas) {
    for (IMeasurementSchema schema : schemas) {
      if (!chunkWriters.containsKey(schema.getMeasurementId())) {
        this.chunkWriters.put(schema.getMeasurementId(), new ChunkWriterImpl(schema));
      }
    }
  }

  @Override
  public int write(long time, List<DataPoint> data) throws IOException {
    int pointCount = 0;
    for (DataPoint point : data) {
      if (checkIsHistoryData(point.getMeasurementId(), time)) {
        continue;
      }
      if (pointCount == 0) {
        pointCount++;
      }
      point.writeTo(
          time, chunkWriters.get(point.getMeasurementId())); // write time and value to page
      lastTimeMap.put(point.getMeasurementId(), time);
    }
    return pointCount;
  }

  @Override
  public int write(Tablet tablet) {
    int pointCount = 0;
    List<IMeasurementSchema> timeseries = tablet.getSchemas();
    for (int row = 0; row < tablet.rowSize; row++) {
      long time = tablet.timestamps[row];
      boolean hasOneColumnWritten = false;
      for (int column = 0; column < timeseries.size(); column++) {
        String measurementId = timeseries.get(column).getMeasurementId();
        if (checkIsHistoryData(measurementId, time)) {
          continue;
        }
        hasOneColumnWritten = true;
        switch (timeseries.get(column).getType()) {
          case INT32:
            chunkWriters.get(measurementId).write(time, ((int[]) tablet.values[column])[row]);
            break;
          case INT64:
            chunkWriters.get(measurementId).write(time, ((long[]) tablet.values[column])[row]);
            break;
          case FLOAT:
            chunkWriters.get(measurementId).write(time, ((float[]) tablet.values[column])[row]);
            break;
          case DOUBLE:
            chunkWriters.get(measurementId).write(time, ((double[]) tablet.values[column])[row]);
            break;
          case BOOLEAN:
            chunkWriters.get(measurementId).write(time, ((boolean[]) tablet.values[column])[row]);
            break;
          case TEXT:
            chunkWriters.get(measurementId).write(time, ((Binary[]) tablet.values[column])[row]);
            break;
          default:
            throw new UnSupportedDataTypeException(
                String.format("Data type %s is not supported.", timeseries.get(column).getType()));
        }
        lastTimeMap.put(measurementId, time);
      }
      if (hasOneColumnWritten) {
        pointCount++;
      }
    }
    return pointCount;
  }

  @Override
  public long flushToFileWriter(TsFileIOWriter fileWriter) throws IOException {
    LOG.debug("start flush device id:{}", deviceId);
    // make sure all the pages have been compressed into buffers, so that we can get correct
    // groupWriter.getCurrentChunkGroupSize().
    sealAllChunks();
    long currentChunkGroupSize = getCurrentChunkGroupSize();
    for (IChunkWriter seriesWriter : chunkWriters.values()) {
      seriesWriter.writeToFileWriter(fileWriter);
    }
    return currentChunkGroupSize;
  }

  @Override
  public long updateMaxGroupMemSize() {
    long bufferSize = 0;
    for (IChunkWriter seriesWriter : chunkWriters.values()) {
      bufferSize += seriesWriter.estimateMaxSeriesMemSize();
    }
    return bufferSize;
  }

  @Override
  public long getCurrentChunkGroupSize() {
    long size = 0;
    for (IChunkWriter writer : chunkWriters.values()) {
      size += writer.getSerializedChunkSize();
    }
    return size;
  }

  /** seal all the chunks which may has un-sealed pages in force. */
  private void sealAllChunks() {
    for (IChunkWriter writer : chunkWriters.values()) {
      writer.sealCurrentPage();
    }
  }

  private boolean checkIsHistoryData(String measurementId, long time) {
    if (time <= lastTimeMap.getOrDefault(measurementId, -1L)) {
      LOG.warn(
          "not allowed to write out-of-order data, time should later than "
              + time
              + ", skip dataPoint : "
              + deviceId
              + "."
              + measurementId
              + " at time "
              + time);
      return true;
    }
    return false;
  }
}
