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
package org.apache.tsfile.write.chunk;

import org.apache.tsfile.common.constant.TsFileConstant;
import org.apache.tsfile.encrypt.EncryptParameter;
import org.apache.tsfile.encrypt.EncryptUtils;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.exception.write.WriteProcessException;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.DateUtils;
import org.apache.tsfile.write.UnSupportedDataTypeException;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.record.Tablet.ColumnCategory;
import org.apache.tsfile.write.record.datapoint.DataPoint;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.writer.TsFileIOWriter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.LocalDate;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** a implementation of IChunkGroupWriter. */
public class NonAlignedChunkGroupWriterImpl implements IChunkGroupWriter {

  private static final Logger LOG = LoggerFactory.getLogger(NonAlignedChunkGroupWriterImpl.class);

  private final IDeviceID deviceId;

  /** Map(measurementID, ChunkWriterImpl). Aligned measurementId is empty. */
  private final Map<String, ChunkWriterImpl> chunkWriters = new LinkedHashMap<>();

  private EncryptParameter encryptParam;

  // measurementId -> lastTime
  private Map<String, Long> lastTimeMap = new HashMap<>();

  public NonAlignedChunkGroupWriterImpl(IDeviceID deviceId) {
    this.deviceId = deviceId;
    this.encryptParam = EncryptUtils.encryptParam;
  }

  public NonAlignedChunkGroupWriterImpl(IDeviceID deviceId, EncryptParameter encryptParam) {
    this.deviceId = deviceId;
    this.encryptParam = encryptParam;
  }

  @Override
  public void tryToAddSeriesWriter(IMeasurementSchema schema) {
    if (!chunkWriters.containsKey(schema.getMeasurementName())) {
      this.chunkWriters.put(schema.getMeasurementName(), new ChunkWriterImpl(schema, encryptParam));
    }
  }

  @Override
  public void tryToAddSeriesWriter(List<IMeasurementSchema> schemas) {
    for (IMeasurementSchema schema : schemas) {
      if (!chunkWriters.containsKey(schema.getMeasurementName())) {
        this.chunkWriters.put(
            schema.getMeasurementName(), new ChunkWriterImpl(schema, encryptParam));
      }
    }
  }

  @Override
  public int write(long time, List<DataPoint> data) throws IOException, WriteProcessException {
    int pointCount = 0;
    for (DataPoint point : data) {
      checkIsHistoryData(point.getMeasurementId(), time);

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
  public int write(Tablet tablet) throws IOException, WriteProcessException {
    return write(tablet, 0, tablet.getRowSize());
  }

  @Override
  public int write(Tablet tablet, int startRowIndex, int endRowIndex)
      throws WriteProcessException, IOException {
    int maxPointCount = 0, pointCount;
    List<IMeasurementSchema> timeseries = tablet.getSchemas();
    for (int column = 0; column < tablet.getSchemas().size(); column++) {
      if (tablet.getColumnTypes() != null
          && tablet.getColumnTypes().get(column) != ColumnCategory.MEASUREMENT) {
        continue;
      }
      String measurementId = timeseries.get(column).getMeasurementName();
      TSDataType tsDataType = timeseries.get(column).getType();
      pointCount = 0;
      for (int row = startRowIndex; row < endRowIndex; row++) {
        // check isNull in tablet
        if (tablet.bitMaps != null
            && tablet.bitMaps[column] != null
            && tablet.bitMaps[column].isMarked(row)) {
          continue;
        }
        long time = tablet.timestamps[row];
        checkIsHistoryData(measurementId, time);
        pointCount++;
        switch (tsDataType) {
          case INT32:
            chunkWriters.get(measurementId).write(time, ((int[]) tablet.values[column])[row]);
            break;
          case DATE:
            chunkWriters
                .get(measurementId)
                .write(
                    time,
                    DateUtils.parseDateExpressionToInt(((LocalDate[]) tablet.values[column])[row]));
            break;
          case INT64:
          case TIMESTAMP:
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
          case BLOB:
          case STRING:
            chunkWriters.get(measurementId).write(time, ((Binary[]) tablet.values[column])[row]);
            break;
          default:
            throw new UnSupportedDataTypeException(
                String.format("Data type %s is not supported.", tsDataType));
        }
        lastTimeMap.put(measurementId, time);
      }
      maxPointCount = Math.max(pointCount, maxPointCount);
    }
    return maxPointCount;
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

  private void checkIsHistoryData(String measurementId, long time) throws WriteProcessException {
    final Long lastTime = lastTimeMap.get(measurementId);
    if (lastTime != null && time <= lastTime) {
      throw new WriteProcessException(
          "Not allowed to write out-of-order data in timeseries "
              + deviceId
              + TsFileConstant.PATH_SEPARATOR
              + measurementId
              + ", time should later than "
              + lastTimeMap.get(measurementId));
    }
  }

  public Map<String, Long> getLastTimeMap() {
    return this.lastTimeMap;
  }

  public void setLastTimeMap(Map<String, Long> lastTimeMap) {
    this.lastTimeMap = lastTimeMap;
  }
}
