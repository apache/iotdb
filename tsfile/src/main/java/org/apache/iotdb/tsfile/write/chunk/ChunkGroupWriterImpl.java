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

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.record.datapoint.DataPoint;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.writer.TsFileIOWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** a implementation of IChunkGroupWriter. */
public class ChunkGroupWriterImpl implements IChunkGroupWriter {

  private static final Logger LOG = LoggerFactory.getLogger(ChunkGroupWriterImpl.class);

  private final String deviceId;

  // measurementID -> ValueChunkWriter
  private Map<String, ValueChunkWriter> measurementChunkWriterMap = new HashMap<>();

  private Set<String> writenMeasurementSet = new HashSet<>();

  /** Map(measurementID, ChunkWriterImpl). Aligned measurementId is empty. */
  private Map<String, IChunkWriter> chunkWriters = new HashMap<>();

  private Map<String, Long> lastTimeMap = new HashMap<>();

  public ChunkGroupWriterImpl(String deviceId) {
    this.deviceId = deviceId;
  }

  @Override
  public void tryToAddSeriesWriter(IMeasurementSchema schema, int pageSizeThreshold) {
    if (!chunkWriters.containsKey(schema.getMeasurementId())) {
      this.chunkWriters.put(schema.getMeasurementId(), new ChunkWriterImpl(schema));
    }
  }

  @Override
  public void tryToAddAlignedSeriesWriter(List<IMeasurementSchema> schemas, int pageSizeThreshold) {
    IChunkWriter vectorChunkWriter;
    if (!chunkWriters.containsKey("")) {
      vectorChunkWriter = new VectorChunkWriterImpl();
    } else {
      vectorChunkWriter = chunkWriters.get("");
    }
    for (IMeasurementSchema schema : schemas) {
      if (!measurementChunkWriterMap.containsKey(schema.getMeasurementId())) {
        ValueChunkWriter valueChunkWriter =
            new ValueChunkWriter(
                schema.getMeasurementId(),
                schema.getCompressor(),
                schema.getType(),
                schema.getEncodingType(),
                schema.getValueEncoder());
        // measurementChunkWriterMap.put(schema.getMeasurementId(),
        // measurementChunkWriterMap.size());
        measurementChunkWriterMap.put(schema.getMeasurementId(), valueChunkWriter);
        vectorChunkWriter.addValueChunkWriter(
            valueChunkWriter); // Todo:是否需要这步？或者直接在ChunkGroupWriter里调用ValueChunkWriter进行写入
        this.chunkWriters.put("", vectorChunkWriter);
      }
    }
  }

  @Override
  public void write(long time, List<DataPoint> data) throws IOException, WriteProcessException {
    for (DataPoint point : data) {
      if (checkIsHistoryData(point.getMeasurementId(), time)) {
        continue;
      }
      point.writeTo(
          time, chunkWriters.get(point.getMeasurementId())); // write time and value to page
      lastTimeMap.put(point.getMeasurementId(), time);
    }
  }

  @Override
  public void writeAligned(long time, List<DataPoint> data) throws WriteProcessException {
    IChunkWriter vectorChunkWriter = chunkWriters.get("");
    if (checkIsHistoryData("", time)) {
      throw new WriteProcessException("not allowed to write out-of-order data, skip time: " + time);
    }
    for (DataPoint point : data) {
      // point.writeTo(time, vectorChunkWriter); // only write value to valuePage
      writenMeasurementSet.add(point.getMeasurementId());
      boolean isNull = point == null;
      writeAlignedByTimestamp(
          point.getMeasurementId(), point.getType(), time, point.getValue(), isNull);
    }
    writeEmptyData(time);
    vectorChunkWriter.write(time);
    lastTimeMap.put("", time);
  }

  private void writeAlignedByTimestamp(
      String measurementId, TSDataType tsDataType, long time, Object value, boolean isNull) {
    ValueChunkWriter valueChunkWriter = measurementChunkWriterMap.get(measurementId);
    switch (tsDataType) {
      case BOOLEAN:
        valueChunkWriter.write(time, (boolean) value, isNull);
        break;
      case INT32:
        valueChunkWriter.write(time, (int) value, isNull);
        break;
      case INT64:
        valueChunkWriter.write(time, (long) value, isNull);
        break;
      case FLOAT:
        valueChunkWriter.write(time, (float) value, isNull);
        break;
      case DOUBLE:
        valueChunkWriter.write(time, (double) value, isNull);
        break;
      case TEXT:
        valueChunkWriter.write(time, (Binary) value, isNull);
        break;
      default:
        throw new UnSupportedDataTypeException(
            String.format("Data type %s is not supported.", tsDataType));
    }
  }

  private void writeEmptyData(long time) {
    for (Map.Entry<String, ValueChunkWriter> entry : measurementChunkWriterMap.entrySet()) {
      if (!writenMeasurementSet.contains(entry.getKey())) {
        entry.getValue().write(time, 0, true);
      }
    }
    writenMeasurementSet.clear();
  }

  /*@Override
  public void write(Tablet tablet) throws WriteProcessException {
    List<IMeasurementSchema> timeseries = tablet.getSchemas();
    for (int i = 0; i < timeseries.size(); i++) {
      String measurementId = timeseries.get(i).getMeasurementId();
      TSDataType dataType = timeseries.get(i).getType();
      if (!chunkWriters.containsKey(measurementId)) {
        throw new NoMeasurementException("measurement id" + measurementId + " not found!");
      }
      if (dataType.equals(TSDataType.VECTOR)) {
        writeVectorDataType(tablet, measurementId, i);
      } else {
        writeByDataType(tablet, measurementId, dataType, i);
      }
    }
  }*/

  @Override
  public void write(Tablet tablet) { // Todo:修改优化，添加checkIsHistoryData("",time);
    List<IMeasurementSchema> timeseries = tablet.getSchemas();

    for (int row = 0; row < tablet.rowSize; row++) {
      long time = tablet.timestamps[row];
      for (int column = 0; column < timeseries.size(); column++) {
        String measurementId = timeseries.get(column).getMeasurementId();
        if (checkIsHistoryData(measurementId, time)) {
          continue;
        }
        boolean isNull = false;
        // check isNull by bitMap in tablet
        if (tablet.bitMaps != null
            && tablet.bitMaps[column] != null
            && tablet.bitMaps[column].isMarked(row)) {
          isNull = true;
        }
        switch (timeseries.get(column).getType()) {
          case INT32:
            chunkWriters
                .get(measurementId)
                .write(time, ((int[]) tablet.values[column])[row], isNull);
            break;
          case INT64:
            chunkWriters
                .get(measurementId)
                .write(time, ((long[]) tablet.values[column])[row], isNull);
            break;
          case FLOAT:
            chunkWriters
                .get(measurementId)
                .write(time, ((float[]) tablet.values[column])[row], isNull);
            break;
          case DOUBLE:
            chunkWriters
                .get(measurementId)
                .write(time, ((double[]) tablet.values[column])[row], isNull);
            break;
          case BOOLEAN:
            chunkWriters
                .get(measurementId)
                .write(time, ((boolean[]) tablet.values[column])[row], isNull);
            break;
          case TEXT:
            chunkWriters
                .get(measurementId)
                .write(time, ((Binary[]) tablet.values[column])[row], isNull);
            break;
          default:
            throw new UnSupportedDataTypeException(
                String.format("Data type %s is not supported.", timeseries.get(column).getType()));
        }
        lastTimeMap.put(measurementId, time);
      }
    }

    /*for (int i = 0; i < timeseries.size(); i++) {
      String measurementId = timeseries.get(i).getMeasurementId();
      for (long time : tablet.timestamps) {
        checkIsHistoryData(measurementId, time);
      }
      TSDataType dataType = timeseries.get(i).getType();
      if (!chunkWriters.containsKey(measurementId)) {
        LOG.error("measurement id" + measurementId + " not found!");
      }
      int batchSize = tablet.rowSize;
      switch (dataType) {
        case INT32:
          chunkWriters
              .get(measurementId)
              .write(tablet.timestamps, (int[]) tablet.values[i], batchSize);
          break;
        case INT64:
          chunkWriters
              .get(measurementId)
              .write(tablet.timestamps, (long[]) tablet.values[i], batchSize);
          break;
        case FLOAT:
          chunkWriters
              .get(measurementId)
              .write(tablet.timestamps, (float[]) tablet.values[i], batchSize);
          break;
        case DOUBLE:
          chunkWriters
              .get(measurementId)
              .write(tablet.timestamps, (double[]) tablet.values[i], batchSize);
          break;
        case BOOLEAN:
          chunkWriters
              .get(measurementId)
              .write(tablet.timestamps, (boolean[]) tablet.values[i], batchSize);
          break;
        case TEXT:
          chunkWriters
              .get(measurementId)
              .write(tablet.timestamps, (Binary[]) tablet.values[i], batchSize);
          break;
        default:
          throw new UnSupportedDataTypeException(
              String.format("Data type %s is not supported.", dataType));
      }
    }*/
  }

  @Override
  public void writeAligned(Tablet tablet) {
    List<IMeasurementSchema> measurementSchemas = tablet.getSchemas();
    IChunkWriter vectorChunkWriter = chunkWriters.get("");
    for (int row = 0; row < tablet.rowSize; row++) {
      long time = tablet.timestamps[row];
      if (checkIsHistoryData("", time)) {
        continue;
      }
      for (int columnIndex = 0; columnIndex < measurementSchemas.size(); columnIndex++) {
        writenMeasurementSet.add(measurementSchemas.get(columnIndex).getMeasurementId());
        boolean isNull = false;
        // check isNull by bitMap in tablet
        if (tablet.bitMaps != null
            && tablet.bitMaps[columnIndex] != null
            && tablet.bitMaps[columnIndex].isMarked(row)) {
          isNull = true;
        }
        ValueChunkWriter valueChunkWriter =
            measurementChunkWriterMap.get(measurementSchemas.get(columnIndex).getMeasurementId());
        switch (measurementSchemas.get(columnIndex).getType()) {
          case BOOLEAN:
            valueChunkWriter.write(time, ((boolean[]) tablet.values[columnIndex])[row], isNull);
            break;
          case INT32:
            valueChunkWriter.write(time, ((int[]) tablet.values[columnIndex])[row], isNull);
            break;
          case INT64:
            valueChunkWriter.write(time, ((long[]) tablet.values[columnIndex])[row], isNull);
            break;
          case FLOAT:
            valueChunkWriter.write(time, ((float[]) tablet.values[columnIndex])[row], isNull);
            break;
          case DOUBLE:
            valueChunkWriter.write(time, ((double[]) tablet.values[columnIndex])[row], isNull);
            break;
          case TEXT:
            valueChunkWriter.write(time, ((Binary[]) tablet.values[columnIndex])[row], isNull);
            break;
          default:
            throw new UnSupportedDataTypeException(
                String.format(
                    "Data type %s is not supported.",
                    measurementSchemas.get(columnIndex).getType()));
        }
      }
      writeEmptyData(time);
      vectorChunkWriter.write(time);
      lastTimeMap.put("", time);
    }
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
    if (time <= lastTimeMap.getOrDefault(measurementId, new Long(0))) {
      LOG.warn(
          "not allowed to write out-of-order data, skip dataPoint deviceId: "
              + deviceId
              + " measurementId: "
              + measurementId
              + ", time: "
              + time);
      return true;
    }
    return false;
  }

  @Override
  public int getSeriesNumber() {
    return chunkWriters.size();
  }
}
