/**
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
import java.util.List;
import java.util.Map;

import org.apache.iotdb.tsfile.exception.write.NoMeasurementException;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.file.footer.ChunkGroupFooter;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.write.record.RowBatch;
import org.apache.iotdb.tsfile.write.record.datapoint.DataPoint;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.iotdb.tsfile.write.writer.TsFileIOWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * a implementation of IChunkGroupWriter.
 */
public class ChunkGroupWriterImpl implements IChunkGroupWriter {

  private static final Logger LOG = LoggerFactory.getLogger(ChunkGroupWriterImpl.class);

  private final String deviceId;

  /**
   * Map(measurementID, ChunkWriterImpl).
   */
  private Map<String, IChunkWriter> chunkWriters = new HashMap<>();

  public ChunkGroupWriterImpl(String deviceId) {
    this.deviceId = deviceId;
  }

  @Override
  public void tryToAddSeriesWriter(MeasurementSchema schema, int pageSizeThreshold) {
    if (!chunkWriters.containsKey(schema.getMeasurementId())) {
      ChunkBuffer chunkBuffer = new ChunkBuffer(schema);
      IChunkWriter seriesWriter = new ChunkWriterImpl(schema, chunkBuffer, pageSizeThreshold);
      this.chunkWriters.put(schema.getMeasurementId(), seriesWriter);
    }
  }

  @Override
  public void write(long time, List<DataPoint> data) throws WriteProcessException, IOException {
    for (DataPoint point : data) {
      String measurementId = point.getMeasurementId();
      if (!chunkWriters.containsKey(measurementId)) {
        throw new NoMeasurementException(
            "time " + time + ", measurement id " + measurementId + " not found!");
      }
      point.writeTo(time, chunkWriters.get(measurementId));

    }
  }

  @Override
  public void write(RowBatch rowBatch) throws WriteProcessException, IOException {
    List<MeasurementSchema> measurements = rowBatch.measurements;
    for (int i = 0; i < measurements.size(); i++) {
      String measurementId = measurements.get(i).getMeasurementId();
      TSDataType dataType = measurements.get(i).getType();
      if (!chunkWriters.containsKey(measurementId)) {
        throw new NoMeasurementException("measurement id" + measurementId + " not found!");
      }
      writeByDataType(rowBatch, measurementId, dataType, i);
    }
  }

  private void writeByDataType(
          RowBatch rowBatch, String measurementId, TSDataType dataType, int index) throws IOException {
    switch (dataType) {
      case INT32:
        chunkWriters.get(measurementId).write(rowBatch.timestamps, (int[]) rowBatch.values[index]);
        break;
      case INT64:
        chunkWriters.get(measurementId).write(rowBatch.timestamps, (long[]) rowBatch.values[index]);
        break;
      case FLOAT:
        chunkWriters.get(measurementId).write(rowBatch.timestamps, (float[]) rowBatch.values[index]);
        break;
      case DOUBLE:
        chunkWriters.get(measurementId).write(rowBatch.timestamps, (double[]) rowBatch.values[index]);
        break;
      case BOOLEAN:
        chunkWriters.get(measurementId).write(rowBatch.timestamps, (boolean[]) rowBatch.values[index]);
        break;
      case TEXT:
        chunkWriters.get(measurementId).write(rowBatch.timestamps, (Binary[]) rowBatch.values[index]);
        break;
      default:
        throw new UnSupportedDataTypeException(
                String.format("Data type %s is not supported.", dataType));
    }
  }

  @Override
  public ChunkGroupFooter flushToFileWriter(TsFileIOWriter fileWriter) throws IOException {
    LOG.debug("start flush device id:{}", deviceId);
    // make sure all the pages have been compressed into buffers, so that we can get correct
    // groupWriter.getCurrentChunkGroupSize().
    sealAllChunks();
    ChunkGroupFooter footer = new ChunkGroupFooter(deviceId, getCurrentChunkGroupSize(),
        getSeriesNumber());
    for (IChunkWriter seriesWriter : chunkWriters.values()) {
      seriesWriter.writeToFileWriter(fileWriter);
    }
    return footer;
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
      size += writer.getCurrentChunkSize();
    }
    return size;
  }

  /**
   * seal all the chunks which may has un-sealed pages in force.
   */
  private void sealAllChunks() {
    for (IChunkWriter writer : chunkWriters.values()) {
      writer.sealCurrentPage();
    }
  }

  @Override
  public int getSeriesNumber() {
    return chunkWriters.size();
  }
}
