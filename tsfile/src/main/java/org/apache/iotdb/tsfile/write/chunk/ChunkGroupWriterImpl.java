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

import org.apache.iotdb.tsfile.exception.write.NoMeasurementException;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.record.datapoint.DataPoint;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.UnaryMeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.VectorMeasurementSchema;
import org.apache.iotdb.tsfile.write.writer.TsFileIOWriter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** a implementation of IChunkGroupWriter. */
public class ChunkGroupWriterImpl implements IChunkGroupWriter {

  private static final Logger LOG = LoggerFactory.getLogger(ChunkGroupWriterImpl.class);

  private final String deviceId;

  /** Map(measurementID, ChunkWriterImpl). */
  private Map<String, IChunkWriter> chunkWriters = new HashMap<>();

  public ChunkGroupWriterImpl(String deviceId) {
    this.deviceId = deviceId;
  }

  @Override
  public void tryToAddSeriesWriter(IMeasurementSchema schema, int pageSizeThreshold) {
    if (!chunkWriters.containsKey(schema.getMeasurementId())) {
      IChunkWriter seriesWriter = null;
      // initialize depend on schema type
      if (schema instanceof VectorMeasurementSchema) {
        seriesWriter = new VectorChunkWriterImpl(schema);
      } else if (schema instanceof UnaryMeasurementSchema) {
        seriesWriter = new ChunkWriterImpl(schema);
      }
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
  }

  /**
   * write if data type is VECTOR this method write next n column values (belong to one vector), and
   * return n to increase index
   *
   * @param tablet table
   * @param measurement vector measurement
   * @param index measurement start index
   */
  private void writeVectorDataType(Tablet tablet, String measurement, int index) {
    // reference: MemTableFlushTask.java
    int batchSize = tablet.rowSize;
    VectorMeasurementSchema vectorMeasurementSchema =
        (VectorMeasurementSchema) tablet.getSchemas().get(index);
    List<TSDataType> valueDataTypes = vectorMeasurementSchema.getSubMeasurementsTSDataTypeList();
    IChunkWriter vectorChunkWriter = chunkWriters.get(measurement);
    for (int row = 0; row < batchSize; row++) {
      long time = tablet.timestamps[row];
      for (int columnIndex = 0; columnIndex < valueDataTypes.size(); columnIndex++) {
        boolean isNull = false;
        // check isNull by bitMap in tablet
        if (tablet.bitMaps != null
            && tablet.bitMaps[columnIndex] != null
            && tablet.bitMaps[columnIndex].isMarked(row)) {
          isNull = true;
        }
        switch (valueDataTypes.get(columnIndex)) {
          case BOOLEAN:
            vectorChunkWriter.write(time, ((boolean[]) tablet.values[columnIndex])[row], isNull);
            break;
          case INT32:
            vectorChunkWriter.write(time, ((int[]) tablet.values[columnIndex])[row], isNull);
            break;
          case INT64:
            vectorChunkWriter.write(time, ((long[]) tablet.values[columnIndex])[row], isNull);
            break;
          case FLOAT:
            vectorChunkWriter.write(time, ((float[]) tablet.values[columnIndex])[row], isNull);
            break;
          case DOUBLE:
            vectorChunkWriter.write(time, ((double[]) tablet.values[columnIndex])[row], isNull);
            break;
          case TEXT:
            vectorChunkWriter.write(time, ((Binary[]) tablet.values[columnIndex])[row], isNull);
            break;
          default:
            throw new UnSupportedDataTypeException(
                String.format("Data type %s is not supported.", valueDataTypes.get(columnIndex)));
        }
      }
      vectorChunkWriter.write(time);
    }
  }

  /**
   * write by data type dataType should not be VECTOR! VECTOR type should use writeVector
   *
   * @param tablet table contain all time and value
   * @param measurementId current measurement
   * @param dataType current data type
   * @param index which column values should be write
   */
  private void writeByDataType(
      Tablet tablet, String measurementId, TSDataType dataType, int index) {
    int batchSize = tablet.rowSize;
    switch (dataType) {
      case INT32:
        chunkWriters
            .get(measurementId)
            .write(tablet.timestamps, (int[]) tablet.values[index], batchSize);
        break;
      case INT64:
        chunkWriters
            .get(measurementId)
            .write(tablet.timestamps, (long[]) tablet.values[index], batchSize);
        break;
      case FLOAT:
        chunkWriters
            .get(measurementId)
            .write(tablet.timestamps, (float[]) tablet.values[index], batchSize);
        break;
      case DOUBLE:
        chunkWriters
            .get(measurementId)
            .write(tablet.timestamps, (double[]) tablet.values[index], batchSize);
        break;
      case BOOLEAN:
        chunkWriters
            .get(measurementId)
            .write(tablet.timestamps, (boolean[]) tablet.values[index], batchSize);
        break;
      case TEXT:
        chunkWriters
            .get(measurementId)
            .write(tablet.timestamps, (Binary[]) tablet.values[index], batchSize);
        break;
      default:
        throw new UnSupportedDataTypeException(
            String.format("Data type %s is not supported.", dataType));
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

  @Override
  public int getSeriesNumber() {
    return chunkWriters.size();
  }
}
