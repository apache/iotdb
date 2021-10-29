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
import java.util.List;
import java.util.Map;
import org.apache.iotdb.tsfile.exception.write.NoMeasurementException;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.record.datapoint.DataPoint;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.VectorMeasurementSchema;
import org.apache.iotdb.tsfile.write.writer.TsFileIOWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** a implementation of IChunkGroupWriter. */
public class ChunkGroupWriterImpl implements IChunkGroupWriter {

  private static final Logger LOG = LoggerFactory.getLogger(ChunkGroupWriterImpl.class);

  private final String deviceId;

  // measurementID -> indexOfValueChunkWriter
  //  Map<String, Integer> measurementChunkWriterMap = new HashMap<>();
  Map<String, ValueChunkWriter> measurementChunkWriterMap = new HashMap<>();

  /** Map(measurementID, ChunkWriterImpl). */
  private Map<String, IChunkWriter> chunkWriters = new HashMap<>();

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
        measurementChunkWriterMap.put(schema.getMeasurementId(), valueChunkWriter);
        vectorChunkWriter.addValueChunkWriter(
            valueChunkWriter); // Todo:是否需要这步？或者直接在ChunkGroupWriter里调用ValueChunkWriter进行写入
        this.chunkWriters.put("", vectorChunkWriter);
      }
    }
  }

  @Override
  public void write(long time, List<DataPoint> data) throws IOException {
    for (DataPoint point : data) {
      String measurementId = point.getMeasurementId();
      /*if (!chunkWriters.containsKey(measurementId)) {
        throw new NoMeasurementException(
            "time " + time + ", measurement id " + measurementId + " not found!");
      }*/
      point.writeTo(time, chunkWriters.get(measurementId)); // write time and value to page
    }
  }

  @Override
  public void writeAligned(long time, List<DataPoint> data) throws IOException {
    //    for (DataPoint d : data) {
    //      if (!chunkWriters.containsKey("")
    //          || !chunkWriters.get("").getValueChunkWriterMap().containsKey(d.getMeasurementId()))
    // {
    //        throw new NoMeasurementException(
    //            "aligned measurements not found in device " + deviceId + " !");
    //      }
    //    }
    IChunkWriter vectorChunkWriter = chunkWriters.get("");
    for (DataPoint point : data) {
      // point.writeTo(time, vectorChunkWriter); // only write value to valuePage
      ValueChunkWriter valueChunkWriter =
          measurementChunkWriterMap.get(
              point.getMeasurementId()); // Todo:应该要用VectorChunkWriter进行写入比较好
      boolean isNull = point == null;
      switch (point.getType()) {
        case BOOLEAN:
          valueChunkWriter.write(time, (boolean) point.getValue(), isNull);
          break;
        case INT32:
          valueChunkWriter.write(time, (int) point.getValue(), isNull);
          break;
        case INT64:
          valueChunkWriter.write(time, (long) point.getValue(), isNull);
          break;
        case FLOAT:
          valueChunkWriter.write(time, (float) point.getValue(), isNull);
          break;
        case DOUBLE:
          valueChunkWriter.write(time, (double) point.getValue(), isNull);
          break;
        case TEXT:
          valueChunkWriter.write(time, (Binary) point.getValue(), isNull);
          break;
        default:
          throw new UnSupportedDataTypeException(
              String.format("Data type %s is not supported.", point.getType()));
      }
    }
    vectorChunkWriter.write(time);
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

  public void write(Tablet tablet) throws NoMeasurementException {
    List<IMeasurementSchema> timeseries = tablet.getSchemas();
    for (int i = 0; i < timeseries.size(); i++) {
      String measurementId = timeseries.get(i).getMeasurementId();
      TSDataType dataType = timeseries.get(i).getType();
      if (!chunkWriters.containsKey(measurementId)) {
        throw new NoMeasurementException("measurement id" + measurementId + " not found!");
      }
      writeByDataType(tablet, measurementId, dataType, i);
    }
  }

  public void writeAligned(Tablet tablet) { // 写操作时，一个Tablet要么全是该设备的非对齐序列，要么全是对齐序列。两者不能混合放入一个Tablet里
    int batchSize = tablet.rowSize;
    List<IMeasurementSchema> measurementSchemas = tablet.getSchemas();
    IChunkWriter vectorChunkWriter = chunkWriters.get("");
    for (int row = 0; row < batchSize; row++) {
      long time = tablet.timestamps[row];
      for (int columnIndex = 0; columnIndex < measurementSchemas.size(); columnIndex++) {
        boolean isNull = false;
        // check isNull by bitMap in tablet
        if (tablet.bitMaps != null
            && tablet.bitMaps[columnIndex] != null
            && tablet.bitMaps[columnIndex].isMarked(row)) {
          isNull = true;
        }
        switch (measurementSchemas.get(columnIndex).getType()) {
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
                String.format(
                    "Data type %s is not supported.",
                    measurementSchemas.get(columnIndex).getType()));
        }
      }
      vectorChunkWriter.write(time);
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
