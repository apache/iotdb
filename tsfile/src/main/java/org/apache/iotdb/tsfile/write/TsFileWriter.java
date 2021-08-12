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
package org.apache.iotdb.tsfile.write;

import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.exception.write.NoMeasurementException;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.write.chunk.ChunkGroupWriterImpl;
import org.apache.iotdb.tsfile.write.chunk.IChunkGroupWriter;
import org.apache.iotdb.tsfile.write.record.TSRecord;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.record.datapoint.DataPoint;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.Schema;
import org.apache.iotdb.tsfile.write.writer.RestorableTsFileIOWriter;
import org.apache.iotdb.tsfile.write.writer.TsFileIOWriter;
import org.apache.iotdb.tsfile.write.writer.TsFileOutput;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * TsFileWriter is the entrance for writing processing. It receives a record and send it to
 * responding chunk group write. It checks memory size for all writing processing along its strategy
 * and flush data stored in memory to OutputStream. At the end of writing, user should call {@code
 * close()} method to flush the last data outside and close the normal outputStream and error
 * outputStream.
 */
public class TsFileWriter implements AutoCloseable {

  protected static final TSFileConfig config = TSFileDescriptor.getInstance().getConfig();
  private static final Logger LOG = LoggerFactory.getLogger(TsFileWriter.class);
  /** schema of this TsFile. */
  protected final Schema schema;
  /** IO writer of this TsFile. */
  private final TsFileIOWriter fileWriter;

  private final int pageSize;
  private long recordCount = 0;

  private Map<String, IChunkGroupWriter> groupWriters = new HashMap<>();

  /** min value of threshold of data points num check. */
  private long recordCountForNextMemCheck = 100;

  private long chunkGroupSizeThreshold;

  /**
   * init this TsFileWriter.
   *
   * @param file the File to be written by this TsFileWriter
   */
  public TsFileWriter(File file) throws IOException {
    this(new TsFileIOWriter(file), new Schema(), TSFileDescriptor.getInstance().getConfig());
  }

  /**
   * init this TsFileWriter.
   *
   * @param fileWriter the io writer of this TsFile
   */
  public TsFileWriter(TsFileIOWriter fileWriter) throws IOException {
    this(fileWriter, new Schema(), TSFileDescriptor.getInstance().getConfig());
  }

  /**
   * init this TsFileWriter.
   *
   * @param file the File to be written by this TsFileWriter
   * @param schema the schema of this TsFile
   */
  public TsFileWriter(File file, Schema schema) throws IOException {
    this(new TsFileIOWriter(file), schema, TSFileDescriptor.getInstance().getConfig());
  }

  /**
   * init this TsFileWriter.
   *
   * @param output the TsFileOutput of the file to be written by this TsFileWriter
   * @param schema the schema of this TsFile
   * @throws IOException
   */
  public TsFileWriter(TsFileOutput output, Schema schema) throws IOException {
    this(new TsFileIOWriter(output), schema, TSFileDescriptor.getInstance().getConfig());
  }

  /**
   * init this TsFileWriter.
   *
   * @param file the File to be written by this TsFileWriter
   * @param schema the schema of this TsFile
   * @param conf the configuration of this TsFile
   */
  public TsFileWriter(File file, Schema schema, TSFileConfig conf) throws IOException {
    this(new TsFileIOWriter(file), schema, conf);
  }

  /**
   * init this TsFileWriter.
   *
   * @param fileWriter the io writer of this TsFile
   * @param schema the schema of this TsFile
   * @param conf the configuration of this TsFile
   */
  protected TsFileWriter(TsFileIOWriter fileWriter, Schema schema, TSFileConfig conf)
      throws IOException {
    if (!fileWriter.canWrite()) {
      throw new IOException(
          "the given file Writer does not support writing any more. Maybe it is an complete TsFile");
    }
    this.fileWriter = fileWriter;

    if (fileWriter instanceof RestorableTsFileIOWriter) {
      this.schema = new Schema(((RestorableTsFileIOWriter) fileWriter).getKnownSchema());
    } else {
      this.schema = schema;
    }
    this.pageSize = conf.getPageSizeInByte();
    this.chunkGroupSizeThreshold = conf.getGroupSizeInByte();
    config.setTSFileStorageFs(conf.getTSFileStorageFs());
    if (this.pageSize >= chunkGroupSizeThreshold) {
      LOG.warn(
          "TsFile's page size {} is greater than chunk group size {}, please enlarge the chunk group"
              + " size or decrease page size. ",
          pageSize,
          chunkGroupSizeThreshold);
    }
  }

  public void registerSchemaTemplate(
      String templateName, Map<String, IMeasurementSchema> template) {
    schema.registerSchemaTemplate(templateName, template);
  }

  public void registerDevice(String deviceId, String templateName) {
    schema.registerDevice(deviceId, templateName);
  }

  public void registerTimeseries(Path path, IMeasurementSchema measurementSchema)
      throws WriteProcessException {
    if (schema.containsTimeseries(path)) {
      throw new WriteProcessException("given timeseries has exists! " + path);
    }
    schema.registerTimeseries(path, measurementSchema);
  }

  /**
   * Confirm whether the record is legal. If legal, add it into this RecordWriter.
   *
   * @param record - a record responding a line
   * @return - whether the record has been added into RecordWriter legally
   * @throws WriteProcessException exception
   */
  private boolean checkIsTimeSeriesExist(TSRecord record) throws WriteProcessException {
    IChunkGroupWriter groupWriter;
    if (!groupWriters.containsKey(record.deviceId)) {
      groupWriter = new ChunkGroupWriterImpl(record.deviceId);
      groupWriters.put(record.deviceId, groupWriter);
    } else {
      groupWriter = groupWriters.get(record.deviceId);
    }

    // add all SeriesWriter of measurements in this TSRecord to this ChunkGroupWriter
    for (DataPoint dp : record.dataPointList) {
      String measurementId = dp.getMeasurementId();
      Path path = new Path(record.deviceId, measurementId);
      if (schema.containsTimeseries(path)) {
        groupWriter.tryToAddSeriesWriter(schema.getSeriesSchema(path), pageSize);
      } else if (schema.getSchemaTemplates() != null && schema.getSchemaTemplates().size() == 1) {
        // use the default template without needing to register device
        Map<String, IMeasurementSchema> template =
            schema.getSchemaTemplates().entrySet().iterator().next().getValue();
        if (template.containsKey(path.getMeasurement())) {
          groupWriter.tryToAddSeriesWriter(template.get(path.getMeasurement()), pageSize);
        }
      } else {
        throw new NoMeasurementException("input path is invalid: " + path);
      }
    }
    return true;
  }

  /**
   * Confirm whether the tablet is legal.
   *
   * @param tablet - a tablet data responding multiple columns
   * @return - whether the tablet's measurements have been added into RecordWriter legally
   * @throws WriteProcessException exception
   */
  private void checkIsTimeSeriesExist(Tablet tablet) throws WriteProcessException {
    IChunkGroupWriter groupWriter;
    if (!groupWriters.containsKey(tablet.prefixPath)) {
      groupWriter = new ChunkGroupWriterImpl(tablet.prefixPath);
      groupWriters.put(tablet.prefixPath, groupWriter);
    } else {
      groupWriter = groupWriters.get(tablet.prefixPath);
    }
    String deviceId = tablet.prefixPath;

    // add all SeriesWriter of measurements in this Tablet to this ChunkGroupWriter
    for (IMeasurementSchema timeseries : tablet.getSchemas()) {
      String measurementId = timeseries.getMeasurementId();
      Path path = new Path(deviceId, measurementId);
      if (schema.containsTimeseries(path)) {
        groupWriter.tryToAddSeriesWriter(schema.getSeriesSchema(path), pageSize);
      } else if (schema.getSchemaTemplates() != null && schema.getSchemaTemplates().size() == 1) {
        // use the default template without needing to register device
        Map<String, IMeasurementSchema> template =
            schema.getSchemaTemplates().entrySet().iterator().next().getValue();
        if (template.containsKey(path.getMeasurement())) {
          groupWriter.tryToAddSeriesWriter(template.get(path.getMeasurement()), pageSize);
        }
      } else {
        throw new NoMeasurementException("input measurement is invalid: " + measurementId);
      }
    }
  }

  /**
   * write a record in type of T.
   *
   * @param record - record responding a data line
   * @return true -size of tsfile or metadata reaches the threshold. false - otherwise
   * @throws IOException exception in IO
   * @throws WriteProcessException exception in write process
   */
  public boolean write(TSRecord record) throws IOException, WriteProcessException {
    // make sure the ChunkGroupWriter for this TSRecord exist
    checkIsTimeSeriesExist(record);
    // get corresponding ChunkGroupWriter and write this TSRecord
    groupWriters.get(record.deviceId).write(record.time, record.dataPointList);
    ++recordCount;
    return checkMemorySizeAndMayFlushChunks();
  }

  /**
   * write a tablet
   *
   * @param tablet - multiple time series of one device that share a time column
   * @throws IOException exception in IO
   * @throws WriteProcessException exception in write process
   */
  public boolean write(Tablet tablet) throws IOException, WriteProcessException {
    // make sure the ChunkGroupWriter for this Tablet exist
    checkIsTimeSeriesExist(tablet);
    // get corresponding ChunkGroupWriter and write this Tablet
    groupWriters.get(tablet.prefixPath).write(tablet);
    recordCount += tablet.rowSize;
    return checkMemorySizeAndMayFlushChunks();
  }

  /**
   * calculate total memory size occupied by all ChunkGroupWriter instances currently.
   *
   * @return total memory size used
   */
  private long calculateMemSizeForAllGroup() {
    long memTotalSize = 0;
    for (IChunkGroupWriter group : groupWriters.values()) {
      memTotalSize += group.updateMaxGroupMemSize();
    }
    return memTotalSize;
  }

  /**
   * check occupied memory size, if it exceeds the chunkGroupSize threshold, flush them to given
   * OutputStream.
   *
   * @return true - size of tsfile or metadata reaches the threshold. false - otherwise
   * @throws IOException exception in IO
   */
  private boolean checkMemorySizeAndMayFlushChunks() throws IOException {
    if (recordCount >= recordCountForNextMemCheck) {
      long memSize = calculateMemSizeForAllGroup();
      assert memSize > 0;
      if (memSize > chunkGroupSizeThreshold) {
        LOG.debug("start to flush chunk groups, memory space occupy:{}", memSize);
        recordCountForNextMemCheck = recordCount * chunkGroupSizeThreshold / memSize;
        return flushAllChunkGroups();
      } else {
        recordCountForNextMemCheck = recordCount * chunkGroupSizeThreshold / memSize;
        return false;
      }
    }
    return false;
  }

  /**
   * flush the data in all series writers of all chunk group writers and their page writers to
   * outputStream.
   *
   * @return true - size of tsfile or metadata reaches the threshold. false - otherwise. But this
   *     function just return false, the Override of IoTDB may return true.
   * @throws IOException exception in IO
   */
  public boolean flushAllChunkGroups() throws IOException {
    if (recordCount > 0) {
      for (Map.Entry<String, IChunkGroupWriter> entry : groupWriters.entrySet()) {
        String deviceId = entry.getKey();
        IChunkGroupWriter groupWriter = entry.getValue();
        fileWriter.startChunkGroup(deviceId);
        long pos = fileWriter.getPos();
        long dataSize = groupWriter.flushToFileWriter(fileWriter);
        if (fileWriter.getPos() - pos != dataSize) {
          throw new IOException(
              String.format(
                  "Flushed data size is inconsistent with computation! Estimated: %d, Actual: %d",
                  dataSize, fileWriter.getPos() - pos));
        }
        fileWriter.endChunkGroup();
      }
      reset();
    }
    return false;
  }

  private void reset() {
    groupWriters.clear();
    recordCount = 0;
  }

  /**
   * calling this method to write the last data remaining in memory and close the normal and error
   * OutputStream.
   *
   * @throws IOException exception in IO
   */
  @Override
  public void close() throws IOException {
    LOG.info("start close file");
    flushAllChunkGroups();
    fileWriter.endFile();
  }

  /**
   * this function is only for Test.
   *
   * @return TsFileIOWriter
   */
  public TsFileIOWriter getIOWriter() {
    return this.fileWriter;
  }
}
