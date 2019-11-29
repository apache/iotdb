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


import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.write.chunk.ChunkWriterImplV2;
import org.apache.iotdb.tsfile.write.record.TSRecord;
import org.apache.iotdb.tsfile.write.record.datapoint.DataPoint;
import org.apache.iotdb.tsfile.write.schemaV2.SchemaV2;
import org.apache.iotdb.tsfile.write.schemaV2.TimeseriesSchema;
import org.apache.iotdb.tsfile.write.writer.TsFileIOWriter;
import org.apache.iotdb.tsfile.write.writer.TsFileIOWriterV2;
import org.apache.iotdb.tsfile.write.writer.TsFileOutput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TsFileWriter is the entrance for writing processing. It receives a record and send it to
 * responding chunk group write. It checks memory size for all writing processing along its strategy
 * and flush data stored in memory to OutputStream. At the end of writing, user should call {@code
 * close()} method to flush the last data outside and close the normal outputStream and error
 * outputStream.
 */
public class TsFileWriterV2 implements AutoCloseable{

  private static final Logger LOG = LoggerFactory.getLogger(TsFileWriter.class);
  protected static final TSFileConfig config = TSFileDescriptor.getInstance().getConfig();

  /**
   * schema of this TsFile.
   **/
  protected final SchemaV2 schema;
  /**
   * IO writer of this TsFile.
   **/
  private final TsFileIOWriterV2 fileWriter;
  private final int pageSize;
  private long recordCount = 0;

  private Map<Path, ChunkWriterImplV2> chunkWriters = new HashMap<>();

  /**
   * min value of threshold of data points num check.
   **/
  private long recordCountForNextMemCheck = 100;
  private long chunkGroupSizeThreshold;

  /**
   * init this TsFileWriter.
   *
   * @param file the File to be written by this TsFileWriter
   */
  public TsFileWriterV2(File file) throws IOException {
    this(new TsFileIOWriterV2(file), new SchemaV2(), TSFileDescriptor.getInstance().getConfig());
  }

  /**
   * init this TsFileWriter.
   *
   * @param fileWriter the io writer of this TsFile
   */
  public TsFileWriterV2(TsFileIOWriterV2 fileWriter) throws IOException {
    this(fileWriter, new SchemaV2(), TSFileDescriptor.getInstance().getConfig());
  }

  /**
   * init this TsFileWriter.
   *
   * @param file the File to be written by this TsFileWriter
   * @param schema the schema of this TsFile
   */
  public TsFileWriterV2(File file, SchemaV2 schema) throws IOException {
    this(new TsFileIOWriterV2(file), schema, TSFileDescriptor.getInstance().getConfig());
  }

  /**
   * init this TsFileWriter.
   *
   * @param output the TsFileOutput of the file to be written by this TsFileWriter
   * @param schema the schema of this TsFile
   * @throws IOException
   */
  public TsFileWriterV2(TsFileOutput output, SchemaV2 schema) throws IOException {
    this(new TsFileIOWriterV2(output), schema, TSFileDescriptor.getInstance().getConfig());
  }

  /**
   * init this TsFileWriter.
   *
   * @param file the File to be written by this TsFileWriter
   * @param schema the schema of this TsFile
   * @param conf the configuration of this TsFile
   */
  public TsFileWriterV2(File file, SchemaV2 schema, TSFileConfig conf) throws IOException {
    this(new TsFileIOWriterV2(file), schema, conf);
  }

  /**
   * init this TsFileWriter.
   *
   * @param fileWriter the io writer of this TsFile
   * @param schema the schema of this TsFile
   * @param conf the configuration of this TsFile
   */
  protected TsFileWriterV2(TsFileIOWriterV2 fileWriter, SchemaV2 schema, TSFileConfig conf)
      throws IOException {
    if (!fileWriter.canWrite()) {
      throw new IOException(
          "the given file Writer does not support writing any more. Maybe it is an complete TsFile");
    }
    this.fileWriter = fileWriter;
    this.schema = schema;
    this.pageSize = conf.getPageSizeInByte();
    this.chunkGroupSizeThreshold = conf.getGroupSizeInByte();
    config.setTSFileStorageFs(conf.getTSFileStorageFs().name());
    if (this.pageSize >= chunkGroupSizeThreshold) {
      LOG.warn(
          "TsFile's page size {} is greater than chunk group size {}, please enlarge the chunk group"
              + " size or decrease page size. ", pageSize, chunkGroupSizeThreshold);
    }
  }

  /**
   * add a measurementSchema to this TsFile.
   */
  public void addDeviceTemplates(Map<String, TimeseriesSchema> template) throws WriteProcessException {

  }

  /**
   * Confirm whether the record is legal. If legal, add it into this RecordWriter.
   *
   * @param record - a record responding a line
   * @return - whether the record has been added into RecordWriter legally
   * @throws WriteProcessException exception
   */
  private boolean checkIsTimeSeriesExist(TSRecord record) throws WriteProcessException {
    for (DataPoint dataPoint: record.dataPointList) {
      Path path = new Path(record.deviceId, dataPoint.getMeasurementId());
      if (chunkWriters.containsKey(path)) {
        continue;
      }

      if (!schema.containsTimeseries(new Path(record.deviceId, dataPoint.getMeasurementId()))) {
        throw new WriteProcessException("Time series not registered:" + record.deviceId + dataPoint.getMeasurementId());
      } else {
        chunkWriters.put(path, new ChunkWriterImplV2(schema.getSeriesSchema(path)));
      }
    }

    return true;
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

    for (DataPoint dataPoint: record.dataPointList) {
      dataPoint.writeTo(record.time, chunkWriters.get(new Path(record.deviceId, dataPoint.getMeasurementId())));
    }

    ++recordCount;
    return checkMemorySizeAndMayFlushChunks();
  }


  /**
   * calculate total memory size occupied by all ChunkGroupWriter instances currently.
   *
   * @return total memory size used
   */
  private long calculateMemSizeForAllGroup() {
    int memTotalSize = 0;
    for (ChunkWriterImplV2 chunkWriter : chunkWriters.values()) {
      memTotalSize += chunkWriter.estimateMaxSeriesMemSize();
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
        return flushAllChunks();
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
   * function just return false, the Override of IoTDB may return true.
   * @throws IOException exception in IO
   */
  private boolean flushAllChunks() throws IOException {
    for (ChunkWriterImplV2 chunkWriter: chunkWriters.values()) {
      chunkWriter.writeToFileWriter(fileWriter);
    }
    return false;
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
    flushAllChunks();
    fileWriter.endFile(this.schema);
  }

  /**
   * this function is only for Test.
   * @return TsFileIOWriter
   */
  public TsFileIOWriterV2 getIOWriter() {
    return this.fileWriter;
  }

  /**
   * this function is only for Test
   * @throws IOException exception in IO
   */
  public void flushForTest() throws IOException {
    flushAllChunks();
  }
}
