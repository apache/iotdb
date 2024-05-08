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
import org.apache.iotdb.tsfile.utils.MeasurementGroup;
import org.apache.iotdb.tsfile.write.chunk.AlignedChunkGroupWriterImpl;
import org.apache.iotdb.tsfile.write.chunk.IChunkGroupWriter;
import org.apache.iotdb.tsfile.write.chunk.NonAlignedChunkGroupWriterImpl;
import org.apache.iotdb.tsfile.write.record.TSRecord;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.record.datapoint.DataPoint;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.Schema;
import org.apache.iotdb.tsfile.write.schema.VectorMeasurementSchema;
import org.apache.iotdb.tsfile.write.writer.RestorableTsFileIOWriter;
import org.apache.iotdb.tsfile.write.writer.TsFileIOWriter;
import org.apache.iotdb.tsfile.write.writer.TsFileOutput;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
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

  // deviceId -> measurementIdList
  private Map<String, List<String>> flushedMeasurementsInDeviceMap = new HashMap<>();

  // DeviceId -> LastTime
  private Map<String, Long> alignedDeviceLastTimeMap = new HashMap<>();

  // TimeseriesId -> LastTime
  private Map<String, Map<String, Long>> nonAlignedTimeseriesLastTimeMap = new HashMap<>();

  /**
   * if true, this tsfile allow unsequential data when writing; Otherwise, it limits the user to
   * write only sequential data
   */
  private boolean isUnseq = false;

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
      Map<Path, IMeasurementSchema> schemaMap =
          ((RestorableTsFileIOWriter) fileWriter).getKnownSchema();
      Map<Path, MeasurementGroup> measurementGroupMap = new HashMap<>();
      for (Map.Entry<Path, IMeasurementSchema> entry : schemaMap.entrySet()) {
        IMeasurementSchema measurementSchema = entry.getValue();
        if (measurementSchema instanceof VectorMeasurementSchema) {
          MeasurementGroup group =
              measurementGroupMap.getOrDefault(
                  new Path(entry.getKey().getDevice()), new MeasurementGroup(true));
          List<String> measurementList = measurementSchema.getSubMeasurementsList();
          for (int i = 0; i < measurementList.size(); i++) {
            group
                .getMeasurementSchemaMap()
                .put(
                    measurementList.get(i),
                    new MeasurementSchema(
                        measurementList.get(i),
                        measurementSchema.getSubMeasurementsTSDataTypeList().get(i),
                        measurementSchema.getSubMeasurementsTSEncodingList().get(i)));
          }
          measurementGroupMap.put(new Path(entry.getKey().getDevice()), group);
        } else {
          MeasurementGroup group =
              measurementGroupMap.getOrDefault(
                  new Path(entry.getKey().getDevice()), new MeasurementGroup(false));
          group
              .getMeasurementSchemaMap()
              .put(measurementSchema.getMeasurementId(), (MeasurementSchema) measurementSchema);
          measurementGroupMap.put(new Path(entry.getKey().getDevice()), group);
        }
      }
      this.schema = new Schema(measurementGroupMap);
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
      String templateName, Map<String, MeasurementSchema> template, boolean isAligned) {
    schema.registerSchemaTemplate(templateName, new MeasurementGroup(isAligned, template));
  }

  /**
   * This method is used to register all timeseries in the specified template under the specified
   * device.
   *
   * @param deviceId
   * @param templateName
   * @throws WriteProcessException
   */
  public void registerDevice(String deviceId, String templateName) throws WriteProcessException {
    if (!schema.getSchemaTemplates().containsKey(templateName)) {
      throw new WriteProcessException("given template is not existed! " + templateName);
    }
    if (schema.getRegisteredTimeseriesMap().containsKey(new Path(deviceId))) {
      throw new WriteProcessException(
          "this device "
              + deviceId
              + " has been registered, you can only use registerDevice method to register empty device.");
    }
    schema.registerDevice(deviceId, templateName);
  }

  /**
   * Register nonAligned timeseries by single.
   *
   * @param devicePath
   * @param measurementSchema
   * @throws WriteProcessException
   */
  public void registerTimeseries(Path devicePath, MeasurementSchema measurementSchema)
      throws WriteProcessException {
    MeasurementGroup measurementGroup;
    if (schema.containsDevice(devicePath)) {
      measurementGroup = schema.getSeriesSchema(devicePath);
      if (measurementGroup.isAligned()) {
        throw new WriteProcessException(
            "given device " + devicePath + " has been registered for aligned timeseries.");
      } else if (measurementGroup
          .getMeasurementSchemaMap()
          .containsKey(measurementSchema.getMeasurementId())) {
        throw new WriteProcessException(
            "given nonAligned timeseries "
                + (devicePath + "." + measurementSchema.getMeasurementId())
                + " has been registered.");
      }
    } else {
      measurementGroup = new MeasurementGroup(false);
    }
    measurementGroup
        .getMeasurementSchemaMap()
        .put(measurementSchema.getMeasurementId(), measurementSchema);
    schema.registerMeasurementGroup(devicePath, measurementGroup);
  }

  /**
   * Register nonAligned timeseries by groups.
   *
   * @param devicePath
   * @param measurementSchemas
   */
  public void registerTimeseries(Path devicePath, List<MeasurementSchema> measurementSchemas) {
    for (MeasurementSchema schema : measurementSchemas) {
      try {
        registerTimeseries(devicePath, schema);
      } catch (WriteProcessException e) {
        LOG.warn(e.getMessage());
      }
    }
  }

  /**
   * Register aligned timeseries. Once the device is registered for aligned timeseries, it cannot be
   * expanded.
   *
   * @param devicePath
   * @param measurementSchemas
   * @throws WriteProcessException
   */
  public void registerAlignedTimeseries(Path devicePath, List<MeasurementSchema> measurementSchemas)
      throws WriteProcessException {
    if (schema.containsDevice(devicePath)) {
      if (schema.getSeriesSchema(devicePath).isAligned()) {
        throw new WriteProcessException(
            "given device "
                + devicePath
                + " has been registered for aligned timeseries and should not be expanded.");
      } else {
        throw new WriteProcessException(
            "given device " + devicePath + " has been registered for nonAligned timeseries.");
      }
    }
    MeasurementGroup measurementGroup = new MeasurementGroup(true);
    measurementSchemas.forEach(
        measurementSchema -> {
          measurementGroup
              .getMeasurementSchemaMap()
              .put(measurementSchema.getMeasurementId(), measurementSchema);
        });
    schema.registerMeasurementGroup(devicePath, measurementGroup);
  }

  private boolean checkIsTimeseriesExist(TSRecord record, boolean isAligned)
      throws WriteProcessException, IOException {
    // initial ChunkGroupWriter of this device in the TSRecord
    IChunkGroupWriter groupWriter = tryToInitialGroupWriter(record.deviceId, isAligned);

    // initial all SeriesWriters of measurements in this TSRecord
    Path devicePath = new Path(record.deviceId);
    List<MeasurementSchema> measurementSchemas;
    if (schema.containsDevice(devicePath)) {
      measurementSchemas =
          checkIsAllMeasurementsInGroup(
              record.dataPointList, schema.getSeriesSchema(devicePath), isAligned);
      if (isAligned) {
        for (IMeasurementSchema s : measurementSchemas) {
          if (flushedMeasurementsInDeviceMap.containsKey(devicePath.getFullPath())
              && !flushedMeasurementsInDeviceMap
                  .get(devicePath.getFullPath())
                  .contains(s.getMeasurementId())) {
            throw new WriteProcessException(
                "TsFile has flushed chunk group and should not add new measurement "
                    + s.getMeasurementId()
                    + " in device "
                    + devicePath.getFullPath());
          }
        }
      }
      groupWriter.tryToAddSeriesWriter(measurementSchemas);
    } else if (schema.getSchemaTemplates() != null && schema.getSchemaTemplates().size() == 1) {
      // use the default template without needing to register device
      MeasurementGroup measurementGroup =
          schema.getSchemaTemplates().entrySet().iterator().next().getValue();
      measurementSchemas =
          checkIsAllMeasurementsInGroup(record.dataPointList, measurementGroup, isAligned);
      groupWriter.tryToAddSeriesWriter(measurementSchemas);
    } else {
      throw new NoMeasurementException("input devicePath is invalid: " + devicePath);
    }
    return true;
  }

  private void checkIsTimeseriesExist(Tablet tablet, boolean isAligned)
      throws WriteProcessException, IOException {
    IChunkGroupWriter groupWriter = tryToInitialGroupWriter(tablet.deviceId, isAligned);

    Path devicePath = new Path(tablet.deviceId);
    List<MeasurementSchema> schemas = tablet.getSchemas();
    if (schema.containsDevice(devicePath)) {
      checkIsAllMeasurementsInGroup(schema.getSeriesSchema(devicePath), schemas, isAligned);
      if (isAligned) {
        for (IMeasurementSchema s : schemas) {
          if (flushedMeasurementsInDeviceMap.containsKey(devicePath.getFullPath())
              && !flushedMeasurementsInDeviceMap
                  .get(devicePath.getFullPath())
                  .contains(s.getMeasurementId())) {
            throw new WriteProcessException(
                "TsFile has flushed chunk group and should not add new measurement "
                    + s.getMeasurementId()
                    + " in device "
                    + devicePath.getFullPath());
          }
        }
      }
      groupWriter.tryToAddSeriesWriter(schemas);
    } else if (schema.getSchemaTemplates() != null && schema.getSchemaTemplates().size() == 1) {
      MeasurementGroup measurementGroup =
          schema.getSchemaTemplates().entrySet().iterator().next().getValue();
      checkIsAllMeasurementsInGroup(measurementGroup, schemas, isAligned);
      groupWriter.tryToAddSeriesWriter(schemas);
    } else {
      throw new NoMeasurementException("input devicePath is invalid: " + devicePath);
    }
  }

  /**
   * If it's aligned, then all measurementSchemas should be contained in the measurementGroup, or it
   * will throw exception. If it's nonAligned, then remove the measurementSchema that is not
   * contained in the measurementGroup.
   *
   * @param measurementGroup
   * @param measurementSchemas
   * @param isAligned
   * @throws NoMeasurementException
   */
  private void checkIsAllMeasurementsInGroup(
      MeasurementGroup measurementGroup,
      List<MeasurementSchema> measurementSchemas,
      boolean isAligned)
      throws NoMeasurementException {
    if (isAligned && !measurementGroup.isAligned()) {
      throw new NoMeasurementException("no aligned timeseries is registered in the group.");
    } else if (!isAligned && measurementGroup.isAligned()) {
      throw new NoMeasurementException("no nonAligned timeseries is registered in the group.");
    }
    for (MeasurementSchema measurementSchema : measurementSchemas) {
      if (!measurementGroup
          .getMeasurementSchemaMap()
          .containsKey(measurementSchema.getMeasurementId())) {
        if (isAligned) {
          throw new NoMeasurementException(
              "measurement "
                  + measurementSchema.getMeasurementId()
                  + " is not registered or in the default template");
        } else {
          measurementSchemas.remove(measurementSchema);
        }
      }
    }
  }

  /**
   * Check whether all measurements of dataPoints list are in the measurementGroup.
   *
   * @param dataPoints
   * @param measurementGroup
   * @param isAligned
   * @return
   * @throws NoMeasurementException
   */
  private List<MeasurementSchema> checkIsAllMeasurementsInGroup(
      List<DataPoint> dataPoints, MeasurementGroup measurementGroup, boolean isAligned)
      throws NoMeasurementException {
    if (isAligned && !measurementGroup.isAligned()) {
      throw new NoMeasurementException("no aligned timeseries is registered in the group.");
    } else if (!isAligned && measurementGroup.isAligned()) {
      throw new NoMeasurementException("no nonAligned timeseries is registered in the group.");
    }
    List<MeasurementSchema> schemas = new ArrayList<>();
    for (DataPoint dataPoint : dataPoints) {
      if (!measurementGroup.getMeasurementSchemaMap().containsKey(dataPoint.getMeasurementId())) {
        if (isAligned) {
          throw new NoMeasurementException(
              "aligned measurement "
                  + dataPoint.getMeasurementId()
                  + " is not registered or in the default template");
        } else {
          LOG.warn(
              "Ignore nonAligned measurement "
                  + dataPoint.getMeasurementId()
                  + " , because it is not registered or in the default template");
        }
      } else {
        schemas.add(measurementGroup.getMeasurementSchemaMap().get(dataPoint.getMeasurementId()));
      }
    }
    return schemas;
  }

  private IChunkGroupWriter tryToInitialGroupWriter(String deviceId, boolean isAligned) {
    IChunkGroupWriter groupWriter;
    if (!groupWriters.containsKey(deviceId)) {
      if (isAligned) {
        groupWriter = new AlignedChunkGroupWriterImpl(deviceId);
        if (!isUnseq) { // Sequence File
          ((AlignedChunkGroupWriterImpl) groupWriter)
              .setLastTime(alignedDeviceLastTimeMap.getOrDefault(deviceId, -1L));
        }
      } else {
        groupWriter = new NonAlignedChunkGroupWriterImpl(deviceId);
        if (!isUnseq) { // Sequence File
          ((NonAlignedChunkGroupWriterImpl) groupWriter)
              .setLastTimeMap(
                  nonAlignedTimeseriesLastTimeMap.getOrDefault(deviceId, new HashMap<>()));
        }
      }
      groupWriters.put(deviceId, groupWriter);
    } else {
      groupWriter = groupWriters.get(deviceId);
    }
    return groupWriter;
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
    checkIsTimeseriesExist(record, false);
    recordCount += groupWriters.get(record.deviceId).write(record.time, record.dataPointList);
    return checkMemorySizeAndMayFlushChunks();
  }

  public boolean writeAligned(TSRecord record) throws IOException, WriteProcessException {
    checkIsTimeseriesExist(record, true);
    recordCount += groupWriters.get(record.deviceId).write(record.time, record.dataPointList);
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
    checkIsTimeseriesExist(tablet, false);
    // get corresponding ChunkGroupWriter and write this Tablet
    recordCount += groupWriters.get(tablet.deviceId).write(tablet);
    return checkMemorySizeAndMayFlushChunks();
  }

  public boolean writeAligned(Tablet tablet) throws IOException, WriteProcessException {
    // make sure the ChunkGroupWriter for this Tablet exist
    checkIsTimeseriesExist(tablet, true);
    // get corresponding ChunkGroupWriter and write this Tablet
    recordCount += groupWriters.get(tablet.deviceId).write(tablet);
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
        if (groupWriter instanceof AlignedChunkGroupWriterImpl) {
          // add flushed measurements
          List<String> measurementList =
              flushedMeasurementsInDeviceMap.computeIfAbsent(deviceId, p -> new ArrayList<>());
          ((AlignedChunkGroupWriterImpl) groupWriter)
              .getMeasurements()
              .forEach(
                  measurementId -> {
                    if (!measurementList.contains(measurementId)) {
                      measurementList.add(measurementId);
                    }
                  });
          // add lastTime
          if (!isUnseq) { // Sequence TsFile
            this.alignedDeviceLastTimeMap.put(
                deviceId, ((AlignedChunkGroupWriterImpl) groupWriter).getLastTime());
          }
        } else {
          // add lastTime
          if (!isUnseq) { // Sequence TsFile
            this.nonAlignedTimeseriesLastTimeMap.put(
                deviceId, ((NonAlignedChunkGroupWriterImpl) groupWriter).getLastTimeMap());
          }
        }
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
