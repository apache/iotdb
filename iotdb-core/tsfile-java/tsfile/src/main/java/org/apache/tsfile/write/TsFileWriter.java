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
package org.apache.tsfile.write;

import org.apache.tsfile.common.TsFileApi;
import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.encrypt.EncryptParameter;
import org.apache.tsfile.encrypt.IEncryptor;
import org.apache.tsfile.exception.encrypt.EncryptException;
import org.apache.tsfile.exception.write.ConflictDataTypeException;
import org.apache.tsfile.exception.write.NoDeviceException;
import org.apache.tsfile.exception.write.NoMeasurementException;
import org.apache.tsfile.exception.write.NoTableException;
import org.apache.tsfile.exception.write.WriteProcessException;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.TableSchema;
import org.apache.tsfile.read.common.Path;
import org.apache.tsfile.utils.MeasurementGroup;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.utils.WriteUtils;
import org.apache.tsfile.write.chunk.AlignedChunkGroupWriterImpl;
import org.apache.tsfile.write.chunk.IChunkGroupWriter;
import org.apache.tsfile.write.chunk.NonAlignedChunkGroupWriterImpl;
import org.apache.tsfile.write.record.TSRecord;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.record.datapoint.DataPoint;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.Schema;
import org.apache.tsfile.write.writer.RestorableTsFileIOWriter;
import org.apache.tsfile.write.writer.TsFileIOWriter;
import org.apache.tsfile.write.writer.TsFileOutput;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

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

  /** IO writer of this TsFile. */
  private final TsFileIOWriter fileWriter;

  private EncryptParameter encryptParam;

  private final int pageSize;
  private long recordCount = 0;

  // deviceId -> measurementIdList
  private Map<IDeviceID, List<String>> flushedMeasurementsInDeviceMap = new HashMap<>();

  // DeviceId -> LastTime
  private Map<IDeviceID, Long> alignedDeviceLastTimeMap = new HashMap<>();

  // TimeseriesId -> LastTime
  private Map<IDeviceID, Map<String, Long>> nonAlignedTimeseriesLastTimeMap = new HashMap<>();

  /**
   * if true, this tsfile allow unsequential data when writing; Otherwise, it limits the user to
   * write only sequential data
   */
  private boolean isUnseq = false;

  private Map<IDeviceID, IChunkGroupWriter> groupWriters = new TreeMap<>();

  /** min value of threshold of data points num check. */
  private long recordCountForNextMemCheck = 100;

  private long chunkGroupSizeThreshold;

  private boolean isTableWriteAligned = true;

  /**
   * init this TsFileWriter.
   *
   * @param file the File to be written by this TsFileWriter
   */
  @TsFileApi
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
      schema = ((RestorableTsFileIOWriter) fileWriter).getKnownSchema();
    }
    fileWriter.setSchema(schema);

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

    String encryptLevel;
    byte[] encryptKey;
    byte[] dataEncryptKey;
    String encryptType;
    if (config.getEncryptFlag()) {
      encryptLevel = "2";
      encryptType = config.getEncryptType();
      try {
        MessageDigest md = MessageDigest.getInstance("SHA-256");
        md.update("IoTDB is the best".getBytes());
        md.update(config.getEncryptKey().getBytes());
        dataEncryptKey = Arrays.copyOfRange(md.digest(), 0, 16);
        encryptKey =
            IEncryptor.getEncryptor(config.getEncryptType(), config.getEncryptKey().getBytes())
                .encrypt(dataEncryptKey);
      } catch (Exception e) {
        throw new EncryptException(
            "SHA-256 function not found while using SHA-256 to generate data key");
      }
    } else {
      encryptLevel = "0";
      encryptType = "org.apache.tsfile.encrypt.UNENCRYPTED";
      encryptKey = null;
      dataEncryptKey = null;
    }
    this.encryptParam = new EncryptParameter(encryptType, dataEncryptKey);
    if (encryptKey != null) {
      StringBuilder valueStr = new StringBuilder();

      for (byte b : encryptKey) {
        valueStr.append(b).append(",");
      }

      valueStr.deleteCharAt(valueStr.length() - 1);
      String str = valueStr.toString();

      fileWriter.setEncryptParam(encryptLevel, encryptType, str);
    } else {
      fileWriter.setEncryptParam(encryptLevel, encryptType, "");
    }
  }

  public void registerSchemaTemplate(
      String templateName, Map<String, IMeasurementSchema> template, boolean isAligned) {
    getSchema().registerSchemaTemplate(templateName, new MeasurementGroup(isAligned, template));
  }

  /**
   * This method is used to register all timeseries in the specified template under the specified
   * device.
   */
  public void registerDevice(String deviceIdString, String templateName)
      throws WriteProcessException {
    IDeviceID deviceID = IDeviceID.Factory.DEFAULT_FACTORY.create(deviceIdString);
    if (!getSchema().getSchemaTemplates().containsKey(templateName)) {
      throw new WriteProcessException("given template is not existed! " + templateName);
    }
    if (getSchema().getRegisteredTimeseriesMap().containsKey(deviceID)) {
      throw new WriteProcessException(
          "this device "
              + deviceIdString
              + " has been registered, you can only use registerDevice method to register empty device.");
    }
    getSchema().registerDevice(deviceID, templateName);
  }

  @TsFileApi
  public void registerTimeseries(String deviceId, IMeasurementSchema measurementSchema)
      throws WriteProcessException {
    registerTimeseries(IDeviceID.Factory.DEFAULT_FACTORY.create(deviceId), measurementSchema);
  }

  @Deprecated
  public void registerTimeseries(Path devicePath, IMeasurementSchema measurementSchema)
      throws WriteProcessException {
    registerTimeseries(devicePath.getIDeviceID(), measurementSchema);
  }

  /** Register nonAligned timeseries by single. */
  @TsFileApi
  public void registerTimeseries(IDeviceID deviceID, IMeasurementSchema measurementSchema)
      throws WriteProcessException {
    MeasurementGroup measurementGroup;
    if (getSchema().containsDevice(deviceID)) {
      measurementGroup = getSchema().getSeriesSchema(deviceID);
      if (measurementGroup.isAligned()) {
        throw new WriteProcessException(
            "given device " + deviceID + " has been registered for aligned timeseries.");
      } else if (measurementGroup
          .getMeasurementSchemaMap()
          .containsKey(measurementSchema.getMeasurementName())) {
        throw new WriteProcessException(
            "given nonAligned timeseries "
                + (deviceID + "." + measurementSchema.getMeasurementName())
                + " has been registered.");
      }
    } else {
      measurementGroup = new MeasurementGroup(false);
    }
    measurementGroup
        .getMeasurementSchemaMap()
        .put(measurementSchema.getMeasurementName(), measurementSchema);
    getSchema().registerMeasurementGroup(deviceID, measurementGroup);
  }

  @Deprecated
  /** Register nonAligned timeseries by groups. */
  public void registerTimeseries(Path devicePath, List<IMeasurementSchema> measurementSchemas) {
    for (IMeasurementSchema schema : measurementSchemas) {
      try {
        registerTimeseries(devicePath.getIDeviceID(), schema);
      } catch (WriteProcessException e) {
        LOG.warn(e.getMessage());
      }
    }
  }

  @TsFileApi
  public void registerAlignedTimeseries(
      String deviceId, List<IMeasurementSchema> measurementSchemas) throws WriteProcessException {
    registerAlignedTimeseries(
        IDeviceID.Factory.DEFAULT_FACTORY.create(deviceId), measurementSchemas);
  }

  public void registerAlignedTimeseries(
      Path devicePath, List<IMeasurementSchema> measurementSchemas) throws WriteProcessException {
    registerAlignedTimeseries(devicePath.getIDeviceID(), measurementSchemas);
  }

  /**
   * Register aligned timeseries. Once the device is registered for aligned timeseries, it cannot be
   * expanded.
   */
  @TsFileApi
  public void registerAlignedTimeseries(
      IDeviceID deviceID, List<IMeasurementSchema> measurementSchemas)
      throws WriteProcessException {
    if (getSchema().containsDevice(deviceID)) {
      if (getSchema().getSeriesSchema(deviceID).isAligned()) {
        throw new WriteProcessException(
            "given device "
                + deviceID
                + " has been registered for aligned timeseries and should not be expanded.");
      } else {
        throw new WriteProcessException(
            "given device " + deviceID + " has been registered for nonAligned timeseries.");
      }
    }
    MeasurementGroup measurementGroup = new MeasurementGroup(true);
    measurementSchemas.forEach(
        measurementSchema -> {
          measurementGroup
              .getMeasurementSchemaMap()
              .put(measurementSchema.getMeasurementName(), measurementSchema);
        });
    getSchema().registerMeasurementGroup(deviceID, measurementGroup);
  }

  private boolean checkIsTimeseriesExist(TSRecord record, boolean isAligned)
      throws WriteProcessException, IOException {
    // initial ChunkGroupWriter of this device in the TSRecord
    final IDeviceID deviceID = record.deviceId;
    IChunkGroupWriter groupWriter = tryToInitialGroupWriter(deviceID, isAligned);

    // initial all SeriesWriters of measurements in this TSRecord
    List<IMeasurementSchema> measurementSchemas;
    if (getSchema().containsDevice(deviceID)) {
      measurementSchemas =
          checkIsAllMeasurementsInGroup(
              record.dataPointList, getSchema().getSeriesSchema(deviceID), isAligned);
      if (isAligned) {
        for (IMeasurementSchema s : measurementSchemas) {
          if (flushedMeasurementsInDeviceMap.containsKey(deviceID)
              && !flushedMeasurementsInDeviceMap.get(deviceID).contains(s.getMeasurementName())) {
            throw new WriteProcessException(
                "TsFile has flushed chunk group and should not add new measurement "
                    + s.getMeasurementName()
                    + " in device "
                    + deviceID);
          }
        }
      }
      groupWriter.tryToAddSeriesWriter(measurementSchemas);
    } else if (getSchema().getSchemaTemplates() != null
        && getSchema().getSchemaTemplates().size() == 1) {
      // use the default template without needing to register device
      MeasurementGroup measurementGroup =
          getSchema().getSchemaTemplates().entrySet().iterator().next().getValue();
      measurementSchemas =
          checkIsAllMeasurementsInGroup(record.dataPointList, measurementGroup, isAligned);
      groupWriter.tryToAddSeriesWriter(measurementSchemas);
    } else {
      throw new NoDeviceException(deviceID.toString());
    }
    return true;
  }

  private void checkIsTableExistAndSetColumnCategoryList(Tablet tablet) throws WriteProcessException {
    String tableName = tablet.getTableName();
    final TableSchema tableSchema = getSchema().getTableSchemaMap().get(tableName);
    if (tableSchema == null) {
      throw new NoTableException(tableName);
    }

    List<Tablet.ColumnCategory> columnCategoryListForTablet = new ArrayList<>(tablet.getSchemas().size());
    for (IMeasurementSchema writingColumnSchema : tablet.getSchemas()) {
      final int columnIndex = tableSchema.findColumnIndex(writingColumnSchema.getMeasurementName());
      if (columnIndex < 0) {
        throw new NoMeasurementException(writingColumnSchema.getMeasurementName());
      }
      final IMeasurementSchema registeredColumnSchema =
          tableSchema.getColumnSchemas().get(columnIndex);
      if (!writingColumnSchema.getType().equals(registeredColumnSchema.getType())) {
        throw new ConflictDataTypeException(
            writingColumnSchema.getType(), registeredColumnSchema.getType());
      }
      columnCategoryListForTablet.add(tableSchema.getColumnTypes().get(columnIndex));
    }
    tablet.setColumnCategories(columnCategoryListForTablet);
  }

  private void checkIsTimeseriesExist(Tablet tablet, boolean isAligned)
      throws WriteProcessException, IOException {
    final IDeviceID deviceID = IDeviceID.Factory.DEFAULT_FACTORY.create(tablet.getDeviceId());
    IChunkGroupWriter groupWriter = tryToInitialGroupWriter(deviceID, isAligned);

    List<IMeasurementSchema> schemas = tablet.getSchemas();
    if (getSchema().containsDevice(deviceID)) {
      checkIsAllMeasurementsInGroup(getSchema().getSeriesSchema(deviceID), schemas, isAligned);
      if (isAligned) {
        for (IMeasurementSchema s : schemas) {
          if (flushedMeasurementsInDeviceMap.containsKey(deviceID)
              && !flushedMeasurementsInDeviceMap.get(deviceID).contains(s.getMeasurementName())) {
            throw new WriteProcessException(
                "TsFile has flushed chunk group and should not add new measurement "
                    + s.getMeasurementName()
                    + " in device "
                    + deviceID);
          }
        }
      }
      groupWriter.tryToAddSeriesWriter(schemas);
    } else if (getSchema().getSchemaTemplates() != null
        && getSchema().getSchemaTemplates().size() == 1) {
      MeasurementGroup measurementGroup =
          getSchema().getSchemaTemplates().entrySet().iterator().next().getValue();
      checkIsAllMeasurementsInGroup(measurementGroup, schemas, isAligned);
      groupWriter.tryToAddSeriesWriter(schemas);
    } else {
      throw new NoDeviceException(deviceID.toString());
    }
  }

  /**
   * If it's aligned, then all measurementSchemas should be contained in the measurementGroup, or it
   * will throw exception. If it's nonAligned, then remove the measurementSchema that is not
   * contained in the measurementGroup.
   */
  private void checkIsAllMeasurementsInGroup(
      MeasurementGroup measurementGroup,
      List<IMeasurementSchema> measurementSchemas,
      boolean isAligned)
      throws NoMeasurementException {
    if (isAligned && !measurementGroup.isAligned()) {
      throw new NoMeasurementException("aligned");
    } else if (!isAligned && measurementGroup.isAligned()) {
      throw new NoMeasurementException("nonAligned");
    }
    for (IMeasurementSchema measurementSchema : measurementSchemas) {
      if (!measurementGroup
          .getMeasurementSchemaMap()
          .containsKey(measurementSchema.getMeasurementName())) {
        if (isAligned) {
          throw new NoMeasurementException(measurementSchema.getMeasurementName());
        } else {
          measurementSchemas.remove(measurementSchema);
        }
      }
    }
  }

  /** Check whether all measurements of dataPoints list are in the measurementGroup. */
  private List<IMeasurementSchema> checkIsAllMeasurementsInGroup(
      List<DataPoint> dataPoints, MeasurementGroup measurementGroup, boolean isAligned)
      throws NoMeasurementException {
    if (isAligned && !measurementGroup.isAligned()) {
      throw new NoMeasurementException("aligned");
    } else if (!isAligned && measurementGroup.isAligned()) {
      throw new NoMeasurementException("nonAligned");
    }
    List<IMeasurementSchema> schemas = new ArrayList<>();
    for (DataPoint dataPoint : dataPoints) {
      if (!measurementGroup.getMeasurementSchemaMap().containsKey(dataPoint.getMeasurementId())) {
        if (isAligned) {
          throw new NoMeasurementException(dataPoint.getMeasurementId());
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

  private IChunkGroupWriter tryToInitialGroupWriter(IDeviceID deviceId, boolean isAligned) {
    IChunkGroupWriter groupWriter = groupWriters.get(deviceId);
    if (groupWriter == null) {
      if (isAligned) {
        groupWriter = new AlignedChunkGroupWriterImpl(deviceId, encryptParam);
        if (!isUnseq) { // Sequence File
          ((AlignedChunkGroupWriterImpl) groupWriter)
              .setLastTime(alignedDeviceLastTimeMap.get(deviceId));
        }
      } else {
        groupWriter = new NonAlignedChunkGroupWriterImpl(deviceId, encryptParam);
        if (!isUnseq) { // Sequence File
          ((NonAlignedChunkGroupWriterImpl) groupWriter)
              .setLastTimeMap(
                  nonAlignedTimeseriesLastTimeMap.getOrDefault(deviceId, new HashMap<>()));
        }
      }
      groupWriters.put(deviceId, groupWriter);
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
  @TsFileApi
  public boolean writeRecord(TSRecord record) throws IOException, WriteProcessException {
    MeasurementGroup measurementGroup = getSchema().getSeriesSchema(record.deviceId);
    if (measurementGroup == null) {
      throw new NoDeviceException(record.deviceId.toString());
    }
    checkIsTimeseriesExist(record, measurementGroup.isAligned());
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
  @TsFileApi
  public boolean writeTree(Tablet tablet) throws IOException, WriteProcessException {
    IDeviceID deviceID = IDeviceID.Factory.DEFAULT_FACTORY.create(tablet.getDeviceId());
    MeasurementGroup measurementGroup = getSchema().getSeriesSchema(deviceID);
    if (measurementGroup == null) {
      throw new NoDeviceException(deviceID.toString());
    }
    // make sure the ChunkGroupWriter for this Tablet exist
    checkIsTimeseriesExist(tablet, measurementGroup.isAligned());
    // get corresponding ChunkGroupWriter and write this Tablet
    recordCount += groupWriters.get(deviceID).write(tablet);
    return checkMemorySizeAndMayFlushChunks();
  }

  @Deprecated
  public boolean writeAligned(Tablet tablet) throws IOException, WriteProcessException {
    // make sure the ChunkGroupWriter for this Tablet exist
    checkIsTimeseriesExist(tablet, true);
    // get corresponding ChunkGroupWriter and write this Tablet
    recordCount +=
        groupWriters
            .get(IDeviceID.Factory.DEFAULT_FACTORY.create(tablet.getDeviceId()))
            .write(tablet);
    return checkMemorySizeAndMayFlushChunks();
  }

  /**
   * calculate total memory size occupied by allT ChunkGroupWriter instances currently.
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
        return flush();
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
  @TsFileApi
  public boolean flush() throws IOException {
    if (recordCount > 0) {
      for (Map.Entry<IDeviceID, IChunkGroupWriter> entry : groupWriters.entrySet()) {
        IDeviceID deviceId = entry.getKey();
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
  @TsFileApi
  public void close() throws IOException {
    LOG.info("start close file");
    flush();
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

  public Schema getSchema() {
    return fileWriter.getSchema();
  }

  /**
   * Write the tablet in to the TsFile with the table-view. The method will try to split the tablet
   * by device. If you know the device association within the tablet, please use writeTable(Tablet
   * tablet, List<Pair<IDeviceID, Integer>> deviceIdEndIndexPairs). One typical case where the other
   * method should be used is that all rows in the tablet belong to the same device.
   *
   * @param table data to write
   * @return true if a flush is triggered after write, false otherwise
   * @throws IOException if the file cannot be written
   * @throws WriteProcessException if the schema is not registered first
   */
  @TsFileApi
  public boolean writeTable(Tablet table) throws IOException, WriteProcessException {
    return writeTable(table, null);
  }

  /**
   * Write the tablet in to the TsFile with the table-view.
   *
   * @param tablet data to write
   * @param deviceIdEndIndexPairs each deviceId and its end row number in row order. For example, if
   *     the first three rows belong to device ("table1", "d1"), the next five rows belong to device
   *     ("table1", "d2"), and the last two rows belong to device ("table1", "d3"), then the list
   *     will be [(("table1", "d1"), 3), (("table1", "d2"), 8), (("table1", "d3"), 10)]. If the list
   *     is not provided, the method will try to split the tablet.
   * @return true if a flush is triggered after write, false otherwise
   * @throws IOException if the file cannot be written
   * @throws WriteProcessException if the schema is not registered first
   */
  public boolean writeTable(Tablet tablet, List<Pair<IDeviceID, Integer>> deviceIdEndIndexPairs)
      throws IOException, WriteProcessException {
    // make sure the ChunkGroupWriter for this Tablet exist and there is no type conflict
    checkIsTableExistAndSetColumnCategoryList(tablet);
    // spilt the tablet by deviceId
    if (deviceIdEndIndexPairs == null) {
      deviceIdEndIndexPairs = WriteUtils.splitTabletByDevice(tablet);
    }

    int startIndex = 0;
    for (Pair<IDeviceID, Integer> pair : deviceIdEndIndexPairs) {
      // get corresponding ChunkGroupWriter and write this Tablet
      recordCount +=
          tryToInitialGroupWriter(pair.left, isTableWriteAligned)
              .write(tablet, startIndex, pair.right);
      startIndex = pair.right;
    }
    return checkMemorySizeAndMayFlushChunks();
  }

  public boolean isTableWriteAligned() {
    return isTableWriteAligned;
  }

  public void setTableWriteAligned(boolean tableWriteAligned) {
    isTableWriteAligned = tableWriteAligned;
  }

  public void registerTableSchema(TableSchema tableSchema) {
    getSchema().registerTableSchema(tableSchema);
  }

  public boolean isGenerateTableSchemaForTree() {
    return getIOWriter().isGenerateTableSchema();
  }

  public void setGenerateTableSchema(boolean generateTableSchema) {
    this.getIOWriter().setGenerateTableSchema(generateTableSchema);
  }
}
