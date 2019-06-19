/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.db.engine.filenodeV2;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.conf.directories.Directories;
import org.apache.iotdb.db.engine.UnsealedTsFileProcessorV2;
import org.apache.iotdb.db.engine.filenode.CopyOnWriteLinkedList;
import org.apache.iotdb.db.engine.filenode.FileNodeProcessorStatus;
import org.apache.iotdb.db.engine.querycontext.GlobalSortedSeriesDataSourceV2;
import org.apache.iotdb.db.engine.querycontext.QueryDataSourceV2;
import org.apache.iotdb.db.engine.querycontext.ReadOnlyMemChunk;
import org.apache.iotdb.db.engine.querycontext.UnsealedTsFileV2;
import org.apache.iotdb.db.engine.version.SimpleFileVersionController;
import org.apache.iotdb.db.engine.version.VersionController;
import org.apache.iotdb.db.exception.FileNodeProcessorException;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetaData;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.write.record.TSRecord;
import org.apache.iotdb.tsfile.write.schema.FileSchema;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileNodeProcessorV2 {

  private static final Logger LOGGER = LoggerFactory.getLogger(FileNodeProcessorV2.class);

  private static final String RESTORE_FILE_SUFFIX = ".restore";

  private static final MManager mManager = MManager.getInstance();
  private static final Directories directories = Directories.getInstance();

  private FileSchema fileSchema;

  // for bufferwrite
  //includes sealed and unsealed tsfiles
  private List<TsFileResourceV2> sequenceFileList;
  private UnsealedTsFileProcessorV2 workBufferWriteProcessor = null;
  private CopyOnWriteLinkedList<UnsealedTsFileProcessorV2> closingBufferWriteProcessor = new CopyOnWriteLinkedList<>();

  // for overflow
  private List<TsFileResourceV2> unSequenceFileList;
  private UnsealedTsFileProcessorV2 workOverflowProcessor = null;

  private CopyOnWriteLinkedList<UnsealedTsFileProcessorV2> closingOverflowProcessor = new CopyOnWriteLinkedList<>();

  /**
   * device -> global latest timestamp of each device
   */
  private Map<String, Long> latestTimeMap;

  /**
   * device -> largest timestamp of the latest memtable to be submitted to asyncFlush
   */
  private Map<String, Long> latestFlushTimeMap = new HashMap<>();

  private String storageGroup;

  private final ReadWriteLock lock;

  private VersionController versionController;

  private String fileNodeRestoreFilePath;
  private FileNodeProcessorStoreV2 fileNodeProcessorStore;
  private final Object fileNodeRestoreLock = new Object();

  public FileNodeProcessorV2(String baseDir, String storageGroup)
      throws FileNodeProcessorException {
    this.storageGroup = storageGroup;
    lock = new ReentrantReadWriteLock();

    File storageGroupDir = new File(baseDir + storageGroup);
    if (!storageGroupDir.exists()) {
      storageGroupDir.mkdir();
      LOGGER.info("The directory of the storage group {} doesn't exist. Create a new " +
          "directory {}", storageGroup, storageGroupDir.getAbsolutePath());
    }

    /**
     * restore
     */
    File restoreFolder = new File(baseDir + storageGroup);
    if (!restoreFolder.exists()) {
      restoreFolder.mkdirs();
      LOGGER.info("The restore directory of the filenode processor {} doesn't exist. Create new " +
          "directory {}", storageGroup, restoreFolder.getAbsolutePath());
    }

    fileNodeRestoreFilePath = new File(restoreFolder, storageGroup + RESTORE_FILE_SUFFIX).getPath();

    try {
      fileNodeProcessorStore = readStoreFromDiskOrCreate();
    } catch (FileNodeProcessorException e) {
      LOGGER.error("The fileNode processor {} encountered an error when recovering restore " +
          "information.", storageGroup);
      throw new FileNodeProcessorException(e);
    }

    // TODO deep clone the lastupdate time, change the getSequenceFileList to V2
    sequenceFileList = fileNodeProcessorStore.getSequenceFileList();
    unSequenceFileList = fileNodeProcessorStore.getUnSequenceFileList();
    latestTimeMap = fileNodeProcessorStore.getLatestTimeMap();

    /**
     * version controller
     */
    try {
      versionController = new SimpleFileVersionController(restoreFolder.getPath());
    } catch (IOException e) {
      throw new FileNodeProcessorException(e);
    }

    // construct the file schema
    this.fileSchema = constructFileSchema(storageGroup);
  }

  private FileSchema constructFileSchema(String storageGroupName) {
    List<MeasurementSchema> columnSchemaList;
    columnSchemaList = mManager.getSchemaForFileName(storageGroupName);

    FileSchema schema = new FileSchema();
    for (MeasurementSchema measurementSchema : columnSchemaList) {
      schema.registerMeasurement(measurementSchema);
    }
    return schema;

  }


  /**
   * add time series.
   */
  public void addTimeSeries(String measurementId, TSDataType dataType, TSEncoding encoding,
      CompressionType compressor, Map<String, String> props) {
    lock.writeLock().lock();
    try {
      fileSchema.registerMeasurement(new MeasurementSchema(measurementId, dataType, encoding,
          compressor, props));
    } finally {
      lock.writeLock().unlock();
    }
  }


  /**
   * read file node store from disk or create a new one
   */
  private FileNodeProcessorStoreV2 readStoreFromDiskOrCreate() throws FileNodeProcessorException {

    synchronized (fileNodeRestoreLock) {
      File restoreFile = new File(fileNodeRestoreFilePath);
      if (!restoreFile.exists() || restoreFile.length() == 0) {
        return new FileNodeProcessorStoreV2(false, new HashMap<>(),
            new ArrayList<>(), FileNodeProcessorStatus.NONE, 0);
      }
      try (FileInputStream inputStream = new FileInputStream(fileNodeRestoreFilePath)) {
        return FileNodeProcessorStoreV2.deSerialize(inputStream);
      } catch (IOException e) {
        LOGGER
            .error("Failed to deserialize the FileNodeRestoreFile {}, {}", fileNodeRestoreFilePath,
                e);
        throw new FileNodeProcessorException(e);
      }
    }
  }

  private void writeStoreToDisk(FileNodeProcessorStoreV2 fileNodeProcessorStore)
      throws FileNodeProcessorException {

    synchronized (fileNodeRestoreLock) {
      try (FileOutputStream fileOutputStream = new FileOutputStream(fileNodeRestoreFilePath)) {
        fileNodeProcessorStore.serialize(fileOutputStream);
        LOGGER.debug("The filenode processor {} writes restore information to the restore file",
            storageGroup);
      } catch (IOException e) {
        throw new FileNodeProcessorException(e);
      }
    }
  }


  public boolean insert(TSRecord tsRecord) {
    lock.writeLock().lock();
    boolean result;

    try {
      // init map
      latestTimeMap.putIfAbsent(tsRecord.deviceId, Long.MIN_VALUE);
      latestFlushTimeMap.putIfAbsent(tsRecord.deviceId, Long.MIN_VALUE);

      // write to sequence or unsequence file
      if (tsRecord.time > latestFlushTimeMap.get(tsRecord.deviceId)) {
        result = writeUnsealedDataFile(workBufferWriteProcessor, tsRecord, true);
      } else {
        result = writeUnsealedDataFile(workOverflowProcessor, tsRecord, false);
      }
    } catch (Exception e) {
      LOGGER.error("insert tsRecord to unsealed data file failed, because {}", e.getMessage(), e);
      result = false;
    } finally {
      lock.writeLock().unlock();
    }

    return result;
  }

  private boolean writeUnsealedDataFile(UnsealedTsFileProcessorV2 unsealedTsFileProcessor,
      TSRecord tsRecord, boolean sequence) throws IOException {
    boolean result;
    // create a new BufferWriteProcessor
    if (unsealedTsFileProcessor == null) {
      if (sequence) {
        String baseDir = directories.getNextFolderForTsfile();
        String filePath = Paths.get(baseDir, storageGroup, tsRecord.time + "").toString();
        unsealedTsFileProcessor = new UnsealedTsFileProcessorV2(storageGroup, new File(filePath),
            fileSchema, versionController, this::closeBufferWriteProcessorCallBack);
        sequenceFileList.add(unsealedTsFileProcessor.getTsFileResource());
      } else {
        // TODO check if the disk is full
        String baseDir = IoTDBDescriptor.getInstance().getConfig().getOverflowDataDir();
        String filePath = Paths.get(baseDir, storageGroup, tsRecord.time + "").toString();
        unsealedTsFileProcessor = new UnsealedTsFileProcessorV2(storageGroup, new File(filePath),
            fileSchema, versionController, this::closeBufferWriteProcessorCallBack);
        unSequenceFileList.add(unsealedTsFileProcessor.getTsFileResource());
      }
    }

    // write BufferWrite
    result = unsealedTsFileProcessor.write(tsRecord);

    // try to update the latest time of the device of this tsRecord
    if (result && latestTimeMap.get(tsRecord.deviceId) < tsRecord.time) {
      latestTimeMap.put(tsRecord.deviceId, tsRecord.time);
    }

    // check memtable size and may asyncFlush the workMemtable
    if (unsealedTsFileProcessor.shouldFlush()) {
      flushAndCheckClose(unsealedTsFileProcessor, sequence);
    }

    return result;
  }


  public QueryDataSourceV2 query(String deviceId, String measurementId) {

    List<TsFileResourceV2> sequnceResources = getFileReSourceListForQuery(sequenceFileList, deviceId, measurementId);
    List<TsFileResourceV2> unsequnceResources = getFileReSourceListForQuery(unSequenceFileList, deviceId, measurementId);

    return new QueryDataSourceV2(new GlobalSortedSeriesDataSourceV2(new Path(deviceId, measurementId), sequnceResources),
        new GlobalSortedSeriesDataSourceV2(new Path(deviceId, measurementId), unsequnceResources));

  }


  /**
   *
   * @param tsFileResources includes sealed and unsealed tsfile resources
   * @return fill unsealed tsfile resources with memory data and ChunkMetadataList of data in disk
   */
  private List<TsFileResourceV2> getFileReSourceListForQuery(List<TsFileResourceV2> tsFileResources, String deviceId, String measurementId) {

    MeasurementSchema mSchema = fileSchema.getMeasurementSchema(measurementId);
    TSDataType dataType = mSchema.getType();

    List<TsFileResourceV2> tsfileResourcesForQuery = new ArrayList<>();
    for (TsFileResourceV2 tsFileResource : tsFileResources) {
      synchronized (tsFileResource) {
        if (!tsFileResource.getStartTimeMap().isEmpty()) {
          if (!tsFileResource.isClosed()) {
            tsfileResourcesForQuery.add(tsFileResource);
          } else {
            Pair<ReadOnlyMemChunk, List<ChunkMetaData>> pair = tsFileResource
                .getUnsealedFileProcessor()
                .queryUnsealedFile(deviceId, measurementId, dataType, mSchema.getProps());
            tsfileResourcesForQuery
                .add(new UnsealedTsFileV2(tsFileResource.getFile(), pair.left, pair.right));
          }
        }
      }
    }
    return tsfileResourcesForQuery;
  }



  /**
   * ensure there must be a flush thread submitted after close() is called, therefore the close task
   * will be executed by a flush thread. -- said by qiaojialin
   *
   * only called by insert(), thread-safety should be ensured by caller
   */
  private void flushAndCheckClose(UnsealedTsFileProcessorV2 unsealedTsFileProcessor,
      boolean sequence) {
    boolean shouldClose = false;
    // check file size and may close the BufferWrite
    if (unsealedTsFileProcessor.shouldClose()) {
      if (sequence) {
        closingBufferWriteProcessor.add(unsealedTsFileProcessor);
      } else {
        closingOverflowProcessor.add(unsealedTsFileProcessor);
      }
      unsealedTsFileProcessor.close();
      shouldClose = true;
    }

    unsealedTsFileProcessor.asyncFlush();

    if (shouldClose) {
      if (sequence) {
        workBufferWriteProcessor = null;
      } else {
        workOverflowProcessor = null;
      }
    }

    // update the largest timestamp in the last flushing memtable
    for (Entry<String, Long> entry : latestTimeMap.entrySet()) {
      latestFlushTimeMap.put(entry.getKey(), entry.getValue());
    }
  }


  /**
   * return the memtable to MemTablePool and make metadata in writer visible
   */
  private void closeBufferWriteProcessorCallBack(Object bufferWriteProcessor) {
    closingBufferWriteProcessor.remove((UnsealedTsFileProcessorV2) bufferWriteProcessor);
    synchronized (fileNodeProcessorStore) {
      fileNodeProcessorStore.setLatestTimeMap(latestTimeMap);

      if (!sequenceFileList.isEmpty()) {
        // end time with one start time
        Map<String, Long> endTimeMap = new HashMap<>();
        TsFileResourceV2 resource = workBufferWriteProcessor.getTsFileResource();
        synchronized (resource) {
          for (Entry<String, Long> startTime : resource.getStartTimeMap().entrySet()) {
            String deviceId = startTime.getKey();
            endTimeMap.put(deviceId, latestTimeMap.get(deviceId));
          }
          resource.setEndTimeMap(endTimeMap);
        }
      }
      fileNodeProcessorStore.setSequenceFileList(sequenceFileList);
      try {
        writeStoreToDisk(fileNodeProcessorStore);
      } catch (FileNodeProcessorException e) {
        LOGGER.error("write FileNodeStore info error, because {}", e.getMessage(), e);
      }
    }
  }

  public void forceClose() {
    lock.writeLock().lock();
    try {
      if (workBufferWriteProcessor != null) {
        closingBufferWriteProcessor.add(workBufferWriteProcessor);
        workBufferWriteProcessor.forceClose();
        workBufferWriteProcessor = null;
      }
    } finally {
      lock.writeLock().unlock();
    }
  }
}
