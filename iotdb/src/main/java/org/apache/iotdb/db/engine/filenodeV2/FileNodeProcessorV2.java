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

import static org.apache.iotdb.tsfile.common.constant.SystemConstant.TSFILE_SUFFIX;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.conf.directories.DirectoryManager;
import org.apache.iotdb.db.engine.modification.Deletion;
import org.apache.iotdb.db.engine.modification.ModificationFile;
import org.apache.iotdb.db.engine.querycontext.QueryDataSourceV2;
import org.apache.iotdb.db.engine.querycontext.ReadOnlyMemChunk;
import org.apache.iotdb.db.engine.version.SimpleFileVersionController;
import org.apache.iotdb.db.engine.version.VersionController;
import org.apache.iotdb.db.exception.FileNodeProcessorException;
import org.apache.iotdb.db.exception.ProcessorException;
import org.apache.iotdb.db.exception.UnsealedTsFileProcessorException;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.qp.physical.crud.DeletePlan;
import org.apache.iotdb.db.qp.physical.crud.InsertPlan;
import org.apache.iotdb.db.utils.datastructure.TVListAllocator;
import org.apache.iotdb.db.writelog.recover.TsFileRecoverPerformer;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetaData;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.write.schema.FileSchema;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileNodeProcessorV2 {

  private static final Logger LOGGER = LoggerFactory.getLogger(FileNodeProcessorV2.class);

  private FileSchema fileSchema;

  // includes sealed and unsealed sequence tsfiles
  private List<TsFileResourceV2> sequenceFileList = new ArrayList<>();
  private UnsealedTsFileProcessorV2 workSequenceTsFileProcessor = null;
  private CopyOnReadLinkedList<UnsealedTsFileProcessorV2> closingSequenceTsFileProcessor = new CopyOnReadLinkedList<>();

  // includes sealed and unsealed unSequnce tsfiles
  private List<TsFileResourceV2> unSequenceFileList = new ArrayList<>();
  private UnsealedTsFileProcessorV2 workUnSequenceTsFileProcessor = null;
  private CopyOnReadLinkedList<UnsealedTsFileProcessorV2> closingUnSequenceTsFileProcessor = new CopyOnReadLinkedList<>();

  /**
   * device -> global latest timestamp of each device
   */
  private Map<String, Long> latestTimeForEachDevice = new HashMap<>();

  /**
   * device -> largest timestamp of the latest memtable to be submitted to asyncFlush
   */
  private Map<String, Long> latestFlushedTimeForEachDevice = new HashMap<>();

  private String storageGroupName;

  private final ReadWriteLock lock = new ReentrantReadWriteLock();

  private final Object closeFileNodeCondition = new Object();

  private final ThreadLocal<Long> timerr = new ThreadLocal<>();

  /**
   * Mark whether to close file node
   */
  private volatile boolean toBeClosed;

  private VersionController versionController;

  private ReentrantLock mergeDeleteLock = new ReentrantLock();

  /**
   * This is the modification file of the result of the current merge.
   */
  private ModificationFile mergingModification;

  private TVListAllocator allocator = new TVListAllocator();

  public FileNodeProcessorV2(String baseDir, String storageGroupName) throws ProcessorException {
    this.storageGroupName = storageGroupName;

    // construct the file schema
    this.fileSchema = constructFileSchema(storageGroupName);

    /**
     * version controller
     */
    try {
      File storageGroupInfoDir = new File(baseDir, storageGroupName);
      if (storageGroupInfoDir.mkdirs()) {
        LOGGER.info("Storage Group Info Directory {} doesn't exist, create it",
            storageGroupInfoDir.getPath());
      }

      versionController = new SimpleFileVersionController(
          storageGroupInfoDir.getPath());
    } catch (IOException e) {
      throw new FileNodeProcessorException(e);
    }

    recover();
  }

  private void recover() throws ProcessorException {
    LOGGER.info("recover FileNodeProcessor {}", storageGroupName);
    List<File> tsFiles = new ArrayList<>();
    List<String> seqFileFolders = DirectoryManager.getInstance().getAllTsFileFolders();
    for (String baseDir : seqFileFolders) {
      File fileFolder = new File(baseDir, storageGroupName);
      if (!fileFolder.exists()) {
        continue;
      }
      for (File tsfile : fileFolder.listFiles(file -> file.getName().endsWith(TSFILE_SUFFIX))) {
        tsFiles.add(tsfile);
      }
    }
    recoverSeqFiles(tsFiles);

    tsFiles.clear();
    List<String> unseqFileFolder = DirectoryManager.getInstance().getAllOverflowFileFolders();
    for (String baseDir : unseqFileFolder) {
      File fileFolder = new File(baseDir, storageGroupName);
      if (!fileFolder.exists()) {
        continue;
      }
      for (File tsfile : fileFolder.listFiles(file -> file.getName().endsWith(TSFILE_SUFFIX))) {
        tsFiles.add(tsfile);
      }
    }
    recoverUnseqFiles(tsFiles);

    for (TsFileResourceV2 resource : sequenceFileList) {
      latestTimeForEachDevice.putAll(resource.getEndTimeMap());
      latestFlushedTimeForEachDevice.putAll(resource.getEndTimeMap());
    }
  }

  private void recoverSeqFiles(List<File> tsfiles) throws ProcessorException {
    tsfiles.sort(new CompareFileName());
    for (File tsfile : tsfiles) {
      TsFileResourceV2 tsFileResource = new TsFileResourceV2(tsfile);
      sequenceFileList.add(tsFileResource);
      TsFileRecoverPerformer recoverPerformer = new TsFileRecoverPerformer(storageGroupName + "-"
          , fileSchema, versionController, tsFileResource, false);
      recoverPerformer.recover();
    }
  }

  private void recoverUnseqFiles(List<File> tsfiles) throws ProcessorException {
    tsfiles.sort(new CompareFileName());
    for (File tsfile : tsfiles) {
      TsFileResourceV2 tsFileResource = new TsFileResourceV2(tsfile);
      unSequenceFileList.add(tsFileResource);
      TsFileRecoverPerformer recoverPerformer = new TsFileRecoverPerformer(storageGroupName + "-",
          fileSchema,
          versionController, tsFileResource, true);
      recoverPerformer.recover();
    }
  }

  class CompareFileName implements Comparator<File> {

    @Override
    public int compare(File o1, File o2) {
      String[] items1 = o1.getName().split("-");
      String[] items2 = o2.getName().split("-");
      if (Long.valueOf(items1[0]) - Long.valueOf(items2[0]) == 0) {
        return Long.compare(Long.valueOf(items1[1]), Long.valueOf(items2[1]));
      } else {
        return Long.compare(Long.valueOf(items1[0]), Long.valueOf(items2[0]));
      }
    }
  }

  private FileSchema constructFileSchema(String storageGroupName) {
    List<MeasurementSchema> columnSchemaList;
    columnSchemaList = MManager.getInstance().getSchemaForFileName(storageGroupName);

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
    writeLock();
    try {
      fileSchema.registerMeasurement(new MeasurementSchema(measurementId, dataType, encoding,
          compressor, props));
    } finally {
      writeUnlock();
    }
  }

  /**
   * @return -1: failed, 1: Overflow, 2:Bufferwrite
   */
  public boolean insert(InsertPlan insertPlan) {
    writeLock();
    try {
      if (toBeClosed) {
        throw new FileNodeProcessorException(
            "storage group " + storageGroupName + " is to be closed, this insertion is rejected");
      }
      // init map
      latestTimeForEachDevice.putIfAbsent(insertPlan.getDeviceId(), Long.MIN_VALUE);
      latestFlushedTimeForEachDevice.putIfAbsent(insertPlan.getDeviceId(), Long.MIN_VALUE);

      // insert to sequence or unSequence file
      if (insertPlan.getTime() > latestFlushedTimeForEachDevice.get(insertPlan.getDeviceId())) {
        return insertUnsealedDataFile(insertPlan, true);
      } else {
        return insertUnsealedDataFile(insertPlan, false);
      }
    } catch (FileNodeProcessorException | IOException e) {
      LOGGER.error("insert tsRecord to unsealed data file failed, because {}", e.getMessage(), e);
      return false;
    } finally {
      writeUnlock();
    }
  }

  private boolean insertUnsealedDataFile(InsertPlan insertPlan, boolean sequence)
      throws IOException {
    UnsealedTsFileProcessorV2 unsealedTsFileProcessor;
    boolean result;
    // create a new BufferWriteProcessor
    long start1 = System.currentTimeMillis();
    if (sequence) {
      if (workSequenceTsFileProcessor == null) {
        workSequenceTsFileProcessor = createTsFileProcessor(true);
        sequenceFileList.add(workSequenceTsFileProcessor.getTsFileResource());
      }
      unsealedTsFileProcessor = workSequenceTsFileProcessor;
    } else {
      if (workUnSequenceTsFileProcessor == null) {
        workUnSequenceTsFileProcessor = createTsFileProcessor(false);
        unSequenceFileList.add(workUnSequenceTsFileProcessor.getTsFileResource());
      }
      unsealedTsFileProcessor = workUnSequenceTsFileProcessor;
    }
    start1 = System.currentTimeMillis() - start1;
    if (start1 > 1000) {
      LOGGER.info("FNP {} create a new unsealed file processor cost: {}", storageGroupName, start1);
    }

    // insert BufferWrite
    long start2 = System.currentTimeMillis();
    result = unsealedTsFileProcessor.insert(insertPlan, sequence);
    start2 = System.currentTimeMillis() - start2;
    if (start2 > 1000) {
      LOGGER.info("FNP {} insert a record into unsealed file processor cost: {}", storageGroupName,
          start2);
    }

    // try to update the latest time of the device of this tsRecord
    if (result && latestTimeForEachDevice.get(insertPlan.getDeviceId()) < insertPlan.getTime()) {
      latestTimeForEachDevice.put(insertPlan.getDeviceId(), insertPlan.getTime());
    }

    // check memtable size and may asyncFlush the workMemtable
    long time1 = System.currentTimeMillis();
    if (unsealedTsFileProcessor.shouldFlush()) {

      LOGGER.info("The memtable size {} reaches the threshold, async flush it to tsfile: {}",
          unsealedTsFileProcessor.getWorkMemTableMemory(),
          unsealedTsFileProcessor.getTsFileResource().getFile().getAbsolutePath());

      if (unsealedTsFileProcessor.shouldClose()) {
        asyncCloseTsFileProcessor(unsealedTsFileProcessor, sequence);
      } else {
        unsealedTsFileProcessor.asyncFlush();
      }
    }
    time1 = System.currentTimeMillis() - time1;
    if (time1 > 1000) {
      LOGGER.info("FNP {} check flush and close cost: {}ms", storageGroupName, time1);
    }

    return result;
  }

  private UnsealedTsFileProcessorV2 createTsFileProcessor(boolean sequence) throws IOException {
    String baseDir;
    if (sequence) {
      baseDir = DirectoryManager.getInstance().getNextFolderForSequenceFile();
    } else {
      baseDir = DirectoryManager.getInstance().getNextFolderForUnSequenceFile();
    }
    new File(baseDir, storageGroupName).mkdirs();

    String filePath = Paths.get(baseDir, storageGroupName,
        System.currentTimeMillis() + "-" + versionController.nextVersion()).toString()
        + TSFILE_SUFFIX;

    if (sequence) {
      return new UnsealedTsFileProcessorV2(storageGroupName, new File(filePath),
          fileSchema, versionController, this::closeUnsealedTsFileProcessorCallback,
          this::updateLatestFlushTimeCallback, allocator);
    } else {
      return new UnsealedTsFileProcessorV2(storageGroupName, new File(filePath),
          fileSchema, versionController, this::closeUnsealedTsFileProcessorCallback,
          () -> true, allocator);
    }
  }

  /**
   * only called by insert(), thread-safety should be ensured by caller
   */
  private void asyncCloseTsFileProcessor(UnsealedTsFileProcessorV2 unsealedTsFileProcessor,
      boolean sequence) {

    // check file size and may close the BufferWrite
    if (sequence) {
      closingSequenceTsFileProcessor.add(unsealedTsFileProcessor);
      workSequenceTsFileProcessor = null;
      updateEndTimeMap(unsealedTsFileProcessor);
    } else {
      closingUnSequenceTsFileProcessor.add(unsealedTsFileProcessor);
      workUnSequenceTsFileProcessor = null;
    }

    // async close tsfile
    unsealedTsFileProcessor.asyncClose();

    LOGGER.info("The file size {} reaches the threshold, async close tsfile: {}.",
        unsealedTsFileProcessor.getTsFileResource().getFileSize(),
        unsealedTsFileProcessor.getTsFileResource().getFile().getAbsolutePath());
  }


  // TODO need a read lock, please consider the concurrency with flush manager threads.
  public QueryDataSourceV2 query(String deviceId, String measurementId)
      throws FileNodeProcessorException {
    lock.readLock().lock();
    try {
      List<TsFileResourceV2> seqResources = getFileReSourceListForQuery(sequenceFileList,
          deviceId, measurementId);
      List<TsFileResourceV2> unseqResources = getFileReSourceListForQuery(unSequenceFileList,
          deviceId, measurementId);
      return new QueryDataSourceV2(new Path(deviceId, measurementId), seqResources, unseqResources);
    } finally {
      lock.readLock().unlock();
    }
  }

  private void writeLock() {
    long time = System.currentTimeMillis();
    lock.writeLock().lock();
    time = System.currentTimeMillis() - time;
    if (time > 1000) {
      LOGGER.info("storage group {} wait for write lock cost: {}", storageGroupName, time,
          new RuntimeException());
    }
    timerr.set(System.currentTimeMillis());
  }

  private void writeUnlock() {
    lock.writeLock().unlock();
    long time = System.currentTimeMillis() - timerr.get();
    if (time > 1000) {
      LOGGER.info("storage group {} take lock for {}ms", storageGroupName, time,
          new RuntimeException());
    }
  }


  /**
   * @param tsFileResources includes sealed and unsealed tsfile resources
   * @return fill unsealed tsfile resources with memory data and ChunkMetadataList of data in disk
   */
  private List<TsFileResourceV2> getFileReSourceListForQuery(List<TsFileResourceV2> tsFileResources,
      String deviceId, String measurementId) throws FileNodeProcessorException {

    MeasurementSchema mSchema = fileSchema.getMeasurementSchema(measurementId);
    TSDataType dataType = mSchema.getType();

    List<TsFileResourceV2> tsfileResourcesForQuery = new ArrayList<>();
    for (TsFileResourceV2 tsFileResource : tsFileResources) {
      if (!tsFileResource.containsDevice(deviceId)) {
        continue;
      }
      synchronized (tsFileResource) {
        if (!tsFileResource.getStartTimeMap().isEmpty()) {
          if (tsFileResource.isClosed()) {
            tsfileResourcesForQuery.add(tsFileResource);
          } else {
            Pair<ReadOnlyMemChunk, List<ChunkMetaData>> pair;
            try {
              pair = tsFileResource
                  .getUnsealedFileProcessor()
                  .query(deviceId, measurementId, dataType, mSchema.getProps());
            } catch (UnsealedTsFileProcessorException e) {
              throw new FileNodeProcessorException(e);
            }
            tsfileResourcesForQuery
                .add(new TsFileResourceV2(tsFileResource.getFile(), pair.left, pair.right));
          }
        }
      }
    }
    return tsfileResourcesForQuery;
  }


  /**
   * Delete data whose timestamp <= 'timestamp' and belong to timeseries deviceId.measurementId.
   *
   * @param deviceId the deviceId of the timeseries to be deleted.
   * @param measurementId the measurementId of the timeseries to be deleted.
   * @param timestamp the delete range is (0, timestamp].
   */
  public void delete(String deviceId, String measurementId, long timestamp) throws IOException {
    // TODO: how to avoid partial deletion?
    writeLock();

    // record what files are updated so we can roll back them in case of exception
    List<ModificationFile> updatedModFiles = new ArrayList<>();

    try {
      Long lastUpdateTime = latestTimeForEachDevice.get(deviceId);
      // no tsfile data, the delete operation is invalid
      if (lastUpdateTime == null || lastUpdateTime == Long.MIN_VALUE) {
        LOGGER.debug("No device {} in SG {}, deletion invalid", deviceId, storageGroupName);
        return;
      }

      // write log
      if (IoTDBDescriptor.getInstance().getConfig().isEnableWal()) {
        if (workSequenceTsFileProcessor != null) {
          workSequenceTsFileProcessor.getLogNode()
              .write(new DeletePlan(timestamp, new Path(deviceId, measurementId)));
        }
        if (workUnSequenceTsFileProcessor != null) {
          workUnSequenceTsFileProcessor.getLogNode()
              .write(new DeletePlan(timestamp, new Path(deviceId, measurementId)));
        }
      }

      Path fullPath = new Path(deviceId, measurementId);
      Deletion deletion = new Deletion(fullPath, versionController.nextVersion(), timestamp);
      if (mergingModification != null) {
        mergingModification.write(deletion);
        updatedModFiles.add(mergingModification);
      }

      deleteFiles(sequenceFileList, deletion, updatedModFiles);
      deleteFiles(unSequenceFileList, deletion, updatedModFiles);

    } catch (Exception e) {
      // roll back
      for (ModificationFile modFile : updatedModFiles) {
        modFile.abort();
      }
      throw new IOException(e);
    } finally {
      writeUnlock();
    }
  }


  private void deleteFiles(List<TsFileResourceV2> tsFileResourceList, Deletion deletion,
      List<ModificationFile> updatedModFiles)
      throws IOException {
    String deviceId = deletion.getDevice();
    for (TsFileResourceV2 tsFileResource : tsFileResourceList) {
      if (!tsFileResource.containsDevice(deviceId) ||
          deletion.getTimestamp() < tsFileResource.getStartTimeMap().get(deviceId)) {
        continue;
      }

      // write deletion into modification file
      tsFileResource.getModFile().write(deletion);

      // delete data in memory of unsealed file
      if (!tsFileResource.isClosed()) {
        UnsealedTsFileProcessorV2 tsfileProcessor = tsFileResource.getUnsealedFileProcessor();
        tsfileProcessor.delete(deletion);
      }

      // add a record in case of rollback
      updatedModFiles.add(tsFileResource.getModFile());
    }
  }


  public void asyncForceClose() {
    writeLock();
    LOGGER.info("async force close all file in storage group: {}", storageGroupName);
    try {
      if (workSequenceTsFileProcessor != null) {
        closingSequenceTsFileProcessor.add(workSequenceTsFileProcessor);
        workSequenceTsFileProcessor.asyncClose();
        updateEndTimeMap(workSequenceTsFileProcessor);
        workSequenceTsFileProcessor = null;
      }
      if (workUnSequenceTsFileProcessor != null) {
        closingUnSequenceTsFileProcessor.add(workUnSequenceTsFileProcessor);
        workUnSequenceTsFileProcessor.asyncClose();
        workUnSequenceTsFileProcessor = null;
      }
    } finally {
      writeUnlock();
    }
  }

  /**
   * when close an UnsealedTsFileProcessor, update its EndTimeMap immediately
   *
   * @param tsFileProcessor processor to be closed
   */
  private void updateEndTimeMap(UnsealedTsFileProcessorV2 tsFileProcessor) {
    TsFileResourceV2 resource = tsFileProcessor.getTsFileResource();
    for (Entry<String, Long> startTime : resource.getStartTimeMap().entrySet()) {
      String deviceId = startTime.getKey();
      resource.getEndTimeMap().put(deviceId, latestTimeForEachDevice.get(deviceId));
    }
  }

  /**
   * This method will be blocked until all tsfile processors are closed.
   */
  public void syncCloseFileNode() {
    synchronized (closeFileNodeCondition) {
      try {
        asyncForceClose();
        while (true) {
          if (closingSequenceTsFileProcessor.isEmpty() && closingUnSequenceTsFileProcessor
              .isEmpty()) {
            break;
          }
          closeFileNodeCondition.wait();
        }
      } catch (InterruptedException e) {
        LOGGER
            .error("CloseFileNodeCondition occurs error while waiting for closing the file node {}",
                storageGroupName, e);
      }
    }
  }


  /**
   * This method will be blocked until this file node can be closed.
   */
  public void syncCloseAndStopFileNode(Supplier<Boolean> removeProcessorFromManagerCallback) {
    synchronized (closeFileNodeCondition) {
      try {
        asyncForceClose();
        toBeClosed = true;
        while (true) {
          if (closingSequenceTsFileProcessor.isEmpty() && closingUnSequenceTsFileProcessor
              .isEmpty()) {
            removeProcessorFromManagerCallback.get();
            break;
          }
          closeFileNodeCondition.wait();
        }
      } catch (InterruptedException e) {
        LOGGER
            .error("CloseFileNodeCondition occurs error while waiting for closing the file node {}",
                storageGroupName, e);
      }
    }
  }


  public boolean updateLatestFlushTimeCallback() {
    long time = System.currentTimeMillis();
    writeLock();
    try {
      // update the largest timestamp in the last flushing memtable
      long time1 = System.currentTimeMillis();
      for (Entry<String, Long> entry : latestTimeForEachDevice.entrySet()) {
        latestFlushedTimeForEachDevice.put(entry.getKey(), entry.getValue());
      }
      time1 = System.currentTimeMillis() - time1;
      if (time1 > 1000) {
        LOGGER.info("update latest flush time call back cost {}ms", time1);
      }
    } finally {
      writeUnlock();
    }
    time = System.currentTimeMillis() - time;
    if (time > 1000) {
      LOGGER.info("update latest flush time call back all cost {}ms", time);
    }
    return true;
  }

  /**
   * put the memtable back to the MemTablePool and make the metadata in writer visible
   */
  // TODO please consider concurrency with query and insert method.
  public void closeUnsealedTsFileProcessorCallback(
      UnsealedTsFileProcessorV2 unsealedTsFileProcessor) {
    try {
      unsealedTsFileProcessor.close();
    } catch (IOException e) {
      LOGGER.error("storage group: {} close unsealedTsFileProcessor failed", storageGroupName, e);
    }
    if (closingSequenceTsFileProcessor.contains(unsealedTsFileProcessor)) {
      closingSequenceTsFileProcessor.remove(unsealedTsFileProcessor);
    } else {
      closingUnSequenceTsFileProcessor.remove(unsealedTsFileProcessor);
    }
    LOGGER.info("signal closing file node condition");
    synchronized (closeFileNodeCondition) {
      closeFileNodeCondition.notify();
    }
  }


  public UnsealedTsFileProcessorV2 getWorkSequenceTsFileProcessor() {
    return workSequenceTsFileProcessor;
  }

  public UnsealedTsFileProcessorV2 getWorkUnSequenceTsFileProcessor() {
    return workUnSequenceTsFileProcessor;
  }

  public String getStorageGroupName() {
    return storageGroupName;
  }

  public int getClosingProcessorSize() {
    return unSequenceFileList.size() + sequenceFileList.size();
  }
}
