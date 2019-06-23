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
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;
import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.conf.directories.DirectoryManager;
import org.apache.iotdb.db.engine.filenode.CopyOnReadLinkedList;
import org.apache.iotdb.db.engine.modification.Deletion;
import org.apache.iotdb.db.engine.modification.ModificationFile;
import org.apache.iotdb.db.engine.querycontext.QueryDataSourceV2;
import org.apache.iotdb.db.engine.querycontext.ReadOnlyMemChunk;
import org.apache.iotdb.db.engine.version.SimpleFileVersionController;
import org.apache.iotdb.db.engine.version.VersionController;
import org.apache.iotdb.db.exception.FileNodeProcessorException;
import org.apache.iotdb.db.exception.ProcessorException;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.qp.physical.crud.DeletePlan;
import org.apache.iotdb.db.qp.physical.crud.InsertPlan;
import org.apache.iotdb.db.writelog.recover.SeqTsFileRecoverPerformer;
import org.apache.iotdb.db.writelog.recover.UnSeqTsFileRecoverPerformer;
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

  private static final MManager mManager = MManager.getInstance();
  private static final DirectoryManager directoryManager = DirectoryManager.getInstance();

  private FileSchema fileSchema;

  // includes sealed and unsealed sequnce tsfiles
  private List<TsFileResourceV2> sequenceFileList = new ArrayList<>();
  private UnsealedTsFileProcessorV2 workSequenceTsFileProcessor = null;
  private CopyOnReadLinkedList<UnsealedTsFileProcessorV2> closingSequenceTsFileProcessor = new CopyOnReadLinkedList<>();

  // includes sealed and unsealed unsequnce tsfiles
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

  private final ReadWriteLock lock;

  private Condition closeFileNodeCondition;

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

  public FileNodeProcessorV2(String baseDir, String storageGroupName) throws ProcessorException {
    this.storageGroupName = storageGroupName;
    lock = new ReentrantReadWriteLock();
    closeFileNodeCondition = lock.writeLock().newCondition();

    recover();

    /**
     * version controller
     */
    try {
      File storageGroupInfoDir = new File(baseDir, storageGroupName);
      if (storageGroupInfoDir.mkdirs()) {
        LOGGER.info("Storage Group Info Directory {} doesn't exist, create it", storageGroupInfoDir.getPath());
      }

      versionController = new SimpleFileVersionController(
          storageGroupInfoDir.getPath());
    } catch (IOException e) {
      throw new FileNodeProcessorException(e);
    }

    // construct the file schema
    this.fileSchema = constructFileSchema(storageGroupName);
  }

  private void recover() throws ProcessorException {
    List<File> tsFiles = new ArrayList<>();
    List<String> seqFileFolders = directoryManager.getAllTsFileFolders();
    for (String baseDir: seqFileFolders) {
      File fileFolder = new File(baseDir, storageGroupName);
      if (!fileFolder.exists()) {
        continue;
      }
      for (File tsfile: fileFolder.listFiles()) {
        tsFiles.add(tsfile);
      }
    }
    recoverSeqFiles(tsFiles);

    tsFiles.clear();
    List<String> unseqFileFolder = directoryManager.getAllOverflowFileFolders();
    for (String baseDir: unseqFileFolder) {
      File fileFolder = new File(baseDir, storageGroupName);
      if (!fileFolder.exists()) {
        continue;
      }
      for (File tsfile: fileFolder.listFiles()) {
        tsFiles.add(tsfile);
      }
    }
    recoverUnseqFiles(tsFiles);
  }

  private static int getTsFileSerialNum(File tsFile) {
    return Integer.parseInt(tsFile.getName().split("-")[1]);
  }

  private void recoverSeqFiles(List<File> tsfiles) throws ProcessorException {
    tsfiles.sort(Comparator.comparingInt(FileNodeProcessorV2::getTsFileSerialNum));
    for (File tsfile: tsfiles) {
      TsFileResourceV2 tsFileResource = new TsFileResourceV2(tsfile);
      sequenceFileList.add(tsFileResource);
      SeqTsFileRecoverPerformer recoverPerformer = new SeqTsFileRecoverPerformer(storageGroupName + "-", fileSchema, versionController, tsFileResource);
      recoverPerformer.recover();
    }
  }

  private void recoverUnseqFiles(List<File> tsfiles) throws ProcessorException {
    tsfiles.sort(Comparator.comparingInt(FileNodeProcessorV2::getTsFileSerialNum));
    for (File tsfile: tsfiles) {
      TsFileResourceV2 tsFileResource = new TsFileResourceV2(tsfile);
      unSequenceFileList.add(tsFileResource);
      UnSeqTsFileRecoverPerformer recoverPerformer = new UnSeqTsFileRecoverPerformer(storageGroupName + "-", fileSchema,
          versionController, tsFileResource);
      recoverPerformer.recover();
    }
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
   * @return -1: failed, 1: Overflow, 2:Bufferwrite
   */
  public boolean insert(InsertPlan insertPlan) {
    lock.writeLock().lock();

    try {
      if(toBeClosed){
        throw new FileNodeProcessorException("storage group " + storageGroupName + " is to be closed, this insertion is rejected");
      }
      // init map
      latestTimeForEachDevice.putIfAbsent(insertPlan.getDeviceId(), Long.MIN_VALUE);
      latestFlushedTimeForEachDevice.putIfAbsent(insertPlan.getDeviceId(), Long.MIN_VALUE);

      boolean result;
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
      lock.writeLock().unlock();
    }
  }

  private boolean insertUnsealedDataFile(InsertPlan insertPlan, boolean sequence) throws IOException {
    lock.writeLock().lock();
    UnsealedTsFileProcessorV2 unsealedTsFileProcessor;
    try {
      boolean result;
      // create a new BufferWriteProcessor
      if (sequence) {
        if (workSequenceTsFileProcessor == null) {

          // TODO directoryManager add method getAndCreateNextFolderTsfile
          String baseDir = directoryManager.getNextFolderForTsfile();
          String filePath = Paths.get(baseDir, storageGroupName, System.currentTimeMillis() + "-" + versionController.nextVersion()).toString();

          new File(baseDir, storageGroupName).mkdirs();
          workSequenceTsFileProcessor = new UnsealedTsFileProcessorV2(storageGroupName, new File(filePath),
              fileSchema, versionController, this::closeUnsealedTsFileProcessorCallback, this::updateLatestFlushTimeCallback);

          sequenceFileList.add(workSequenceTsFileProcessor.getTsFileResource());
        }
        unsealedTsFileProcessor = workSequenceTsFileProcessor;
      } else {
        if (workUnSequenceTsFileProcessor == null) {
          // TODO check if the disk is full, move this
          String baseDir = directoryManager.getNextFolderForOverflowFile();
          new File(baseDir, storageGroupName).mkdirs();
          String filePath = Paths.get(baseDir, storageGroupName, System.currentTimeMillis() + "-" + +versionController.nextVersion()).toString();

          workUnSequenceTsFileProcessor = new UnsealedTsFileProcessorV2(storageGroupName, new File(filePath),
              fileSchema, versionController, this::closeUnsealedTsFileProcessorCallback, this::updateLatestFlushTimeCallback);

          unSequenceFileList.add(workUnSequenceTsFileProcessor.getTsFileResource());
        }
        unsealedTsFileProcessor = workUnSequenceTsFileProcessor;
      }

      // insert BufferWrite
      result = unsealedTsFileProcessor.insert(insertPlan);

      // try to update the latest time of the device of this tsRecord
      if (result && latestTimeForEachDevice.get(insertPlan.getDeviceId()) < insertPlan.getTime()) {
        latestTimeForEachDevice.put(insertPlan.getDeviceId(), insertPlan.getTime());
      }

      // check memtable size and may asyncFlush the workMemtable
      if (unsealedTsFileProcessor.shouldFlush()) {
        flushAndCheckShouldClose(unsealedTsFileProcessor, sequence);
      }

      return result;
    }finally {
      lock.writeLock().unlock();
    }
  }


  // TODO need a read lock, please consider the concurrency with flush manager threads.
  public QueryDataSourceV2 query(String deviceId, String measurementId) {
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

  /**
   * @param tsFileResources includes sealed and unsealed tsfile resources
   * @return fill unsealed tsfile resources with memory data and ChunkMetadataList of data in disk
   */
  private List<TsFileResourceV2> getFileReSourceListForQuery(List<TsFileResourceV2> tsFileResources,
      String deviceId, String measurementId) {

    MeasurementSchema mSchema = fileSchema.getMeasurementSchema(measurementId);
    TSDataType dataType = mSchema.getType();

    List<TsFileResourceV2> tsfileResourcesForQuery = new ArrayList<>();
    for (TsFileResourceV2 tsFileResource : tsFileResources) {
      synchronized (tsFileResource) {
        if (!tsFileResource.getStartTimeMap().isEmpty()) {
          if (tsFileResource.isClosed()) {
            tsfileResourcesForQuery.add(tsFileResource);
          } else {
            Pair<ReadOnlyMemChunk, List<ChunkMetaData>> pair = tsFileResource
                .getUnsealedFileProcessor()
                .query(deviceId, measurementId, dataType, mSchema.getProps());
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
    mergeDeleteLock.lock();
    long lastUpdateTime = latestTimeForEachDevice.get(deviceId);
    // no tsfile data, the delete operation is invalid
    if (lastUpdateTime == Long.MIN_VALUE) {
      LOGGER.warn("The last update time is -1, delete overflow is invalid, "
          + "the storage group is {}", storageGroupName);
      return;
    }

    // write log
    if (IoTDBDescriptor.getInstance().getConfig().isEnableWal()) {
      workSequenceTsFileProcessor.getLogNode().write(new DeletePlan(timestamp, new Path(deviceId, measurementId)));
      workUnSequenceTsFileProcessor.getLogNode().write(new DeletePlan(timestamp, new Path(deviceId, measurementId)));
    }

    long version = versionController.nextVersion();

    // record what files are updated so we can roll back them in case of exception
    List<ModificationFile> updatedModFiles = new ArrayList<>();

    try {
      String fullPath = deviceId + IoTDBConstant.PATH_SEPARATOR + measurementId;
      Deletion deletion = new Deletion(fullPath, version, timestamp);
      if (mergingModification != null) {
        mergingModification.write(deletion);
        updatedModFiles.add(mergingModification);
      }

      // delete sequence files
      for (TsFileResourceV2 tsFileResource : sequenceFileList) {
        if (!tsFileResource.containsDevice(deviceId) ||
            tsFileResource.getStartTimeMap().get(deviceId) <= deletion.getTimestamp()) {
          continue;
        }
        if (tsFileResource.isClosed()) {
          tsFileResource.getModFile().write(deletion);
          updatedModFiles.add(tsFileResource.getModFile());
        } else {
          // TODO delete unsealed file
        }
      }

      // delete unSequence files
      for (TsFileResourceV2 tsFileResource : unSequenceFileList) {
        if (!tsFileResource.containsDevice(deviceId) ||
            tsFileResource.getStartTimeMap().get(deviceId) <= deletion.getTimestamp()) {
          continue;
        }
        if (tsFileResource.isClosed()) {
          tsFileResource.getModFile().write(deletion);
          updatedModFiles.add(tsFileResource.getModFile());
        } else {
          // TODO delete unsealed file
        }
      }


    } catch (Exception e) {
      // roll back
      for (ModificationFile modFile : updatedModFiles) {
        modFile.abort();
      }
      throw new IOException(e);
    } finally {
      mergeDeleteLock.unlock();
    }
  }


  /**
   * ensure there must be a flush thread submitted after setCloseMark() is called, therefore the setCloseMark task
   * will be executed by a flush thread.
   *
   * only called by insert(), thread-safety should be ensured by caller
   */
  private void flushAndCheckShouldClose(UnsealedTsFileProcessorV2 unsealedTsFileProcessor,
      boolean sequence) {
    // check file size and may setCloseMark the BufferWrite
    if (unsealedTsFileProcessor.shouldClose()) {
      if (sequence) {
        closingSequenceTsFileProcessor.add(unsealedTsFileProcessor);
        workSequenceTsFileProcessor = null;
      } else {
        closingUnSequenceTsFileProcessor.add(unsealedTsFileProcessor);
        workUnSequenceTsFileProcessor = null;
      }
      unsealedTsFileProcessor.setCloseMark();
    }

    unsealedTsFileProcessor.asyncFlush();
  }

  public void asyncForceClose() {
    lock.writeLock().lock();
    try {
      if (workSequenceTsFileProcessor != null) {
        closingSequenceTsFileProcessor.add(workSequenceTsFileProcessor);
        workSequenceTsFileProcessor.asyncClose();
        workSequenceTsFileProcessor = null;
      }
      if (workUnSequenceTsFileProcessor != null) {
        closingUnSequenceTsFileProcessor.add(workUnSequenceTsFileProcessor);
        workUnSequenceTsFileProcessor.asyncClose();
        workUnSequenceTsFileProcessor = null;
      }
    } finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * This method will be blocked until all tsfile processors are closed.
   */
  public void syncCloseFileNode(){
    lock.writeLock().lock();
    try {
      asyncForceClose();
      while (true) {
        if (closingSequenceTsFileProcessor.isEmpty() && closingUnSequenceTsFileProcessor.isEmpty()
            && workSequenceTsFileProcessor == null && workUnSequenceTsFileProcessor == null) {
          break;
        }
        closeFileNodeCondition.await();
      }
    } catch (InterruptedException e) {
      LOGGER.error("CloseFileNodeConditon occurs error while waiting for closing the file node {}",
          storageGroupName, e);
    } finally {
      lock.writeLock().unlock();
    }
  }


  /**
   * This method will be blocked until this file node can be closed.
   */
  public void syncCloseAndStopFileNode(Supplier<Boolean> removeProcessorFromManagerCallback){
    lock.writeLock().lock();
    try {
      asyncForceClose();
      toBeClosed = true;
      while (true) {
        if (closingSequenceTsFileProcessor.isEmpty() && closingUnSequenceTsFileProcessor.isEmpty()
            && workSequenceTsFileProcessor == null && workUnSequenceTsFileProcessor == null) {
          removeProcessorFromManagerCallback.get();
          break;
        }
        closeFileNodeCondition.await();
      }
    } catch (InterruptedException e) {
      LOGGER.error("CloseFileNodeConditon occurs error while waiting for closing the file node {}",
          storageGroupName, e);
    } finally {
      lock.writeLock().unlock();
    }
  }


  public boolean updateLatestFlushTimeCallback() {
    lock.writeLock().lock();
    try {
      // update the largest timestamp in the last flushing memtable
      for (Entry<String, Long> entry : latestTimeForEachDevice.entrySet()) {
        latestFlushedTimeForEachDevice.put(entry.getKey(), entry.getValue());
      }
    } finally {
      lock.writeLock().unlock();
    }
    return true;
  }

  /**
   * put the memtable back to the MemTablePool and make the metadata in writer visible
   */
  // TODO please consider concurrency with query and insert method.
  public void closeUnsealedTsFileProcessorCallback(UnsealedTsFileProcessorV2 unsealedTsFileProcessor) {
    lock.writeLock().lock();
    try {
      if (closingSequenceTsFileProcessor.contains(unsealedTsFileProcessor)) {
        closingSequenceTsFileProcessor.remove(unsealedTsFileProcessor);
      } else {
        closingUnSequenceTsFileProcessor.remove(unsealedTsFileProcessor);
      }
      // end time with one start time
      TsFileResourceV2 resource = unsealedTsFileProcessor.getTsFileResource();
      synchronized (resource) {
        for (Entry<String, Long> startTime : resource.getStartTimeMap().entrySet()) {
          String deviceId = startTime.getKey();
          resource.getEndTimeMap().put(deviceId, latestTimeForEachDevice.get(deviceId));
        }
        resource.setClosed(true);
      }
      closeFileNodeCondition.signal();
    }finally {
      lock.writeLock().unlock();
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

  public int getClosingProcessorSize(){
    return unSequenceFileList.size() + sequenceFileList.size();
  }
}
