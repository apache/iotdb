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
package org.apache.iotdb.db.engine.storagegroup;

import static org.apache.iotdb.tsfile.common.constant.SystemConstant.TSFILE_SUFFIX;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
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
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.engine.modification.Deletion;
import org.apache.iotdb.db.engine.modification.ModificationFile;
import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
import org.apache.iotdb.db.engine.querycontext.ReadOnlyMemChunk;
import org.apache.iotdb.db.engine.version.SimpleFileVersionController;
import org.apache.iotdb.db.engine.version.VersionController;
import org.apache.iotdb.db.exception.DiskSpaceInsufficientException;
import org.apache.iotdb.db.exception.StorageGroupProcessorException;
import org.apache.iotdb.db.exception.ProcessorException;
import org.apache.iotdb.db.exception.TsFileProcessorException;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.qp.physical.crud.DeletePlan;
import org.apache.iotdb.db.qp.physical.crud.InsertPlan;
import org.apache.iotdb.db.utils.CopyOnReadLinkedList;
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

public class StorageGroupProcessor {

  private static final Logger logger = LoggerFactory.getLogger(StorageGroupProcessor.class);

  private FileSchema fileSchema;

  // includes sealed and unsealed sequence TsFiles
  private List<TsFileResource> sequenceFileList = new ArrayList<>();
  private TsFileProcessor workSequenceTsFileProcessor = null;
  private CopyOnReadLinkedList<TsFileProcessor> closingSequenceTsFileProcessor = new CopyOnReadLinkedList<>();

  // includes sealed and unsealed unSequence TsFiles
  private List<TsFileResource> unSequenceFileList = new ArrayList<>();
  private TsFileProcessor workUnSequenceTsFileProcessor = null;
  private CopyOnReadLinkedList<TsFileProcessor> closingUnSequenceTsFileProcessor = new CopyOnReadLinkedList<>();

  /**
   * device -> global latest timestamp of each device
   * latestTimeForEachDevice caches non-flushed changes upon timestamps of each device, and is used
   * to update latestFlushedTimeForEachDevice when a flush is issued.
   */
  private Map<String, Long> latestTimeForEachDevice = new HashMap<>();

  /**
   * device -> largest timestamp of the latest memtable to be submitted to asyncFlush
   * latestFlushedTimeForEachDevice determines whether a data point should be put into a sequential
   * file or an unsequential file. Data of some device with timestamp less than or equals to the
   * device's latestFlushedTime should go into an unsequential file.
   */
  private Map<String, Long> latestFlushedTimeForEachDevice = new HashMap<>();

  private String storageGroupName;

  private final ReadWriteLock insertLock = new ReentrantReadWriteLock();

  private final Object closeFileNodeCondition = new Object();

  private final ReadWriteLock closeQueryLock = new ReentrantReadWriteLock();
  /**
   * Mark whether to close file node
   */
  private volatile boolean toBeClosed;

  /**
   * versionController assigns a version for each MemTable and deletion/update such that after
   * they are persisted, the order of insertions, deletions and updates can be re-determined.
   */
  private VersionController versionController;

  /**
   * mergeDeleteLock is to be used in merge. Concurrent deletion and merge may result in losing
   * some deletion in the merged new file, so a lock is necessary.
   */
  private ReentrantLock mergeDeleteLock = new ReentrantLock();

  /**
   * This is the modification file of the result of the current merge. Because the merged file
   * may be invisible at this moment, without this, deletion/update during merge could be lost.
   */
  private ModificationFile mergingModification;


  public StorageGroupProcessor(String baseDir, String storageGroupName) throws ProcessorException {
    this.storageGroupName = storageGroupName;

    // construct the file schema
    this.fileSchema = constructFileSchema(storageGroupName);

    try {
      File storageGroupInfoDir = new File(baseDir, storageGroupName);
      if (storageGroupInfoDir.mkdirs()) {
        logger.info("Storage Group Info Directory {} doesn't exist, create it",
            storageGroupInfoDir.getPath());
      }

      versionController = new SimpleFileVersionController(
          storageGroupInfoDir.getPath());
    } catch (IOException e) {
      throw new StorageGroupProcessorException(e);
    }

    recover();
  }

  private void recover() throws ProcessorException {
    logger.info("recover StorageGroupProcessor {}", storageGroupName);
    // collect TsFiles from sequential data directory
    List<File> tsFiles = new ArrayList<>();
    List<String> seqFileFolders = DirectoryManager.getInstance().getAllTsFileFolders();
    for (String baseDir : seqFileFolders) {
      File fileFolder = new File(baseDir, storageGroupName);
      if (!fileFolder.exists()) {
        continue;
      }
      Collections
          .addAll(tsFiles, fileFolder.listFiles(file -> file.getName().endsWith(TSFILE_SUFFIX)));
    }
    recoverSeqFiles(tsFiles);

    // collect TsFiles from unsequential data directory
    tsFiles.clear();
    List<String> unseqFileFolder = DirectoryManager.getInstance().getAllOverflowFileFolders();
    for (String baseDir : unseqFileFolder) {
      File fileFolder = new File(baseDir, storageGroupName);
      if (!fileFolder.exists()) {
        continue;
      }
      Collections
          .addAll(tsFiles, fileFolder.listFiles(file -> file.getName().endsWith(TSFILE_SUFFIX)));
    }
    recoverUnseqFiles(tsFiles);

    for (TsFileResource resource : sequenceFileList) {
      latestTimeForEachDevice.putAll(resource.getEndTimeMap());
      latestFlushedTimeForEachDevice.putAll(resource.getEndTimeMap());
    }
  }

  private void recoverSeqFiles(List<File> tsFiles) throws ProcessorException {
    tsFiles.sort(new TsFileNameComparator());
    for (File tsFile : tsFiles) {
      TsFileResource tsFileResource = new TsFileResource(tsFile);
      sequenceFileList.add(tsFileResource);
      TsFileRecoverPerformer recoverPerformer = new TsFileRecoverPerformer(storageGroupName + "-"
          , fileSchema, versionController, tsFileResource, false);
      recoverPerformer.recover();
    }
  }

  private void recoverUnseqFiles(List<File> tsFiles) throws ProcessorException {
    tsFiles.sort(new TsFileNameComparator());
    for (File tsFile : tsFiles) {
      TsFileResource tsFileResource = new TsFileResource(tsFile);
      unSequenceFileList.add(tsFileResource);
      TsFileRecoverPerformer recoverPerformer = new TsFileRecoverPerformer(storageGroupName + "-",
          fileSchema,
          versionController, tsFileResource, true);
      recoverPerformer.recover();
    }
  }

  // TsFileNameComparator compares TsFiles by the version number in its name
  // ({systemTime}-{versionNum}.tsfile)
  class TsFileNameComparator implements Comparator<File> {

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
    columnSchemaList = MManager.getInstance().getSchemaForStorageGroup(storageGroupName);

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

  public boolean insert(InsertPlan insertPlan) {
    writeLock();
    try {
      if (toBeClosed) {
        throw new StorageGroupProcessorException(
            "storage group " + storageGroupName + " is to be closed, this insertion is rejected");
      }
      // init map
      latestTimeForEachDevice.putIfAbsent(insertPlan.getDeviceId(), Long.MIN_VALUE);
      latestFlushedTimeForEachDevice.putIfAbsent(insertPlan.getDeviceId(), Long.MIN_VALUE);

      // insert to sequence or unSequence file
      return insertTsFileProcessor(insertPlan,
          insertPlan.getTime() > latestFlushedTimeForEachDevice.get(insertPlan.getDeviceId()));
    } catch (StorageGroupProcessorException | IOException e) {
      logger.error("insert tsRecord to unsealed data file failed, because {}", e.getMessage(), e);
      return false;
    } finally {
      writeUnlock();
    }
  }

  private boolean insertTsFileProcessor(InsertPlan insertPlan, boolean sequence)
      throws IOException {
    TsFileProcessor tsFileProcessor = null;
    boolean result;

    // create a new TsfileProcessor
    try {
      if (sequence) {
        if (workSequenceTsFileProcessor == null) {
          workSequenceTsFileProcessor = createTsFileProcessor(true);
          sequenceFileList.add(workSequenceTsFileProcessor.getTsFileResource());
        }
        tsFileProcessor = workSequenceTsFileProcessor;
      } else {
        if (workUnSequenceTsFileProcessor == null) {
          workUnSequenceTsFileProcessor = createTsFileProcessor(false);
          unSequenceFileList.add(workUnSequenceTsFileProcessor.getTsFileResource());
        }
        tsFileProcessor = workUnSequenceTsFileProcessor;
      }
    } catch (DiskSpaceInsufficientException e) {
      logger.error("disk space is insufficient", e);
      StorageEngine.getInstance().setReadOnly(true);
    }

    // insert TsFileProcessor
    result = tsFileProcessor.insert(insertPlan);

    // try to update the latest time of the device of this tsRecord
    if (result && latestTimeForEachDevice.get(insertPlan.getDeviceId()) < insertPlan.getTime()) {
      latestTimeForEachDevice.put(insertPlan.getDeviceId(), insertPlan.getTime());
    }

    // check memtable size and may asyncFlush the work memtable
    if (tsFileProcessor.shouldFlush()) {

      logger.info("The memtable size {} reaches the threshold, async flush it to tsfile: {}",
          tsFileProcessor.getWorkMemTableMemory(),
          tsFileProcessor.getTsFileResource().getFile().getAbsolutePath());

      if (tsFileProcessor.shouldClose()) {
        asyncCloseTsFileProcessor(tsFileProcessor, sequence);
      } else {
        tsFileProcessor.asyncFlush();
      }
    }
    return result;
  }

  private TsFileProcessor createTsFileProcessor(boolean sequence)
      throws IOException, DiskSpaceInsufficientException {
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
      return new TsFileProcessor(storageGroupName, new File(filePath),
          fileSchema, versionController, this::closeUnsealedTsFileProcessor,
          this::updateLatestFlushTimeCallback, sequence);
    } else {
      return new TsFileProcessor(storageGroupName, new File(filePath),
          fileSchema, versionController, this::closeUnsealedTsFileProcessor,
          () -> true, sequence);
    }
  }

  /**
   * only called by insert(), thread-safety should be ensured by caller
   */
  private void asyncCloseTsFileProcessor(TsFileProcessor tsFileProcessor,
      boolean sequence) {

    // check file size and may close the TsFile
    if (sequence) {
      closingSequenceTsFileProcessor.add(tsFileProcessor);
      workSequenceTsFileProcessor = null;
      updateEndTimeMap(tsFileProcessor);
    } else {
      closingUnSequenceTsFileProcessor.add(tsFileProcessor);
      workUnSequenceTsFileProcessor = null;
    }

    // async close tsfile
    tsFileProcessor.asyncClose();

    logger.info("The file size {} reaches the threshold, async close tsfile: {}.",
        tsFileProcessor.getTsFileResource().getFileSize(),
        tsFileProcessor.getTsFileResource().getFile().getAbsolutePath());
  }


  // TODO need a read lock, please consider the concurrency with flush manager threads.
  public QueryDataSource query(String deviceId, String measurementId) {
    insertLock.readLock().lock();
    try {
      List<TsFileResource> seqResources = getFileReSourceListForQuery(sequenceFileList,
          deviceId, measurementId);
      List<TsFileResource> unseqResources = getFileReSourceListForQuery(unSequenceFileList,
          deviceId, measurementId);
      return new QueryDataSource(new Path(deviceId, measurementId), seqResources, unseqResources);
    } finally {
      insertLock.readLock().unlock();
    }
  }

  private void writeLock() {
    insertLock.writeLock().lock();
  }

  private void writeUnlock() {
    insertLock.writeLock().unlock();
  }


  /**
   * @param tsFileResources includes sealed and unsealed tsfile resources
   * @return fill unsealed tsfile resources with memory data and ChunkMetadataList of data in disk
   */
  private List<TsFileResource> getFileReSourceListForQuery(List<TsFileResource> tsFileResources,
      String deviceId, String measurementId) {

    MeasurementSchema mSchema = fileSchema.getMeasurementSchema(measurementId);
    TSDataType dataType = mSchema.getType();

    List<TsFileResource> tsfileResourcesForQuery = new ArrayList<>();
    for (TsFileResource tsFileResource : tsFileResources) {
      // TODO: try filtering files if the query contains time filter
      if (!tsFileResource.containsDevice(deviceId)) {
        continue;
      }
      if (!tsFileResource.getStartTimeMap().isEmpty()) {
        closeQueryLock.readLock().lock();
        try {
          if (tsFileResource.isClosed()) {
            tsfileResourcesForQuery.add(tsFileResource);
          } else {
            // left: in-memory data, right: meta of disk data
            Pair<ReadOnlyMemChunk, List<ChunkMetaData>> pair;
            pair = tsFileResource
                  .getUnsealedFileProcessor()
                  .query(deviceId, measurementId, dataType, mSchema.getProps());
            tsfileResourcesForQuery
                .add(new TsFileResource(tsFileResource.getFile(),
                    tsFileResource.getStartTimeMap(),
                    tsFileResource.getEndTimeMap(), pair.left, pair.right));
          }
        } finally {
          closeQueryLock.readLock().unlock();
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
      if (lastUpdateTime == null) {
        logger.debug("No device {} in SG {}, deletion invalid", deviceId, storageGroupName);
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

      deleteDataInFiles(sequenceFileList, deletion, updatedModFiles);
      deleteDataInFiles(unSequenceFileList, deletion, updatedModFiles);

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


  private void deleteDataInFiles(List<TsFileResource> tsFileResourceList, Deletion deletion,
      List<ModificationFile> updatedModFiles)
      throws IOException {
    String deviceId = deletion.getDevice();
    for (TsFileResource tsFileResource : tsFileResourceList) {
      if (!tsFileResource.containsDevice(deviceId) ||
          deletion.getTimestamp() < tsFileResource.getStartTimeMap().get(deviceId)) {
        continue;
      }

      // write deletion into modification file
      tsFileResource.getModFile().write(deletion);

      // delete data in memory of unsealed file
      if (!tsFileResource.isClosed()) {
        TsFileProcessor tsfileProcessor = tsFileResource.getUnsealedFileProcessor();
        tsfileProcessor.delete(deletion);
      }

      // add a record in case of rollback
      updatedModFiles.add(tsFileResource.getModFile());
    }
  }


  public void asyncForceClose() {
    writeLock();
    logger.info("async force close all file in storage group: {}", storageGroupName);
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
   * when close an TsFileProcessor, update its EndTimeMap immediately
   *
   * @param tsFileProcessor processor to be closed
   */
  private void updateEndTimeMap(TsFileProcessor tsFileProcessor) {
    TsFileResource resource = tsFileProcessor.getTsFileResource();
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
        while (!closingSequenceTsFileProcessor.isEmpty() || !closingUnSequenceTsFileProcessor
            .isEmpty()) {
          closeFileNodeCondition.wait();
        }
      } catch (InterruptedException e) {
        logger
            .error("CloseFileNodeCondition occurs an error while waiting for closing the storage "
                    + "group {}",
                storageGroupName, e);
        Thread.currentThread().interrupt();
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
        logger
            .error("CloseFileNodeCondition occurs error while waiting for closing the file node {}",
                storageGroupName, e);
        Thread.currentThread().interrupt();
      }
    }
  }


  public boolean updateLatestFlushTimeCallback() {
    // update the largest timestamp in the last flushing memtable
    for (Entry<String, Long> entry : latestTimeForEachDevice.entrySet()) {
      latestFlushedTimeForEachDevice.put(entry.getKey(), entry.getValue());
    }
    return true;
  }

  /**
   * put the memtable back to the MemTablePool and make the metadata in writer visible
   */
  // TODO please consider concurrency with query and insert method.
  public void closeUnsealedTsFileProcessor(
      TsFileProcessor tsFileProcessor) throws TsFileProcessorException {
    closeQueryLock.writeLock().lock();
    try {
      tsFileProcessor.close();
    } finally {
      closeQueryLock.writeLock().unlock();
    }
    if (closingSequenceTsFileProcessor.contains(tsFileProcessor)) {
      closingSequenceTsFileProcessor.remove(tsFileProcessor);
    } else {
      closingUnSequenceTsFileProcessor.remove(tsFileProcessor);
    }
    logger.info("signal closing storage group condition in {}", storageGroupName);
    synchronized (closeFileNodeCondition) {
      closeFileNodeCondition.notify();
    }
  }


  public TsFileProcessor getWorkSequenceTsFileProcessor() {
    return workSequenceTsFileProcessor;
  }

  @FunctionalInterface
  public interface CloseTsFileCallBack {
    void call(TsFileProcessor caller) throws TsFileProcessorException;
  }
}
