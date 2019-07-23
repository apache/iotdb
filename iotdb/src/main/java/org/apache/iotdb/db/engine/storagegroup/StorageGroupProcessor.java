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

import static org.apache.iotdb.tsfile.common.constant.TsFileConstant.TSFILE_SUFFIX;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.commons.io.FileUtils;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.conf.directories.DirectoryManager;
import org.apache.iotdb.db.engine.modification.Deletion;
import org.apache.iotdb.db.engine.modification.ModificationFile;
import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
import org.apache.iotdb.db.engine.querycontext.ReadOnlyMemChunk;
import org.apache.iotdb.db.engine.version.SimpleFileVersionController;
import org.apache.iotdb.db.engine.version.VersionController;
import org.apache.iotdb.db.exception.DiskSpaceInsufficientException;
import org.apache.iotdb.db.exception.ProcessorException;
import org.apache.iotdb.db.exception.StorageGroupProcessorException;
import org.apache.iotdb.db.exception.TsFileProcessorException;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.qp.physical.crud.DeletePlan;
import org.apache.iotdb.db.qp.physical.crud.InsertPlan;
import org.apache.iotdb.db.query.context.QueryContext;
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

/**
 * For sequence data, a StorageGroupProcessor has some TsFileProcessors, in which there is only one
 * TsFileProcessor in the working status. <br/>
 *
 * There are two situations to set the working TsFileProcessor to closing status:<br/>
 *
 * (1) when inserting data into the TsFileProcessor, and the TsFileProcessor shouldFlush() (or
 * shouldClose())<br/>
 *
 * (2) someone calls waitForAllCurrentTsFileProcessorsClosed(). (up to now, only flush command from
 * cli will call this method)<br/>
 *
 * UnSequence data has the similar process as above.
 *
 * When a sequence TsFileProcessor is submitted to be flushed, the updateLatestFlushTimeCallback()
 * method will be called as a callback.<br/>
 *
 * When a TsFileProcessor is closed, the closeUnsealedTsFileProcessor() method will be called as a
 * callback.
 */
public class StorageGroupProcessor {

  private static final Logger logger = LoggerFactory.getLogger(StorageGroupProcessor.class);
  /**
   * a read write lock for guaranteeing concurrent safety when accessing all fields in this class
   * (i.e., fileSchema, (un)sequenceFileList, work(un)SequenceTsFileProcessor,
   * closing(Un)SequenceTsFileProcessor, latestTimeForEachDevice, and
   * latestFlushedTimeForEachDevice)
   */
  private final ReadWriteLock insertLock = new ReentrantReadWriteLock();
  /**
   *
   */
  private final Object closeStorageGroupCondition = new Object();
  /**
   * avoid some tsfileResource is changed (e.g., from unsealed to sealed) when a query is executed.
   */
  private final ReadWriteLock closeQueryLock = new ReentrantReadWriteLock();
  /**
   * the schema of time series that belong this storage group
   */
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
   * device -> global latest timestamp of each device latestTimeForEachDevice caches non-flushed
   * changes upon timestamps of each device, and is used to update latestFlushedTimeForEachDevice
   * when a flush is issued.
   */
  private Map<String, Long> latestTimeForEachDevice = new HashMap<>();
  /**
   * device -> largest timestamp of the latest memtable to be submitted to asyncTryToFlush
   * latestFlushedTimeForEachDevice determines whether a data point should be put into a sequential
   * file or an unsequential file. Data of some device with timestamp less than or equals to the
   * device's latestFlushedTime should go into an unsequential file.
   */
  private Map<String, Long> latestFlushedTimeForEachDevice = new HashMap<>();
  private String storageGroupName;
  /**
   * versionController assigns a version for each MemTable and deletion/update such that after they
   * are persisted, the order of insertions, deletions and updates can be re-determined.
   */
  private VersionController versionController;

  /**
   * mergeDeleteLock is to be used in the merge process. Concurrent deletion and merge may result in
   * losing some deletion in the merged new file, so a lock is necessary. TODO reconsidering this
   * when implementing the merge process.
   */
  @SuppressWarnings("unused") // to be used in merge
  private ReentrantLock mergeDeleteLock = new ReentrantLock();

  /**
   * This is the modification file of the result of the current merge. Because the merged file may
   * be invisible at this moment, without this, deletion/update during merge could be lost.
   */
  private ModificationFile mergingModification;

  /**
   * This linked hash set records the access order of sensors used by query.
   */
  private LinkedList<String> lruForSensorUsedInQuery = new LinkedList<>();
  private static final int MAX_CACHE_SENSORS = 5000;


  public StorageGroupProcessor(String systemInfoDir, String storageGroupName)
      throws ProcessorException {
    this.storageGroupName = storageGroupName;

    // construct the file schema
    this.fileSchema = constructFileSchema(storageGroupName);

    try {
      File storageGroupSysDir = new File(systemInfoDir, storageGroupName);
      if (storageGroupSysDir.mkdirs()) {
        logger.info("Storage Group system Directory {} doesn't exist, create it",
            storageGroupSysDir.getPath());
      } else if (!storageGroupSysDir.exists()) {
        logger.error("craete Storage Group system Directory {} failed",
            storageGroupSysDir.getPath());
      }

      versionController = new SimpleFileVersionController(storageGroupSysDir.getPath());
    } catch (IOException e) {
      throw new StorageGroupProcessorException(e);
    }

    recover();
  }

  private void recover() throws ProcessorException {
    logger.info("recover Storage Group  {}", storageGroupName);

    // collect TsFiles from sequential data directory
    List<File> tsFiles = getAllFiles(DirectoryManager.getInstance().getAllSequenceFileFolders());
    recoverSeqFiles(tsFiles);

    // collect TsFiles from unsequential data directory
    tsFiles = getAllFiles(DirectoryManager.getInstance().getAllUnSequenceFileFolders());
    recoverUnseqFiles(tsFiles);

    for (TsFileResource resource : sequenceFileList) {
      latestTimeForEachDevice.putAll(resource.getEndTimeMap());
      latestFlushedTimeForEachDevice.putAll(resource.getEndTimeMap());
    }
  }

  private List<File> getAllFiles(List<String> folders) {
    List<File> tsFiles = new ArrayList<>();
    for (String baseDir : folders) {
      File fileFolder = new File(baseDir, storageGroupName);
      if (!fileFolder.exists()) {
        continue;
      }
      Collections
          .addAll(tsFiles, fileFolder.listFiles(file -> file.getName().endsWith(TSFILE_SUFFIX)));
    }
    return tsFiles;
  }

  private void recoverSeqFiles(List<File> tsFiles) throws ProcessorException {
    tsFiles.sort(this::compareFileName);
    for (File tsFile : tsFiles) {
      TsFileResource tsFileResource = new TsFileResource(tsFile);
      sequenceFileList.add(tsFileResource);
      TsFileRecoverPerformer recoverPerformer = new TsFileRecoverPerformer(storageGroupName + "-"
          , fileSchema, versionController, tsFileResource, false);
      recoverPerformer.recover();
    }
  }

  private void recoverUnseqFiles(List<File> tsFiles) throws ProcessorException {
    tsFiles.sort(this::compareFileName);
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
  public int compareFileName(File o1, File o2) {
    String[] items1 = o1.getName().replace(TSFILE_SUFFIX, "").split("-");
    String[] items2 = o2.getName().replace(TSFILE_SUFFIX, "").split("-");
    if (Long.valueOf(items1[0]) - Long.valueOf(items2[0]) == 0) {
      return Long.compare(Long.valueOf(items1[1]), Long.valueOf(items2[1]));
    } else {
      return Long.compare(Long.valueOf(items1[0]), Long.valueOf(items2[0]));
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
   * add a measurement into the fileSchema.
   */
  public void addMeasurement(String measurementId, TSDataType dataType, TSEncoding encoding,
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
      // init map
      latestTimeForEachDevice.putIfAbsent(insertPlan.getDeviceId(), Long.MIN_VALUE);
      latestFlushedTimeForEachDevice.putIfAbsent(insertPlan.getDeviceId(), Long.MIN_VALUE);

      // insert to sequence or unSequence file
      return insertToTsFileProcessor(insertPlan,
          insertPlan.getTime() > latestFlushedTimeForEachDevice.get(insertPlan.getDeviceId()));
    } catch (IOException e) {
      logger.error("insert tsRecord to unsealed data file failed, because {}", e.getMessage(), e);
      return false;
    } finally {
      writeUnlock();
    }
  }

  private boolean insertToTsFileProcessor(InsertPlan insertPlan, boolean sequence)
      throws IOException {
    TsFileProcessor tsFileProcessor;
    boolean result;

    try {
      if (sequence) {
        if (workSequenceTsFileProcessor == null) {
          // create a new TsfileProcessor
          workSequenceTsFileProcessor = createTsFileProcessor(true);
          sequenceFileList.add(workSequenceTsFileProcessor.getTsFileResource());
        }
        tsFileProcessor = workSequenceTsFileProcessor;
      } else {
        if (workUnSequenceTsFileProcessor == null) {
          // create a new TsfileProcessor
          workUnSequenceTsFileProcessor = createTsFileProcessor(false);
          unSequenceFileList.add(workUnSequenceTsFileProcessor.getTsFileResource());
        }
        tsFileProcessor = workUnSequenceTsFileProcessor;
      }
    } catch (DiskSpaceInsufficientException e) {
      logger.error(
          "disk space is insufficient when creating TsFile processor, change system mode to read-only",
          e);
      IoTDBDescriptor.getInstance().getConfig().setReadOnly(true);
      return false;
    }

    // insert TsFileProcessor
    result = tsFileProcessor.insert(insertPlan);

    // try to update the latest time of the device of this tsRecord
    if (result && latestTimeForEachDevice.get(insertPlan.getDeviceId()) < insertPlan.getTime()) {
      latestTimeForEachDevice.put(insertPlan.getDeviceId(), insertPlan.getTime());
    }

    // check memtable size and may asyncTryToFlush the work memtable
    if (tsFileProcessor.shouldFlush()) {
      logger.info("The memtable size {} reaches the threshold, async flush it to tsfile: {}",
          tsFileProcessor.getWorkMemTableMemory(),
          tsFileProcessor.getTsFileResource().getFile().getAbsolutePath());

      if (tsFileProcessor.shouldClose()) {
        moveOneWorkProcessorToClosingList(sequence);
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
  private void moveOneWorkProcessorToClosingList(boolean sequence) {
    //for sequence tsfile, we update the endTimeMap only when the file is prepared to be closed.
    //for unsequence tsfile, we have maintained the endTimeMap when an insertion comes.
    if (sequence) {
      closingSequenceTsFileProcessor.add(workSequenceTsFileProcessor);
      updateEndTimeMap(workSequenceTsFileProcessor);
      workSequenceTsFileProcessor.asyncClose();
      workSequenceTsFileProcessor = null;
    } else {
      closingUnSequenceTsFileProcessor.add(workUnSequenceTsFileProcessor);
      workUnSequenceTsFileProcessor.asyncClose();
      workUnSequenceTsFileProcessor = null;
    }
  }

  public void syncDeleteDataFiles() {
    waitForAllCurrentTsFileProcessorsClosed();
    writeLock();
    try {
      for (TsFileResource tsFileResource : unSequenceFileList) {
        tsFileResource.close();
      }
      for (TsFileResource tsFileResource : sequenceFileList) {
        tsFileResource.close();
      }
      List<String> folder = DirectoryManager.getInstance().getAllSequenceFileFolders();
      folder.addAll(DirectoryManager.getInstance().getAllUnSequenceFileFolders());
      for (String tsfilePath : folder) {
        File storageGroupFolder = new File(tsfilePath, storageGroupName);
        if (storageGroupFolder.exists()) {
          try {
            FileUtils.deleteDirectory(storageGroupFolder);
          } catch (IOException e) {
            logger.error("Delete tsfiles failed", e);
          }
        }
      }
      this.workSequenceTsFileProcessor = null;
      this.workUnSequenceTsFileProcessor = null;
      this.sequenceFileList.clear();
      this.unSequenceFileList.clear();
      this.latestFlushedTimeForEachDevice.clear();
      this.latestTimeForEachDevice.clear();
    } catch (IOException e) {
      logger.error("Cannot delete files in storage group {}, because", storageGroupName, e);
    } finally {
      writeUnlock();
    }
  }

  /**
   * This method will be blocked until all tsfile processors are closed.
   */
  public void waitForAllCurrentTsFileProcessorsClosed() {
    synchronized (closeStorageGroupCondition) {
      try {
        putAllWorkingTsFileProcessorIntoClosingList();
        while (!closingSequenceTsFileProcessor.isEmpty() || !closingUnSequenceTsFileProcessor
            .isEmpty()) {
          closeStorageGroupCondition.wait();
        }
      } catch (InterruptedException e) {
        logger.error("CloseFileNodeCondition error occurs while waiting for closing the storage "
            + "group {}", storageGroupName, e);
        Thread.currentThread().interrupt();
      }
    }
  }

  public void putAllWorkingTsFileProcessorIntoClosingList() {
    writeLock();
    try {
      logger.info("async force close all files in storage group: {}", storageGroupName);
      if (workSequenceTsFileProcessor != null) {
        moveOneWorkProcessorToClosingList(true);
      }
      if (workUnSequenceTsFileProcessor != null) {
        moveOneWorkProcessorToClosingList(false);
      }
    } finally {
      writeUnlock();
    }
  }

  // TODO need a read lock, please consider the concurrency with flush manager threads.
  public QueryDataSource query(String deviceId, String measurementId, QueryContext context) {
    insertLock.readLock().lock();
    synchronized (lruForSensorUsedInQuery) {
      if (lruForSensorUsedInQuery.size() >= MAX_CACHE_SENSORS) {
        lruForSensorUsedInQuery.removeFirst();
      }
      lruForSensorUsedInQuery.add(measurementId);
    }
    try {
      List<TsFileResource> seqResources = getFileReSourceListForQuery(sequenceFileList,
          deviceId, measurementId, context);
      List<TsFileResource> unseqResources = getFileReSourceListForQuery(unSequenceFileList,
          deviceId, measurementId, context);
      return new QueryDataSource(new Path(deviceId, measurementId), seqResources, unseqResources);
    } finally {
      insertLock.readLock().unlock();
    }
  }

  /**
   * returns the top k% measurements which are recently used in queries.
   */
  public Set calTopKMeasurement(String sensorId, double k) {
    int num = (int) (lruForSensorUsedInQuery.size() * k);
    Set<String> sensorSet = new HashSet<>(num + 1);
    synchronized (lruForSensorUsedInQuery) {
      Iterator<String> iterator = lruForSensorUsedInQuery.descendingIterator();
      while (iterator.hasNext() && sensorSet.size() < num) {
        String sensor = iterator.next();
        if (sensorSet.contains(sensor)) {
          iterator.remove();
        } else {
          sensorSet.add(sensor);
        }
      }
    }
    sensorSet.add(sensorId);
    return sensorSet;
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
      String deviceId, String measurementId, QueryContext context) {

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
                .query(deviceId, measurementId, dataType, mSchema.getProps(), context);
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
   * Delete data whose timestamp <= 'timestamp' and belongs to the timeseries
   * deviceId.measurementId.
   *
   * @param deviceId the deviceId of the timeseries to be deleted.
   * @param measurementId the measurementId of the timeseries to be deleted.
   * @param timestamp the delete range is (0, timestamp].
   */
  public void delete(String deviceId, String measurementId, long timestamp) throws IOException {
    // TODO: how to avoid partial deletion?
    writeLock();

    // record files which are updated so that we can roll back them in case of exception
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
        //TODO check me when implementing the merge process.
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
        tsfileProcessor.deleteDataInMemory(deletion);
      }

      // add a record in case of rollback
      updatedModFiles.add(tsFileResource.getModFile());
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
      resource.forceUpdateEndTime(deviceId, latestTimeForEachDevice.get(deviceId));
    }
  }


  private boolean updateLatestFlushTimeCallback() {
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
    //closingSequenceTsFileProcessor is a thread safety class.
    if (closingSequenceTsFileProcessor.contains(tsFileProcessor)) {
      closingSequenceTsFileProcessor.remove(tsFileProcessor);
    } else {
      closingUnSequenceTsFileProcessor.remove(tsFileProcessor);
    }
    logger.info("signal closing storage group condition in {}", storageGroupName);
    synchronized (closeStorageGroupCondition) {
      closeStorageGroupCondition.notifyAll();
    }
  }


  public TsFileProcessor getWorkSequenceTsFileProcessor() {
    return workSequenceTsFileProcessor;
  }

  @FunctionalInterface
  public interface CloseTsFileCallBack {

    void call(TsFileProcessor caller) throws TsFileProcessorException, IOException;
  }

}
