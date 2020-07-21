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

package org.apache.iotdb.db.writelog.recover;

import static org.apache.iotdb.db.engine.flush.MemTableFlushTask.getFlushLogFile;
import static org.apache.iotdb.db.engine.flush.VmLogger.isVMLoggerFileExist;
import static org.apache.iotdb.db.engine.storagegroup.TsFileProcessor.createNewVMFile;
import static org.apache.iotdb.db.engine.storagegroup.TsFileResource.RESOURCE_SUFFIX;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;
import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.engine.fileSystem.SystemFileFactory;
import org.apache.iotdb.db.engine.flush.MemTableFlushTask;
import org.apache.iotdb.db.engine.memtable.IMemTable;
import org.apache.iotdb.db.engine.memtable.PrimitiveMemTable;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.engine.version.VersionController;
import org.apache.iotdb.db.exception.StorageGroupProcessorException;
import org.apache.iotdb.db.utils.FileLoaderUtils;
import org.apache.iotdb.db.writelog.manager.MultiFileLogNodeManager;
import org.apache.iotdb.tsfile.exception.NotCompatibleTsFileException;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.TimeseriesMetadata;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.write.writer.RestorableTsFileIOWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TsFileRecoverPerformer recovers a SeqTsFile to correct status, redoes the WALs since last crash
 * and removes the redone logs.
 */
public class TsFileRecoverPerformer {

  private static final Logger logger = LoggerFactory.getLogger(TsFileRecoverPerformer.class);

  private String filePath;
  private String logNodePrefix;
  private VersionController versionController;
  private TsFileResource resource;
  private boolean sequence;
  private boolean isLastFile;

  private List<List<TsFileResource>> vmTsFileResources;

  /**
   * @param isLastFile whether this TsFile is the last file of its partition
   * @param vmTsFileResources only last file could have non-empty vmTsFileResources
   */
  public TsFileRecoverPerformer(String logNodePrefix, VersionController versionController,
      TsFileResource currentTsFileResource, boolean sequence, boolean isLastFile,
      List<List<TsFileResource>> vmTsFileResources) {
    this.filePath = currentTsFileResource.getTsFilePath();
    this.logNodePrefix = logNodePrefix;
    this.versionController = versionController;
    this.resource = currentTsFileResource;
    this.sequence = sequence;
    this.isLastFile = isLastFile;
    this.vmTsFileResources = vmTsFileResources;
  }

  /**
   * 1. recover the TsFile by RestorableTsFileIOWriter and truncate the file to remaining corrected
   * data 2. redo the WALs to recover unpersisted data 3. flush and close the file 4. clean WALs
   *
   * @return a RestorableTsFileIOWriter and a list of RestorableTsFileIOWriter of vmfiles, if the
   * file and the vmfiles are not closed before crush, so these writers can be used to continue
   * writing
   */
  public Pair<RestorableTsFileIOWriter, List<List<RestorableTsFileIOWriter>>> recover()
      throws StorageGroupProcessorException {

    File file = FSFactoryProducer.getFSFactory().getFile(filePath);
    List<List<File>> vmFileList = new ArrayList<>();
    for (List<TsFileResource> subTsFileResource : vmTsFileResources) {
      List<File> subVmFileList = new ArrayList<>();
      for (TsFileResource tsFileResource : subTsFileResource) {
        subVmFileList.add(FSFactoryProducer.getFSFactory().getFile(tsFileResource.getTsFilePath()));
      }
      vmFileList.add(subVmFileList);
    }
    if (!file.exists()) {
      logger.error("TsFile {} is missing, will skip its recovery.", filePath);
      return null;
    }
    for (List<File> subVmFileList : vmFileList) {
      for (File vmFile : subVmFileList) {
        if (!vmFile.exists()) {
          logger.error("VMFile {} is missing, will skip its recovery.", vmFile.getPath());
          return null;
        }
      }
    }
    // remove corrupted part of the TsFile
    RestorableTsFileIOWriter restorableTsFileIOWriter;
    List<List<RestorableTsFileIOWriter>> vmRestorableTsFileIOWriterList = new ArrayList<>();
    try {
      restorableTsFileIOWriter = new RestorableTsFileIOWriter(file);
      for (int i = 0; i < vmTsFileResources.size(); i++) {
        vmRestorableTsFileIOWriterList.add(new ArrayList<>());
        for (int j = 0; j < vmTsFileResources.get(i).size(); j++) {
          file = vmFileList.get(i).get(j);
          vmRestorableTsFileIOWriterList.get(i).add(new RestorableTsFileIOWriter(file));
        }
      }
    } catch (NotCompatibleTsFileException e) {
      boolean result = file.delete();
      logger.warn("TsFile {} is incompatible. Delete it successfully {}", filePath, result);
      throw new StorageGroupProcessorException(e);
    } catch (IOException e) {
      throw new StorageGroupProcessorException(e);
    }

    RestorableTsFileIOWriter lastRestorableTsFileIOWriter =
        vmTsFileResources.isEmpty() ? restorableTsFileIOWriter
            : vmRestorableTsFileIOWriterList.get(0)
                .get(vmRestorableTsFileIOWriterList.get(0).size() - 1);

    TsFileResource lastTsFileResource = vmTsFileResources.isEmpty() ? resource
        : vmTsFileResources.get(0).get(vmTsFileResources.get(0).size() - 1);

    boolean isComplete =
        !lastRestorableTsFileIOWriter.hasCrashed() && !lastRestorableTsFileIOWriter.canWrite();

    if (isComplete) {
      // tsfile is complete, vmfile is never complete because it's canWrite() always return true.
      try {
        recoverResource(resource);
        return new Pair<>(restorableTsFileIOWriter, vmRestorableTsFileIOWriterList);
      } catch (IOException e) {
        throw new StorageGroupProcessorException(
            "recover the resource file failed: " + filePath
                + RESOURCE_SUFFIX + e);
      }
    } else {
      if (!vmTsFileResources.isEmpty()) {
        for (int i = 0; i < vmTsFileResources.size(); i++) {
          for (int j = 0; j < vmTsFileResources.get(i).size(); j++) {
            recoverResourceFromWriter(vmRestorableTsFileIOWriterList.get(i).get(j),
                vmTsFileResources.get(i).get(j));
          }
        }
        recoverResourceFromWriter(restorableTsFileIOWriter, resource);
        boolean vmFileNotCrashed = !getFlushLogFile(restorableTsFileIOWriter).exists();
        // if the last file in vmTsFileResources is not crashed
        if (vmFileNotCrashed) {
          try {
            boolean tsFileNotCrashed = !isVMLoggerFileExist(restorableTsFileIOWriter);
            // tsfile is not crash
            if (tsFileNotCrashed) {

              // if wal exists, we should open a new vmfile to replay it
              File newVmFile = createNewVMFile(resource, 0);
              TsFileResource newVmTsFileResource = new TsFileResource(newVmFile);
              RestorableTsFileIOWriter newVMWriter = new RestorableTsFileIOWriter(newVmFile);
              if (redoLogs(newVMWriter, newVmTsFileResource)) {
                vmTsFileResources.get(0).add(newVmTsFileResource);
                vmRestorableTsFileIOWriterList.get(0).add(newVMWriter);
              } else {
                Files.delete(newVmFile.toPath());
              }
            } else {
              IMemTable recoverMemTable = new PrimitiveMemTable();
              recoverMemTable.setVersion(versionController.nextVersion());
              LogReplayer logReplayer = new LogReplayer(logNodePrefix, filePath,
                  resource.getModFile(),
                  versionController, resource, recoverMemTable, sequence);
              logReplayer.replayLogs();
            }
            // clean logs
            MultiFileLogNodeManager.getInstance().deleteNode(
                logNodePrefix + SystemFileFactory.INSTANCE.getFile(filePath).getName());
            updateTsFileResource();
            return new Pair<>(restorableTsFileIOWriter, vmRestorableTsFileIOWriterList);
          } catch (IOException e) {
            throw new StorageGroupProcessorException(
                "recover the resource file failed: " + filePath
                    + RESOURCE_SUFFIX + e);
          }
        }
      } else {
        // tsfile has crashed
        // due to failure, the last ChunkGroup may contain the same data as the WALs, so the time
        // map must be updated first to avoid duplicated insertion
        recoverResourceFromWriter(lastRestorableTsFileIOWriter, lastTsFileResource);
      }
    }

    // redo logs
    redoLogs(lastRestorableTsFileIOWriter, lastTsFileResource);

    // clean logs
    try {
      MultiFileLogNodeManager.getInstance()
          .deleteNode(logNodePrefix + SystemFileFactory.INSTANCE.getFile(filePath).getName());
    } catch (IOException e) {
      throw new StorageGroupProcessorException(e);
    }
    updateTsFileResource();
    return new Pair<>(restorableTsFileIOWriter, vmRestorableTsFileIOWriterList);
  }

  private void updateTsFileResource() {
    for (List<TsFileResource> subTsFileResources : vmTsFileResources) {
      for (TsFileResource tsFileResource : subTsFileResources) {
        for (Entry<String, Integer> entry : tsFileResource.getDeviceToIndexMap().entrySet()) {
          String device = entry.getKey();
          int index = entry.getValue();
          resource.updateStartTime(device, tsFileResource.getStartTime(index));
          resource.updateEndTime(device, tsFileResource.getEndTime(index));
        }
      }
    }
  }

  private void recoverResource(TsFileResource tsFileResource) throws IOException {
    if (tsFileResource.resourceFileExists()) {
      // .resource file exists, deserialize it
      recoverResourceFromFile(tsFileResource);
    } else {
      // .resource file does not exist, read file metadata and recover tsfile resource
      try (TsFileSequenceReader reader = new TsFileSequenceReader(
          tsFileResource.getTsFile().getAbsolutePath())) {
        FileLoaderUtils.updateTsFileResource(reader, tsFileResource);
      }
      // write .resource file
      long fileVersion =
          Long.parseLong(
              tsFileResource.getTsFile().getName().split(IoTDBConstant.FILE_NAME_SEPARATOR)[1]);
      tsFileResource.setHistoricalVersions(Collections.singleton(fileVersion));
      tsFileResource.serialize();
    }
  }

  private void recoverResourceFromFile(TsFileResource tsFileResource) throws IOException {
    try {
      tsFileResource.deserialize();
    } catch (IOException e) {
      logger.warn("Cannot deserialize TsFileResource {}, construct it using "
          + "TsFileSequenceReader", tsFileResource.getTsFile(), e);
      recoverResourceFromReader(tsFileResource);
    }
  }


  private void recoverResourceFromReader(TsFileResource tsFileResource) throws IOException {
    try (TsFileSequenceReader reader =
        new TsFileSequenceReader(tsFileResource.getTsFile().getAbsolutePath(), true)) {
      for (Entry<String, List<TimeseriesMetadata>> entry : reader.getAllTimeseriesMetadata()
          .entrySet()) {
        for (TimeseriesMetadata timeseriesMetaData : entry.getValue()) {
          tsFileResource
              .updateStartTime(entry.getKey(), timeseriesMetaData.getStatistics().getStartTime());
          tsFileResource
              .updateEndTime(entry.getKey(), timeseriesMetaData.getStatistics().getEndTime());
        }
      }
    }
    // write .resource file
    tsFileResource.serialize();
  }


  private void recoverResourceFromWriter(RestorableTsFileIOWriter restorableTsFileIOWriter,
      TsFileResource tsFileResource) {
    Map<String, List<ChunkMetadata>> deviceChunkMetaDataMap =
        restorableTsFileIOWriter.getDeviceChunkMetadataMap();
    for (Map.Entry<String, List<ChunkMetadata>> entry : deviceChunkMetaDataMap.entrySet()) {
      String deviceId = entry.getKey();
      List<ChunkMetadata> chunkMetadataList = entry.getValue();
      for (ChunkMetadata chunkMetaData : chunkMetadataList) {
        tsFileResource.updateStartTime(deviceId, chunkMetaData.getStartTime());
        tsFileResource.updateEndTime(deviceId, chunkMetaData.getEndTime());
      }
    }
    long fileVersion = Long.parseLong(
        tsFileResource.getTsFile().getName().split(IoTDBConstant.FILE_NAME_SEPARATOR)[1]);
    tsFileResource.setHistoricalVersions(Collections.singleton(fileVersion));
  }

  private boolean redoLogs(RestorableTsFileIOWriter restorableTsFileIOWriter,
      TsFileResource tsFileResource) throws StorageGroupProcessorException {
    IMemTable recoverMemTable = new PrimitiveMemTable();
    recoverMemTable.setVersion(versionController.nextVersion());
    LogReplayer logReplayer = new LogReplayer(logNodePrefix, filePath, tsFileResource.getModFile(),
        versionController, tsFileResource, recoverMemTable, sequence);
    logReplayer.replayLogs();
    boolean res = false;
    try {
      if (!recoverMemTable.isEmpty()) {
        // flush logs
        MemTableFlushTask tableFlushTask = new MemTableFlushTask(recoverMemTable,
            restorableTsFileIOWriter,
            tsFileResource.getTsFile().getParentFile().getParentFile().getName());
        tableFlushTask.syncFlushMemTable();
        res = true;
      }

      if (!isLastFile || tsFileResource.isCloseFlagSet()) {
        // end the file if it is not the last file or it is closed before crush
        restorableTsFileIOWriter.endFile();
        tsFileResource.cleanCloseFlag();
        tsFileResource.serialize();
      }
      // otherwise this file is not closed before crush, do nothing so we can continue writing
      // into it
      return res;
    } catch (IOException | InterruptedException | ExecutionException e) {
      throw new StorageGroupProcessorException(e);
    }
  }

}
