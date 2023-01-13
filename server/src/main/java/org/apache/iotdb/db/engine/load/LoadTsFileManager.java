/*
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

package org.apache.iotdb.db.engine.load;

import org.apache.iotdb.common.rpc.thrift.TTimePartitionSlot;
import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.file.SystemFileFactory;
import org.apache.iotdb.commons.utils.FileUtils;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.storagegroup.DataRegion;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.LoadFileException;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.load.LoadTsFilePieceNode;
import org.apache.iotdb.db.mpp.plan.scheduler.load.LoadTsFileScheduler;
import org.apache.iotdb.db.mpp.plan.scheduler.load.LoadTsFileScheduler.LoadCommand;
import org.apache.iotdb.db.utils.FileLoaderUtils;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;
import org.apache.iotdb.tsfile.exception.write.PageException;
import org.apache.iotdb.tsfile.write.writer.TsFileIOWriter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * {@link LoadTsFileManager} is used for dealing with {@link LoadTsFilePieceNode} and {@link
 * LoadCommand}. This class turn the content of a piece of loading TsFile into a new TsFile. When
 * DataNode finish transfer pieces, this class will flush all TsFile and laod them into IoTDB, or
 * delete all.
 */
public class LoadTsFileManager {
  private static final Logger logger = LoggerFactory.getLogger(LoadTsFileManager.class);
  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  private final File loadDir;

  private final Map<String, TsFileWriterManager> uuid2WriterManager;

  private final ScheduledExecutorService cleanupExecutors;
  private final Map<String, ScheduledFuture<?>> uuid2Future;

  public LoadTsFileManager() {
    this.loadDir = SystemFileFactory.INSTANCE.getFile(config.getLoadTsFileDir());
    this.uuid2WriterManager = new ConcurrentHashMap<>();
    this.cleanupExecutors =
        IoTDBThreadPoolFactory.newScheduledThreadPool(0, LoadTsFileManager.class.getName());
    this.uuid2Future = new ConcurrentHashMap<>();

    recover();
  }

  private void recover() {
    if (!loadDir.exists()) {
      return;
    }

    for (File taskDir : loadDir.listFiles()) {
      String uuid = taskDir.getName();
      TsFileWriterManager writerManager = new TsFileWriterManager(taskDir);

      uuid2WriterManager.put(uuid, writerManager);
      writerManager.close();
      uuid2Future.put(
          uuid,
          cleanupExecutors.schedule(
              () -> forceCloseWriterManager(uuid),
              LoadTsFileScheduler.LOAD_TASK_MAX_TIME_IN_SECOND,
              TimeUnit.SECONDS));
    }
  }

  public void writeToDataRegion(DataRegion dataRegion, LoadTsFilePieceNode pieceNode, String uuid)
      throws PageException, IOException {
    if (!uuid2WriterManager.containsKey(uuid)) {
      uuid2Future.put(
          uuid,
          cleanupExecutors.schedule(
              () -> forceCloseWriterManager(uuid),
              LoadTsFileScheduler.LOAD_TASK_MAX_TIME_IN_SECOND,
              TimeUnit.SECONDS));
    }
    TsFileWriterManager writerManager =
        uuid2WriterManager.computeIfAbsent(
            uuid, o -> new TsFileWriterManager(SystemFileFactory.INSTANCE.getFile(loadDir, uuid)));
    for (TsFileData tsFileData : pieceNode.getAllTsFileData()) {
      if (!tsFileData.isModification()) {
        ChunkData chunkData = (ChunkData) tsFileData;
        writerManager.write(
            new DataPartitionInfo(dataRegion, chunkData.getTimePartitionSlot()), chunkData);
      } else {
        writerManager.writeDeletion(tsFileData);
      }
    }
  }

  public boolean loadAll(String uuid) throws IOException, LoadFileException {
    if (!uuid2WriterManager.containsKey(uuid)) {
      return false;
    }
    uuid2WriterManager.get(uuid).loadAll();
    clean(uuid);
    return true;
  }

  public boolean deleteAll(String uuid) throws IOException {
    if (!uuid2WriterManager.containsKey(uuid)) {
      return false;
    }
    clean(uuid);
    return true;
  }

  private void clean(String uuid) {
    uuid2WriterManager.get(uuid).close();
    uuid2WriterManager.remove(uuid);
    uuid2Future.get(uuid).cancel(true);
    uuid2Future.remove(uuid);

    if (loadDir.delete()) { // this method will check if there sub-dir in this dir.
      logger.info("Delete load dir {}.", loadDir.getPath());
    }
  }

  private void forceCloseWriterManager(String uuid) {
    uuid2WriterManager.get(uuid).close();
    uuid2WriterManager.remove(uuid);
    uuid2Future.remove(uuid);

    if (loadDir.delete()) { // this method will check if there sub-dir in this dir.
      logger.info("Delete load dir {}.", loadDir.getPath());
    }
  }

  private class TsFileWriterManager {
    private final File taskDir;
    private Map<DataPartitionInfo, TsFileIOWriter> dataPartition2Writer;
    private Map<DataPartitionInfo, String> dataPartition2LastDevice;
    private boolean isClosed;

    private TsFileWriterManager(File taskDir) {
      this.taskDir = taskDir;
      this.dataPartition2Writer = new HashMap<>();
      this.dataPartition2LastDevice = new HashMap<>();
      this.isClosed = false;

      clearDir(taskDir);
    }

    private void clearDir(File dir) {
      if (dir.exists()) {
        FileUtils.deleteDirectory(dir);
      }
      if (dir.mkdirs()) {
        logger.info("Load TsFile dir {} is created.", dir.getPath());
      }
    }

    private void write(DataPartitionInfo partitionInfo, ChunkData chunkData) throws IOException {
      if (isClosed) {
        throw new IOException(String.format("%s TsFileWriterManager has been closed.", taskDir));
      }
      if (!dataPartition2Writer.containsKey(partitionInfo)) {
        File newTsFile =
            SystemFileFactory.INSTANCE.getFile(
                taskDir, partitionInfo.toString() + TsFileConstant.TSFILE_SUFFIX);
        if (!newTsFile.createNewFile()) {
          logger.error("Can not create TsFile {} for writing.", newTsFile.getPath());
          return;
        }

        dataPartition2Writer.put(partitionInfo, new TsFileIOWriter(newTsFile));
      }
      TsFileIOWriter writer = dataPartition2Writer.get(partitionInfo);
      if (!chunkData.getDevice().equals(dataPartition2LastDevice.getOrDefault(partitionInfo, ""))) {
        if (dataPartition2LastDevice.containsKey(partitionInfo)) {
          writer.endChunkGroup();
        }
        writer.startChunkGroup(chunkData.getDevice());
        dataPartition2LastDevice.put(partitionInfo, chunkData.getDevice());
      }
      chunkData.writeToFileWriter(writer);
    }

    private void writeDeletion(TsFileData deletionData) throws IOException {
      if (isClosed) {
        throw new IOException(String.format("%s TsFileWriterManager has been closed.", taskDir));
      }
      for (Map.Entry<DataPartitionInfo, TsFileIOWriter> entry : dataPartition2Writer.entrySet()) {
        deletionData.writeToFileWriter(entry.getValue());
      }
    }

    private void loadAll() throws IOException, LoadFileException {
      if (isClosed) {
        throw new IOException(String.format("%s TsFileWriterManager has been closed.", taskDir));
      }
      for (Map.Entry<DataPartitionInfo, TsFileIOWriter> entry : dataPartition2Writer.entrySet()) {
        TsFileIOWriter writer = entry.getValue();
        if (writer.isWritingChunkGroup()) {
          writer.endChunkGroup();
        }
        writer.endFile();
        entry.getKey().getDataRegion().loadNewTsFile(generateResource(writer), true);
      }
    }

    private TsFileResource generateResource(TsFileIOWriter writer) throws IOException {
      TsFileResource tsFileResource = FileLoaderUtils.generateTsFileResource(writer);
      tsFileResource.serialize();
      return tsFileResource;
    }

    private void close() {
      if (isClosed) {
        return;
      }
      if (dataPartition2Writer != null) {
        for (Map.Entry<DataPartitionInfo, TsFileIOWriter> entry : dataPartition2Writer.entrySet()) {
          try {
            TsFileIOWriter writer = entry.getValue();
            if (writer.canWrite()) {
              writer.close();
            }
            if (writer.getFile().exists() && !writer.getFile().delete()) {
              logger.warn("Delete File {} error.", writer.getFile());
            }
          } catch (IOException e) {
            logger.warn(
                String.format(
                    "Close TsFileIOWriter %s error.", entry.getValue().getFile().getPath()),
                e);
          }
        }
      }
      if (!taskDir.delete()) {
        logger.warn("Can not delete load dir {}.", taskDir.getPath());
      }
      dataPartition2Writer = null;
      dataPartition2LastDevice = null;
      isClosed = true;
    }
  }

  private class DataPartitionInfo {
    private final DataRegion dataRegion;
    private final TTimePartitionSlot timePartitionSlot;

    private DataPartitionInfo(DataRegion dataRegion, TTimePartitionSlot timePartitionSlot) {
      this.dataRegion = dataRegion;
      this.timePartitionSlot = timePartitionSlot;
    }

    public DataRegion getDataRegion() {
      return dataRegion;
    }

    public TTimePartitionSlot getTimePartitionSlot() {
      return timePartitionSlot;
    }

    @Override
    public String toString() {
      return String.join(
          IoTDBConstant.FILE_NAME_SEPARATOR,
          dataRegion.getDatabaseName(),
          dataRegion.getDataRegionId(),
          Long.toString(timePartitionSlot.getStartTime()));
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      DataPartitionInfo that = (DataPartitionInfo) o;
      return Objects.equals(dataRegion, that.dataRegion)
          && timePartitionSlot.getStartTime() == that.timePartitionSlot.getStartTime();
    }

    @Override
    public int hashCode() {
      return Objects.hash(dataRegion, timePartitionSlot.getStartTime());
    }
  }
}
