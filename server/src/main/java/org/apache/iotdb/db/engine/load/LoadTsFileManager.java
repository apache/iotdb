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
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.file.SystemFileFactory;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.storagegroup.DataRegion;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.engine.storagegroup.TsFileResourceStatus;
import org.apache.iotdb.db.exception.LoadFileException;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.load.LoadTsFilePieceNode;
import org.apache.iotdb.db.mpp.plan.scheduler.load.LoadTsFileScheduler.LoadCommand;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;
import org.apache.iotdb.tsfile.exception.write.PageException;
import org.apache.iotdb.tsfile.file.metadata.TimeseriesMetadata;
import org.apache.iotdb.tsfile.write.writer.TsFileIOWriter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

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
  private final Map<String, Integer> dataPartition2NextTsFileIndex;

  private final ReentrantLock lock;

  public LoadTsFileManager() {
    this.loadDir = SystemFileFactory.INSTANCE.getFile(config.getLoadTsFileDir());
    this.uuid2WriterManager = new ConcurrentHashMap<>();
    this.dataPartition2NextTsFileIndex = new HashMap<>();
    this.lock = new ReentrantLock();

    if (loadDir.delete()) {
      logger.info(String.format("Delete origin load TsFile dir %s.", loadDir.getPath()));
    }
  }

  public void writeToDataRegion(DataRegion dataRegion, LoadTsFilePieceNode pieceNode, String uuid)
      throws PageException, IOException {
    TsFileWriterManager writerManager =
        uuid2WriterManager.computeIfAbsent(
            uuid, o -> new TsFileWriterManager(SystemFileFactory.INSTANCE.getFile(loadDir, uuid)));
    for (ChunkData chunkData : pieceNode.getAllChunkData()) {
      writerManager.write(
          new DataPartitionInfo(dataRegion, chunkData.getTimePartitionSlot()), chunkData);
    }
  }

  public boolean loadAll(String uuid) throws IOException, LoadFileException {
    if (!uuid2WriterManager.containsKey(uuid)) {
      return false;
    }
    uuid2WriterManager.get(uuid).loadAll();
    uuid2WriterManager.get(uuid).close();
    uuid2WriterManager.remove(uuid);
    clean();
    return true;
  }

  public boolean deleteAll(String uuid) throws IOException {
    if (!uuid2WriterManager.containsKey(uuid)) {
      return false;
    }
    uuid2WriterManager.get(uuid).close();
    uuid2WriterManager.remove(uuid);
    clean();
    return true;
  }

  private String getNewTsFileName(String dataPartition) {
    lock.lock();
    try {
      int nextIndex = dataPartition2NextTsFileIndex.getOrDefault(dataPartition, 0) + 1;
      dataPartition2NextTsFileIndex.put(dataPartition, nextIndex);
      return dataPartition
          + IoTDBConstant.FILE_NAME_SEPARATOR
          + nextIndex
          + TsFileConstant.TSFILE_SUFFIX;
    } finally {
      lock.unlock();
    }
  }

  private void clean() {
    if (loadDir.delete()) { // this method will check if there sub-dir in this dir.
      logger.info(String.format("Delete load dir %s.", loadDir.getPath()));
    }
  }

  private class TsFileWriterManager {
    private final File taskDir;
    private Map<DataPartitionInfo, TsFileIOWriter> dataPartition2Writer;
    private Map<DataPartitionInfo, String> dataPartition2LastDevice;

    private TsFileWriterManager(File taskDir) {
      this.taskDir = taskDir;
      this.dataPartition2Writer = new HashMap<>();
      this.dataPartition2LastDevice = new HashMap<>();

      clearDir(taskDir);
    }

    private void clearDir(File dir) {
      if (dir.delete()) {
        logger.info(String.format("Delete origin load TsFile dir %s.", dir.getPath()));
      }
      if (!dir.mkdirs()) {
        logger.warn(String.format("load TsFile dir %s can not be created.", dir.getPath()));
      }
    }

    private void write(DataPartitionInfo partitionInfo, ChunkData chunkData) throws IOException {
      if (!dataPartition2Writer.containsKey(partitionInfo)) {
        File newTsFile =
            SystemFileFactory.INSTANCE.getFile(taskDir, getNewTsFileName(partitionInfo.toString()));
        if (!newTsFile.createNewFile()) {
          logger.error(String.format("Can not create TsFile %s for writing.", newTsFile.getPath()));
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

    private void loadAll() throws IOException, LoadFileException {
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
      TsFileResource tsFileResource = new TsFileResource(writer.getFile());
      Map<String, List<TimeseriesMetadata>> deviceTimeseriesMetadataMap =
          writer.getDeviceTimeseriesMetadataMap();
      for (Map.Entry<String, List<TimeseriesMetadata>> entry :
          deviceTimeseriesMetadataMap.entrySet()) {
        String device = entry.getKey();
        for (TimeseriesMetadata timeseriesMetaData : entry.getValue()) {
          tsFileResource.updateStartTime(device, timeseriesMetaData.getStatistics().getStartTime());
          tsFileResource.updateEndTime(device, timeseriesMetaData.getStatistics().getEndTime());
        }
      }
      tsFileResource.setStatus(TsFileResourceStatus.CLOSED);
      tsFileResource.serialize();
      return tsFileResource;
    }

    private void close() throws IOException {
      for (Map.Entry<DataPartitionInfo, TsFileIOWriter> entry : dataPartition2Writer.entrySet()) {
        TsFileIOWriter writer = entry.getValue();
        if (writer.canWrite()) {
          entry.getValue().close();
        }
      }
      if (!taskDir.delete()) {
        logger.warn(String.format("Can not delete load uuid dir %s.", taskDir.getPath()));
      }
      dataPartition2Writer = null;
      dataPartition2LastDevice = null;
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
          dataRegion.getStorageGroupName(),
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
