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
import org.apache.iotdb.db.mpp.plan.planner.plan.node.load.LoadTsFilePieceNode;
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
import java.util.concurrent.locks.ReentrantLock;

public class LoadTsFileManager {
  private static final Logger logger = LoggerFactory.getLogger(LoadTsFileManager.class);
  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  private final File loadDir;

  private Map<String, TsFileWriterManager> uuid2WriterManager;
  private Map<String, Integer> dataPartition2NextTsFileIndex;

  private final ReentrantLock lock;

  public LoadTsFileManager() {
    this.loadDir = SystemFileFactory.INSTANCE.getFile(config.getLoadTsFileDir());
    this.uuid2WriterManager = new HashMap<>();
    this.dataPartition2NextTsFileIndex = new HashMap<>();
    this.lock = new ReentrantLock();

    clearDir(loadDir);
  }

  private void clearDir(File dir) {
    if (dir.delete()) {
      logger.info(String.format("Delete origin load TsFile dir %s.", dir.getPath()));
    }
    if (dir.mkdirs()) {
      logger.warn(String.format("load TsFile dir %s can not be created.", dir.getPath()));
    }
  }

  public void writeToDataRegion(DataRegion dataRegion, LoadTsFilePieceNode pieceNode, String uuid)
      throws PageException, IOException {
    TsFileWriterManager writerManager =
        uuid2WriterManager.computeIfAbsent(
            uuid, o -> new TsFileWriterManager(SystemFileFactory.INSTANCE.getFile(loadDir, uuid)));
    for (ChunkData chunkData : pieceNode.getAllChunkData()) {
      writerManager.write(
          getDataPartition(
              dataRegion.getLogicalStorageGroupName(),
              dataRegion.getDataRegionId(),
              chunkData.getTimePartitionSlot()),
          chunkData);
    }
  }

  public boolean loadAll(String uuid) throws IOException {
    if (!uuid2WriterManager.containsKey(uuid)) {
      return false;
    }
    uuid2WriterManager.get(uuid).loadAll();
    return true;
  }

  public boolean deleteAll(String uuid) throws IOException {
    if (!uuid2WriterManager.containsKey(uuid)) {
      return false;
    }
    uuid2WriterManager.get(uuid).close();
    return true;
  }

  private String getDataPartition(
      String logicalName, String dataRegionId, TTimePartitionSlot timePartitionSlot) {
    return String.join(
        IoTDBConstant.FILE_NAME_SEPARATOR,
        logicalName,
        dataRegionId,
        Long.toString(timePartitionSlot.getStartTime()));
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

  private class TsFileWriterManager {
    private final File taskDir;
    private Map<String, TsFileIOWriter> dataPartition2Writer;
    private Map<String, String> dataPartition2LastDevice;

    private TsFileWriterManager(File taskDir) {
      this.taskDir = taskDir;
      this.dataPartition2Writer = new HashMap<>();
      this.dataPartition2LastDevice = new HashMap<>();

      clearDir(taskDir);
    }

    private void write(String dataPartition, ChunkData chunkData)
        throws IOException, PageException {
      if (!dataPartition2Writer.containsKey(dataPartition)) {
        File newTsFile =
            SystemFileFactory.INSTANCE.getFile(taskDir, getNewTsFileName(dataPartition));
        if (!newTsFile.createNewFile()) {
          logger.error(String.format("Can not create TsFile %s for writing.", newTsFile.getPath()));
          return;
        }

        dataPartition2Writer.put(dataPartition, new TsFileIOWriter(newTsFile));
      }
      TsFileIOWriter writer = dataPartition2Writer.get(dataPartition);
      if (!chunkData.getDevice().equals(dataPartition2LastDevice.getOrDefault(dataPartition, ""))) {
        if (dataPartition2LastDevice.containsKey(dataPartition)) {
          writer.endChunkGroup();
        }
        writer.startChunkGroup(chunkData.getDevice());
      }
      chunkData.getChunkWriter().writeToFileWriter(writer); // TODO: get writer
    }

    private void loadAll() throws IOException {
      for (Map.Entry<String, TsFileIOWriter> entry : dataPartition2Writer.entrySet()) {
        TsFileIOWriter writer = entry.getValue();
        if (writer.isWritingChunkGroup()) {
          writer.endChunkGroup();
        }
        writer.endFile();
        generateResource(writer); // TODO: load TsFileResource
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
      for (Map.Entry<String, TsFileIOWriter> entry : dataPartition2Writer.entrySet()) {
        entry.getValue().close();
      }
      if (taskDir.delete()) {
        logger.warn(String.format("Can not delete load uuid dir %s.", taskDir.getPath()));
      }
    }
  }
}
