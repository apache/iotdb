/**
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

import static org.apache.iotdb.db.engine.storagegroup.TsFileResource.RESOURCE_SUFFIX;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.ExecutionException;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.engine.memtable.IMemTable;
import org.apache.iotdb.db.engine.memtable.MemTableFlushTask;
import org.apache.iotdb.db.engine.memtable.PrimitiveMemTable;
import org.apache.iotdb.db.engine.version.VersionController;
import org.apache.iotdb.db.exception.ProcessorException;
import org.apache.iotdb.db.writelog.manager.MultiFileLogNodeManager;
import org.apache.iotdb.tsfile.file.metadata.ChunkGroupMetaData;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetaData;
import org.apache.iotdb.tsfile.write.schema.FileSchema;
import org.apache.iotdb.tsfile.write.writer.RestorableTsFileIOWriter;

/**
 * TsFileRecoverPerformer recovers a SeqTsFile to correct status, redoes the WALs since last
 * crash and removes the redone logs.
 */
public class TsFileRecoverPerformer {

  private String insertFilePath;
  private String logNodePrefix;
  private FileSchema fileSchema;
  private VersionController versionController;
  private LogReplayer logReplayer;
  private TsFileResource tsFileResource;
  private boolean acceptUnseq;

  public TsFileRecoverPerformer(String logNodePrefix,
      FileSchema fileSchema, VersionController versionController,
      TsFileResource currentTsFileResource, boolean acceptUnseq) {
    this.insertFilePath = currentTsFileResource.getFile().getPath();
    this.logNodePrefix = logNodePrefix;
    this.fileSchema = fileSchema;
    this.versionController = versionController;
    this.tsFileResource = currentTsFileResource;
    this.acceptUnseq = acceptUnseq;
  }

  /**
   * 1. recover the TsFile by RestorableTsFileIOWriter and truncate position of last recovery
   * 2. redo the WALs to recover unpersisted data
   * 3. flush and close the file
   * 4. clean WALs
   * @throws ProcessorException
   */
  public void recover() throws ProcessorException {
    IMemTable recoverMemTable = new PrimitiveMemTable();
    this.logReplayer = new LogReplayer(logNodePrefix, insertFilePath, tsFileResource.getModFile(),
        versionController,
        tsFileResource, fileSchema, recoverMemTable, acceptUnseq);
    File insertFile = new File(insertFilePath);
    if (!insertFile.exists()) {
      return;
    }
    // remove corrupted part of the TsFile
    RestorableTsFileIOWriter restorableTsFileIOWriter;
    try {
      restorableTsFileIOWriter = new RestorableTsFileIOWriter(insertFile);
    } catch (IOException e) {
      throw new ProcessorException(e);
    }

    if (!restorableTsFileIOWriter.hasCrashed()) {
      try {
        // recover two maps of time
        tsFileResource.deSerialize();
        return;
      } catch (IOException e) {
        throw new ProcessorException("recover the resource file failed: " + insertFilePath
            + RESOURCE_SUFFIX, e);
      }
    } else {
      // due to failure, the last ChunkGroup may contain the same data as the WALs, so the time
      // map must be updated first to avoid duplicated insertion
      for (ChunkGroupMetaData chunkGroupMetaData : restorableTsFileIOWriter.getChunkGroupMetaDatas()) {
        for (ChunkMetaData chunkMetaData : chunkGroupMetaData.getChunkMetaDataList()) {
          tsFileResource.updateTime(chunkGroupMetaData.getDeviceID(), chunkMetaData.getStartTime());
          tsFileResource.updateTime(chunkGroupMetaData.getDeviceID(), chunkMetaData.getEndTime());
        }
      }
    }

    // redo logs
    logReplayer.replayLogs();
    if (!recoverMemTable.isEmpty()) {
      // flush logs
      MemTableFlushTask tableFlushTask = new MemTableFlushTask(recoverMemTable, fileSchema, restorableTsFileIOWriter,
          logNodePrefix);
      try {
        tableFlushTask.flushMemTable();
      } catch (ExecutionException | InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new ProcessorException(e);
      }
    }

    // close file
    try {
      restorableTsFileIOWriter.endFile(fileSchema);
      tsFileResource.serialize();
    } catch (IOException e) {
      throw new ProcessorException("Cannot close file when recovering", e);
    }

    // clean logs
    try {
      MultiFileLogNodeManager.getInstance().deleteNode(logNodePrefix + new File(insertFilePath).getName());
    } catch (IOException e) {
      throw new ProcessorException(e);
    }
  }

}
