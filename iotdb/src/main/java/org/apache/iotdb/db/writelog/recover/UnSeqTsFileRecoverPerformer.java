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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import org.apache.iotdb.db.engine.filenode.TsFileResource;
import org.apache.iotdb.db.engine.memtable.IMemTable;
import org.apache.iotdb.db.engine.memtable.MemTableFlushTask;
import org.apache.iotdb.db.engine.memtable.PrimitiveMemTable;
import org.apache.iotdb.db.engine.version.VersionController;
import org.apache.iotdb.db.exception.ProcessorException;
import org.apache.iotdb.db.writelog.manager.MultiFileLogNodeManager;
import org.apache.iotdb.tsfile.file.metadata.ChunkGroupMetaData;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetaData;
import org.apache.iotdb.tsfile.write.schema.FileSchema;
import org.apache.iotdb.tsfile.write.writer.NativeRestorableIOWriter;

/**
 * SeqTsFileRecoverPerformer recovers a SeqTsFile to correct status, redoes the WALs since last
 * crash and removes the redone logs.
 */
public class UnSeqTsFileRecoverPerformer {

  private String insertFilePath;
  private String logNodePrefix;
  private FileSchema fileSchema;
  private VersionController versionController;
  private LogReplayer logReplayer;
  private TsFileResource tsFileResource;

  public UnSeqTsFileRecoverPerformer(String logNodePrefix,
      FileSchema fileSchema, VersionController versionController,
      TsFileResource currentTsFileResource) {
    this.insertFilePath = currentTsFileResource.getFilePath();
    this.logNodePrefix = logNodePrefix;
    this.fileSchema = fileSchema;
    this.versionController = versionController;
    this.tsFileResource = currentTsFileResource;
  }

  /**
   * 1. redo the WALs to recover unpersisted data
   * 2. flush and close the file
   * 3. clean WALs
   * @throws ProcessorException
   */
  public void recover() throws ProcessorException {
    IMemTable recoverMemTable = new PrimitiveMemTable();
    this.logReplayer = new LogReplayer(logNodePrefix, insertFilePath, tsFileResource.getModFile(),
        versionController,
        tsFileResource, fileSchema, recoverMemTable, true);
    File insertFile = new File(insertFilePath);
    if (!insertFile.exists()) {
      return;
    }
    // remove corrupted part of the TsFile
    NativeRestorableIOWriter restorableTsFileIOWriter = null;
    try {
      restorableTsFileIOWriter = new NativeRestorableIOWriter(insertFile);
    } catch (IOException e) {
      throw new ProcessorException(e);
    }

    for (ChunkGroupMetaData chunkGroupMetaData : restorableTsFileIOWriter.getChunkGroupMetaDatas()) {
      for (ChunkMetaData chunkMetaData : chunkGroupMetaData.getChunkMetaDataList()) {
        tsFileResource.updateTime(chunkGroupMetaData.getDeviceID(), chunkMetaData.getStartTime());
        tsFileResource.updateTime(chunkGroupMetaData.getDeviceID(), chunkMetaData.getEndTime());
      }
    }

    // redo logs
    logReplayer.replayLogs();
    if (recoverMemTable.isEmpty()) {
      return;
    }

    // flush logs
    MemTableFlushTask tableFlushTask = new MemTableFlushTask(restorableTsFileIOWriter,
        logNodePrefix, 0, (a,b) -> {});
    tableFlushTask.flushMemTable(fileSchema, recoverMemTable, versionController.nextVersion());

    // close file
    try {
      restorableTsFileIOWriter.endFile(fileSchema);
    } catch (IOException e) {
      throw new ProcessorException("Cannot setCloseMark file when recovering", e);
    }

    // clean logs
    try {
      MultiFileLogNodeManager.getInstance().deleteNode(logNodePrefix + new File(insertFilePath).getName());
    } catch (IOException e) {
      throw new ProcessorException(e);
    }
  }
}
