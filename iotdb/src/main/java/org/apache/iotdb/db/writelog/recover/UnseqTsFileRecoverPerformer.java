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
import java.io.IOException;
import java.util.List;
import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.engine.memtable.IMemTable;
import org.apache.iotdb.db.engine.memtable.MemTableFlushCallBack;
import org.apache.iotdb.db.engine.memtable.MemTableFlushTask;
import org.apache.iotdb.db.engine.memtable.PrimitiveMemTable;
import org.apache.iotdb.db.engine.modification.ModificationFile;
import org.apache.iotdb.db.engine.overflow.io.OverflowIO;
import org.apache.iotdb.db.engine.version.VersionController;
import org.apache.iotdb.db.exception.ProcessorException;
import org.apache.iotdb.db.writelog.manager.MultiFileLogNodeManager;
import org.apache.iotdb.db.writelog.node.WriteLogNode;
import org.apache.iotdb.tsfile.file.metadata.ChunkGroupMetaData;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetaData;
import org.apache.iotdb.tsfile.file.metadata.TsDeviceMetadata;
import org.apache.iotdb.tsfile.utils.BytesUtils;
import org.apache.iotdb.tsfile.write.schema.FileSchema;

public class UnseqTsFileRecoverPerformer {

  private String logNodePrefix;
  private String insertFilePath;
  private ModificationFile modFile;
  private VersionController versionController;
  private FileSchema fileSchema;
  private OverflowIO overflowIO;
  private List<ChunkGroupMetaData> appendInsertMetadatas;

  public UnseqTsFileRecoverPerformer(String logNodePrefix, String insertFilePath,
      ModificationFile modFile,
      VersionController versionController, FileSchema fileSchema,
      OverflowIO overflowIO,
      List<ChunkGroupMetaData> appendInsertMetadatas) {
    this.logNodePrefix = logNodePrefix;
    this.insertFilePath = insertFilePath;
    this.modFile = modFile;
    this.versionController = versionController;
    this.fileSchema = fileSchema;
    this.overflowIO = overflowIO;
    this.appendInsertMetadatas = appendInsertMetadatas;
  }

  public void recover() throws ProcessorException {
    IMemTable memTable = new PrimitiveMemTable();
    LogReplayer replayer = new LogReplayer(logNodePrefix, insertFilePath,
        modFile, versionController, null, fileSchema, memTable);
    replayer.replayLogs();
    try {
      flush(memTable);
      MultiFileLogNodeManager.getInstance().deleteNode(logNodePrefix + new File(insertFilePath).getName());
    } catch (IOException e) {
      throw new ProcessorException(e);
    }
  }

  public void flush(IMemTable memTable) throws IOException {
    if (memTable != null && !memTable.isEmpty()) {
      overflowIO.toTail();
      long lastPosition = overflowIO.getPos();
      MemTableFlushTask tableFlushTask = new MemTableFlushTask(overflowIO, logNodePrefix, 0,
          (a,b) -> {});
      tableFlushTask.flushMemTable(fileSchema, memTable, versionController.nextVersion());

      List<ChunkGroupMetaData> rowGroupMetaDatas = overflowIO.getChunkGroupMetaDatas();
      appendInsertMetadatas.addAll(rowGroupMetaDatas);
      if (!rowGroupMetaDatas.isEmpty()) {
        overflowIO.getWriter().write(BytesUtils.longToBytes(lastPosition));
        TsDeviceMetadata tsDeviceMetadata = new TsDeviceMetadata();
        tsDeviceMetadata.setChunkGroupMetadataList(rowGroupMetaDatas);
        long start = overflowIO.getPos();
        tsDeviceMetadata.serializeTo(overflowIO.getOutputStream());
        long end = overflowIO.getPos();
        overflowIO.getWriter().write(BytesUtils.intToBytes((int) (end - start)));
        // clear the meta-data of insert IO
        overflowIO.clearRowGroupMetadatas();
      }
    }
  }
}
