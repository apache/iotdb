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
import org.apache.iotdb.db.engine.modification.ModificationFile;
import org.apache.iotdb.db.engine.version.VersionController;
import org.apache.iotdb.db.exception.ProcessorException;
import org.apache.iotdb.db.writelog.manager.MultiFileLogNodeManager;
import org.apache.iotdb.db.writelog.node.WriteLogNode;
import org.apache.iotdb.tsfile.write.schema.FileSchema;
import org.apache.iotdb.tsfile.write.writer.NativeRestorableIOWriter;

public class TsFileRecoverPerformer {

  private String insertFilePath;
  private String processorName;
  private FileSchema fileSchema;
  private VersionController versionController;
  private LogReplayer logReplayer;
  private IMemTable recoverMemTable;

  public TsFileRecoverPerformer(String insertFilePath, String processorName,
      FileSchema fileSchema, VersionController versionController,
      TsFileResource currentTsFileResource, ModificationFile modFile) {
    this.insertFilePath = insertFilePath;
    this.processorName = processorName;
    this.fileSchema = fileSchema;
    this.versionController = versionController;
    this.recoverMemTable = new PrimitiveMemTable();
    this.logReplayer = new LogReplayer(processorName, insertFilePath, modFile, versionController,
        currentTsFileResource, fileSchema, recoverMemTable);
  }

  public boolean recover() throws ProcessorException {
    File insertFile = new File(insertFilePath);
    if (!insertFile.exists()) {
      return false;
    }
    NativeRestorableIOWriter restorableTsFileIOWriter = recoverFile(insertFile);

    logReplayer.replayLogs();

    MemTableFlushTask tableFlushTask = new MemTableFlushTask(restorableTsFileIOWriter,
        processorName, 0, (a,b) -> {});
    tableFlushTask.flushMemTable(fileSchema, recoverMemTable, versionController.nextVersion());

    try {
      restorableTsFileIOWriter.endFile(fileSchema);
    } catch (IOException e) {
      throw new ProcessorException("Cannot setCloseMark file when recovering", e);
    }

    removeTruncatePosition(insertFile);

    WriteLogNode logNode;
    try {
      logNode = MultiFileLogNodeManager.getInstance().getNode(
          processorName + new File(insertFilePath).getName());
      logNode.delete();
    } catch (IOException e) {
      throw new ProcessorException(e);
    }
    return true;
  }


  private NativeRestorableIOWriter recoverFile(File insertFile) throws ProcessorException {
    long truncatePos = getTruncatePosition(insertFile);
    NativeRestorableIOWriter restorableIOWriter;
    if (truncatePos != -1 && insertFile.length() != truncatePos) {
      try (FileChannel channel = new FileOutputStream(insertFile).getChannel()) {
        channel.truncate(truncatePos);
        restorableIOWriter = new NativeRestorableIOWriter(insertFile, true);
      } catch (IOException e) {
        throw new ProcessorException(e);
      }
    } else {
      try {
        restorableIOWriter = new NativeRestorableIOWriter(insertFile, true);
        saveTruncatePosition(insertFile);
      } catch (IOException e) {
        throw new ProcessorException(e);
      }
    }
    return restorableIOWriter;
  }

  private long getTruncatePosition(File insertFile) {
    File parentDir = insertFile.getParentFile();
    File[] truncatePoses = parentDir.listFiles((dir, name) -> name.contains(insertFile.getName() +
        "@"));
    long maxPos = -1;
    if (truncatePoses != null) {
      for (File truncatePos : truncatePoses) {
        long pos = Long.parseLong(truncatePos.getName().split("@")[1]);
        if (pos > maxPos) {
          maxPos = pos;
        }
      }
    }
    return maxPos;
  }

  private void saveTruncatePosition(File insertFile)
      throws IOException {
    File truncatePosFile = new File(insertFile.getParent(),
        insertFile.getName() + "@" + insertFile.length());
    try (FileOutputStream outputStream = new FileOutputStream(truncatePosFile)) {
      outputStream.write(0);
      outputStream.flush();
    }
  }

  private void removeTruncatePosition(File insertFile) {
    File parentDir = insertFile.getParentFile();
    File[] truncatePoses = parentDir.listFiles((dir, name) -> name.contains(insertFile.getName() +
        "@"));
    if (truncatePoses != null) {
      for (File truncatePos : truncatePoses) {
        truncatePos.delete();
      }
    }
  }
}
