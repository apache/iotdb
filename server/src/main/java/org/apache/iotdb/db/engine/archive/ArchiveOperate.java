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
package org.apache.iotdb.db.engine.archive;

import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

/** ArchiveOperate is ArchiveOperateType (SET, CANCEL, PAUSE, etc) and an ArchiveTask */
public class ArchiveOperate {
  private ArchiveOperateType type;
  private ArchiveTask task;

  public ArchiveOperate(ArchiveOperateType type, ArchiveTask task) {
    this.type = type;
    this.task = new ArchiveTask(task);
  }

  public ArchiveOperate(ArchiveOperateType type, long taskId) {
    this.type = type;
    this.task = new ArchiveTask(taskId);
  }

  public ArchiveOperateType getType() {
    return type;
  }

  public ArchiveTask getTask() {
    return task;
  }

  public void serialize(FileOutputStream logFileOutStream) throws IOException {
    int typeNum = type.ordinal();
    ReadWriteIOUtils.write((byte) typeNum, logFileOutStream);
    ReadWriteIOUtils.write(task.getTaskId(), logFileOutStream);

    if (type == ArchiveOperateType.SET) {
      ReadWriteIOUtils.write(task.getStorageGroup().getFullPath(), logFileOutStream);
      ReadWriteIOUtils.write(task.getTargetDir().getPath(), logFileOutStream);
      ReadWriteIOUtils.write(task.getStartTime(), logFileOutStream);
      ReadWriteIOUtils.write(task.getTTL(), logFileOutStream);
      ReadWriteIOUtils.write(task.getSubmitTime(), logFileOutStream);
    }

    logFileOutStream.flush();
  }

  public static ArchiveOperate deserialize(FileInputStream logFileInStream)
      throws IOException, IllegalPathException {
    ArchiveOperateType operateType;

    int typeNum = ReadWriteIOUtils.readByte(logFileInStream);
    if (typeNum >= 0 && typeNum < ArchiveOperateType.values().length)
      operateType = ArchiveOperateType.values()[typeNum];
    else throw new IOException();

    long taskId = ReadWriteIOUtils.readLong(logFileInStream);

    ArchiveTask deserializedTask;
    if (operateType == ArchiveOperateType.SET) {
      PartialPath storageGroup = new PartialPath(ReadWriteIOUtils.readString(logFileInStream));
      String targetDirPath = ReadWriteIOUtils.readString(logFileInStream);
      File targetDir = FSFactoryProducer.getFSFactory().getFile(targetDirPath);
      long startTime = ReadWriteIOUtils.readLong(logFileInStream);
      long ttl = ReadWriteIOUtils.readLong(logFileInStream);
      long submitTime = ReadWriteIOUtils.readLong(logFileInStream);

      deserializedTask =
          new ArchiveTask(taskId, storageGroup, targetDir, ttl, startTime, submitTime);
    } else {
      deserializedTask = new ArchiveTask(taskId);
    }
    return new ArchiveOperate(operateType, deserializedTask);
  }

  public enum ArchiveOperateType {
    SET,
    CANCEL,
    START,
    PAUSE,
    RESUME,
    FINISHED,
    ERROR
  }
}
