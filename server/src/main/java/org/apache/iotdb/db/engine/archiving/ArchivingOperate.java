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
package org.apache.iotdb.db.engine.archiving;

import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

/** ArchivingOperate is ArchivingOperateType (SET, CANCEL, PAUSE, etc) and an ArchivingTask */
public class ArchivingOperate {
  private ArchivingOperateType type;
  private ArchivingTask task;

  public ArchivingOperate(ArchivingOperateType type, ArchivingTask task) {
    this.type = type;
    this.task = new ArchivingTask(task);
  }

  public ArchivingOperate(ArchivingOperateType type, long taskId) {
    this.type = type;
    this.task = new ArchivingTask(taskId);
  }

  public ArchivingOperateType getType() {
    return type;
  }

  public ArchivingTask getTask() {
    return task;
  }

  public void serialize(FileOutputStream logFileOutStream) throws IOException {
    int typeNum = type.ordinal();
    ReadWriteIOUtils.write((byte) typeNum, logFileOutStream);
    ReadWriteIOUtils.write(task.getTaskId(), logFileOutStream);

    if (type == ArchivingOperateType.SET) {
      ReadWriteIOUtils.write(task.getStorageGroup().getFullPath(), logFileOutStream);
      ReadWriteIOUtils.write(task.getTargetDir().getPath(), logFileOutStream);
      ReadWriteIOUtils.write(task.getStartTime(), logFileOutStream);
      ReadWriteIOUtils.write(task.getTTL(), logFileOutStream);
      ReadWriteIOUtils.write(task.getSubmitTime(), logFileOutStream);
    }

    logFileOutStream.flush();
  }

  public static ArchivingOperate deserialize(FileInputStream logFileInStream)
      throws IOException, IllegalPathException {
    ArchivingOperateType operateType;

    int typeNum = ReadWriteIOUtils.readByte(logFileInStream);
    if (typeNum >= 0 && typeNum < ArchivingOperateType.values().length)
      operateType = ArchivingOperateType.values()[typeNum];
    else throw new IOException();

    long taskId = ReadWriteIOUtils.readLong(logFileInStream);

    ArchivingTask deserializedTask;
    if (operateType == ArchivingOperateType.SET) {
      PartialPath storageGroup = new PartialPath(ReadWriteIOUtils.readString(logFileInStream));
      String targetDirPath = ReadWriteIOUtils.readString(logFileInStream);
      File targetDir = FSFactoryProducer.getFSFactory().getFile(targetDirPath);
      long startTime = ReadWriteIOUtils.readLong(logFileInStream);
      long ttl = ReadWriteIOUtils.readLong(logFileInStream);
      long submitTime = ReadWriteIOUtils.readLong(logFileInStream);

      deserializedTask =
          new ArchivingTask(taskId, storageGroup, targetDir, ttl, startTime, submitTime);
    } else {
      deserializedTask = new ArchivingTask(taskId);
    }
    return new ArchivingOperate(operateType, deserializedTask);
  }

  public enum ArchivingOperateType {
    SET,
    CANCEL,
    START,
    PAUSE,
    RESUME,
    FINISHED,
    ERROR
  }
}
