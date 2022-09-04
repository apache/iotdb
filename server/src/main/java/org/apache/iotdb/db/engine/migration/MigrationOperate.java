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
package org.apache.iotdb.db.engine.migration;

import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

/** MigrationOperate is MigrationOperateType (SET, CANCEL, PAUSE, etc) and a MigrationTask */
public class MigrationOperate {
  private MigrationOperateType type;
  private MigrationTask task;

  public MigrationOperate(MigrationOperateType type, MigrationTask task) {
    this.type = type;
    this.task = new MigrationTask(task);
  }

  public MigrationOperate(MigrationOperateType type, long taskId) {
    this.type = type;
    this.task = new MigrationTask(taskId);
  }

  public MigrationOperateType getType() {
    return type;
  }

  public MigrationTask getTask() {
    return task;
  }

  public void serialize(FileOutputStream logFileOutStream) throws IOException {
    int typeNum = type.ordinal();
    ReadWriteIOUtils.write((byte) typeNum, logFileOutStream);
    ReadWriteIOUtils.write(task.getTaskId(), logFileOutStream);

    if (type == MigrationOperateType.SET) {
      ReadWriteIOUtils.write(task.getStorageGroup().getFullPath(), logFileOutStream);
      ReadWriteIOUtils.write(task.getTargetDir().getPath(), logFileOutStream);
      ReadWriteIOUtils.write(task.getStartTime(), logFileOutStream);
      ReadWriteIOUtils.write(task.getTTL(), logFileOutStream);
      ReadWriteIOUtils.write(task.getSubmitTime(), logFileOutStream);
    }

    logFileOutStream.flush();
  }

  public static MigrationOperate deserialize(FileInputStream logFileInStream)
      throws IOException, IllegalPathException {
    MigrationOperateType operateType;

    int typeNum = ReadWriteIOUtils.readByte(logFileInStream);
    if (typeNum >= 0 && typeNum < MigrationOperateType.values().length)
      operateType = MigrationOperateType.values()[typeNum];
    else throw new IOException();

    long taskId = ReadWriteIOUtils.readLong(logFileInStream);

    MigrationTask deserializedTask;
    if (operateType == MigrationOperateType.SET) {
      PartialPath storageGroup = new PartialPath(ReadWriteIOUtils.readString(logFileInStream));
      String targetDirPath = ReadWriteIOUtils.readString(logFileInStream);
      File targetDir = FSFactoryProducer.getFSFactory().getFile(targetDirPath);
      long startTime = ReadWriteIOUtils.readLong(logFileInStream);
      long ttl = ReadWriteIOUtils.readLong(logFileInStream);
      long submitTime = ReadWriteIOUtils.readLong(logFileInStream);

      deserializedTask =
          new MigrationTask(taskId, storageGroup, targetDir, ttl, startTime, submitTime);
    } else {
      deserializedTask = new MigrationTask(taskId);
    }
    return new MigrationOperate(operateType, deserializedTask);
  }

  public enum MigrationOperateType {
    SET,
    CANCEL,
    START,
    PAUSE,
    RESUME,
    FINISHED,
    ERROR
  }
}
