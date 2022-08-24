/*
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
package org.apache.iotdb.db.engine.migration;

import org.apache.iotdb.db.metadata.path.PartialPath;

import java.io.File;

/** Data class for each Migration Task */
public class MigrationTask {
  private long taskId;
  private PartialPath storageGroup;
  private File targetDir;
  private long startTime;
  private long ttl;
  private volatile MigrationTaskStatus status = MigrationTaskStatus.READY;

  public MigrationTask(
      long taskId, PartialPath storageGroup, File targetDir, long ttl, long startTime) {
    this.taskId = taskId;
    this.storageGroup = storageGroup;
    this.targetDir = targetDir;
    this.ttl = ttl;
    this.startTime = startTime;
  }

  // getter and setter functions

  public long getTaskId() {
    return taskId;
  }

  public PartialPath getStorageGroup() {
    return storageGroup;
  }

  public File getTargetDir() {
    return targetDir;
  }

  public long getTTL() {
    return ttl;
  }

  public long getStartTime() {
    return startTime;
  }

  public MigrationTaskStatus getStatus() {
    return status;
  }

  public void setStatus(MigrationTaskStatus status) {
    this.status = status;
  }

  public enum MigrationTaskStatus {
    READY,
    RUNNING,
    UNSET,
    PAUSED,
    CANCELING,
    ERROR,
    FINISHED
  }
}
