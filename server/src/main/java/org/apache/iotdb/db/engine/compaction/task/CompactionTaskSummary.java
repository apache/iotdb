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
package org.apache.iotdb.db.engine.compaction.task;

/** The summary of one {@link AbstractCompactionTask} execution */
public class CompactionTaskSummary {
  private long timeCost = 0L;
  private volatile Status status = Status.NOT_STARTED;
  private long startTime = -1L;

  public CompactionTaskSummary() {}

  public void start() {
    this.status = Status.STARTED;
    this.startTime = System.currentTimeMillis();
  }

  public void finish(boolean success) {
    this.status = success ? Status.SUCCESS : Status.FAILED;
    this.timeCost = System.currentTimeMillis() - this.startTime;
  }

  public void cancel() {
    if (this.status != Status.SUCCESS && this.status != Status.FAILED) {
      this.status = Status.CANCELED;
      if (this.startTime != -1) {
        this.timeCost = System.currentTimeMillis() - this.startTime;
      }
    }
  }

  public boolean isCancel() {
    return this.status == Status.CANCELED;
  }

  public boolean isFinished() {
    return this.status == Status.SUCCESS || this.status == Status.FAILED;
  }

  public boolean isRan() {
    return this.status != Status.NOT_STARTED;
  }

  public boolean isSuccess() {
    return this.status == Status.SUCCESS;
  }

  public long getTimeCost() {
    return timeCost;
  }

  enum Status {
    NOT_STARTED,
    STARTED,
    SUCCESS,
    FAILED,
    CANCELED
  }
}
