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
  private volatile boolean ran = false;
  private volatile boolean finished = false;
  private volatile boolean cancel = false;
  private volatile boolean success = false;

  public CompactionTaskSummary(boolean success) {
    this.success = success;
  }

  public CompactionTaskSummary() {}

  public void start() {
    this.ran = true;
  }

  public void finish(boolean success, long timeCost) {
    this.finished = true;
    this.success = success;
    this.timeCost = timeCost;
  }

  public void cancel() {
    this.cancel = true;
  }

  public boolean isCancel() {
    return cancel;
  }

  public boolean isFinished() {
    return finished;
  }

  public boolean isRan() {
    return ran;
  }

  public boolean isSuccess() {
    return success;
  }

  public long getTimeCost() {
    return timeCost;
  }
}
