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
package org.apache.iotdb.db.sync.sender.pipe;

import org.apache.iotdb.db.sync.sender.pipe.Pipe.PipeStatus;

public abstract class PipeInfo {
  private String pipeName;
  private String pipeSinkName;
  private PipeStatus status;
  private long createTime;

  public PipeInfo(String pipeName, String pipeSinkName, long createTime) {
    this.pipeName = pipeName;
    this.pipeSinkName = pipeSinkName;
    this.createTime = createTime;
    this.status = PipeStatus.STOP;
  }

  public PipeInfo(String pipeName, String pipeSinkName, PipeStatus status, long createTime) {
    this.pipeName = pipeName;
    this.pipeSinkName = pipeSinkName;
    this.createTime = createTime;
    this.status = status;
  }

  public String getPipeName() {
    return pipeName;
  }

  public void setPipeName(String pipeName) {
    this.pipeName = pipeName;
  }

  public String getPipeSinkName() {
    return pipeSinkName;
  }

  public void setPipeSinkName(String pipeSinkName) {
    this.pipeSinkName = pipeSinkName;
  }

  public PipeStatus getStatus() {
    return status;
  }

  public void start() {
    this.status = PipeStatus.RUNNING;
  }

  public void stop() {
    this.status = PipeStatus.STOP;
  }

  public void drop() {
    this.status = PipeStatus.DROP;
  }

  public long getCreateTime() {
    return createTime;
  }

  public void setCreateTime(long createTime) {
    this.createTime = createTime;
  }
}
