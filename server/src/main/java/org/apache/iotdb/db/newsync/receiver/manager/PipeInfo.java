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
package org.apache.iotdb.db.newsync.receiver.manager;

import java.util.Objects;

public class PipeInfo {
  private String pipeName;
  private PipeStatus status;
  private String remoteIp;
  private long createTime;

  public PipeInfo(String pipeName, String remoteIp, PipeStatus status, long createTime) {
    this.pipeName = pipeName;
    this.remoteIp = remoteIp;
    this.status = status;
    this.createTime = createTime;
  }

  public String getPipeName() {
    return pipeName;
  }

  public void setPipeName(String pipeName) {
    this.pipeName = pipeName;
  }

  public PipeStatus getStatus() {
    return status;
  }

  public void setStatus(PipeStatus status) {
    this.status = status;
  }

  public long getCreateTime() {
    return createTime;
  }

  public void setCreateTime(long createTime) {
    this.createTime = createTime;
  }

  public String getRemoteIp() {
    return remoteIp;
  }

  public void setRemoteIp(String remoteIp) {
    this.remoteIp = remoteIp;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    PipeInfo pipeInfo = (PipeInfo) o;
    return createTime == pipeInfo.createTime
        && Objects.equals(pipeName, pipeInfo.pipeName)
        && status == pipeInfo.status
        && Objects.equals(remoteIp, pipeInfo.remoteIp);
  }

  @Override
  public int hashCode() {
    return Objects.hash(pipeName, status, remoteIp, createTime);
  }
}
