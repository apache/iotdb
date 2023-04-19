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
package org.apache.iotdb.commons.sync.pipe;

import org.apache.iotdb.confignode.rpc.thrift.TShowPipeInfo;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Objects;

public class TsFilePipeInfo extends PipeInfo {
  private boolean syncDelOp;
  private long dataStartTimestamp;

  // only used for serialization
  public TsFilePipeInfo() {}

  public TsFilePipeInfo(
      String pipeName,
      String pipeSinkName,
      long createTime,
      long dataStartTimestamp,
      boolean syncDelOp) {
    super(pipeName, pipeSinkName, createTime);
    this.dataStartTimestamp = dataStartTimestamp;
    this.syncDelOp = syncDelOp;
  }

  public TsFilePipeInfo(
      String pipeName,
      String pipeSinkName,
      PipeStatus status,
      long createTime,
      long dataStartTimestamp,
      boolean syncDelOp) {
    super(pipeName, pipeSinkName, status, createTime);
    this.dataStartTimestamp = dataStartTimestamp;
    this.syncDelOp = syncDelOp;
  }

  public boolean isSyncDelOp() {
    return syncDelOp;
  }

  public void setSyncDelOp(boolean syncDelOp) {
    this.syncDelOp = syncDelOp;
  }

  public long getDataStartTimestamp() {
    return dataStartTimestamp;
  }

  public void setDataStartTimestamp(long dataStartTimestamp) {
    this.dataStartTimestamp = dataStartTimestamp;
  }

  @Override
  PipeType getType() {
    return PipeType.TsFilePipe;
  }

  @Override
  public TShowPipeInfo getTShowPipeInfo() {
    return new TShowPipeInfo();
  }

  @Override
  public void serialize(OutputStream outputStream) throws IOException {
    super.serialize(outputStream);
    ReadWriteIOUtils.write(syncDelOp, outputStream);
    ReadWriteIOUtils.write(dataStartTimestamp, outputStream);
  }

  @Override
  protected void deserialize(InputStream inputStream) throws IOException {
    super.deserialize(inputStream);
    syncDelOp = ReadWriteIOUtils.readBool(inputStream);
    dataStartTimestamp = ReadWriteIOUtils.readLong(inputStream);
  }

  @Override
  protected void deserialize(ByteBuffer buffer) {
    super.deserialize(buffer);
    syncDelOp = ReadWriteIOUtils.readBool(buffer);
    dataStartTimestamp = ReadWriteIOUtils.readLong(buffer);
  }

  @Override
  public String toString() {
    return "TsFilePipeInfo{"
        + "pipeName='"
        + pipeName
        + '\''
        + ", pipeSinkName='"
        + pipeSinkName
        + '\''
        + ", status="
        + status
        + ", createTime="
        + createTime
        + ", syncDelOp="
        + syncDelOp
        + ", dataStartTimestamp="
        + dataStartTimestamp
        + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    TsFilePipeInfo that = (TsFilePipeInfo) o;
    return syncDelOp == that.syncDelOp
        && dataStartTimestamp == that.dataStartTimestamp
        && createTime == that.createTime
        && Objects.equals(pipeName, that.pipeName)
        && Objects.equals(pipeSinkName, that.pipeSinkName)
        && status == that.status;
  }

  @Override
  public int hashCode() {
    return Objects.hash(syncDelOp, dataStartTimestamp, pipeName, pipeSinkName, status, createTime);
  }
}
