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
package org.apache.iotdb.confignode.consensus.request.write;

import org.apache.iotdb.commons.utils.BasicStructureSerDeUtil;
import org.apache.iotdb.confignode.consensus.request.ConfigRequest;
import org.apache.iotdb.confignode.consensus.request.ConfigRequestType;
import org.apache.iotdb.confignode.rpc.thrift.TOperateReceiverPipeReq;
import org.apache.iotdb.service.transport.thrift.RequestType;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

public class OperateReceiverPipeReq extends ConfigRequest {
  private RequestType operateType;
  private String pipeName;
  private String remoteIp;
  private long createTime;

  public OperateReceiverPipeReq() {
    super(ConfigRequestType.OperatePipe);
  }

  public OperateReceiverPipeReq(TOperateReceiverPipeReq req) {
    this();
    this.operateType = req.getType();
    this.pipeName = req.getPipeName();
    this.remoteIp = req.getRemoteIp();
    this.createTime = req.getCreateTime();
  }

  public RequestType getOperateType() {
    return operateType;
  }

  public void setOperateType(RequestType operateType) {
    this.operateType = operateType;
  }

  public String getPipeName() {
    return pipeName;
  }

  public void setPipeName(String pipeName) {
    this.pipeName = pipeName;
  }

  public String getRemoteIp() {
    return remoteIp;
  }

  public void setRemoteIp(String remoteIp) {
    this.remoteIp = remoteIp;
  }

  public long getCreateTime() {
    return createTime;
  }

  public void setCreateTime(long createTime) {
    this.createTime = createTime;
  }

  @Override
  protected void serializeImpl(ByteBuffer buffer) {
    buffer.putInt(getType().ordinal());
    BasicStructureSerDeUtil.write(pipeName, buffer);
    BasicStructureSerDeUtil.write(remoteIp, buffer);
    buffer.putInt(operateType.getValue());
    buffer.putLong(createTime);
  }

  @Override
  protected void deserializeImpl(ByteBuffer buffer) throws IOException {
    pipeName = BasicStructureSerDeUtil.readString(buffer);
    remoteIp = BasicStructureSerDeUtil.readString(buffer);
    operateType = RequestType.findByValue(buffer.getInt());
    createTime = buffer.getLong();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    OperateReceiverPipeReq that = (OperateReceiverPipeReq) o;
    return createTime == that.createTime
        && operateType == that.operateType
        && Objects.equals(pipeName, that.pipeName)
        && Objects.equals(remoteIp, that.remoteIp);
  }

  @Override
  public int hashCode() {
    return Objects.hash(operateType, pipeName, remoteIp, createTime);
  }
}
