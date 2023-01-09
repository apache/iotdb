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

import org.apache.iotdb.commons.exception.runtime.SerializationRunTimeException;
import org.apache.iotdb.confignode.rpc.thrift.TShowPipeInfo;
import org.apache.iotdb.tsfile.utils.PublicBAOS;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

public abstract class PipeInfo {
  private static final Logger LOGGER = LoggerFactory.getLogger(PipeInfo.class);

  protected String pipeName;
  protected String pipeSinkName;
  protected PipeStatus status;
  protected long createTime;
  protected PipeMessage.PipeMessageType messageType;

  // only used for serialization
  protected PipeInfo() {}

  protected PipeInfo(String pipeName, String pipeSinkName, long createTime) {
    this.pipeName = pipeName;
    this.pipeSinkName = pipeSinkName;
    this.createTime = createTime;
    this.status = PipeStatus.STOP;
    this.messageType = PipeMessage.PipeMessageType.NORMAL;
  }

  protected PipeInfo(String pipeName, String pipeSinkName, PipeStatus status, long createTime) {
    this.pipeName = pipeName;
    this.pipeSinkName = pipeSinkName;
    this.createTime = createTime;
    this.status = status;
    this.messageType = PipeMessage.PipeMessageType.NORMAL;
  }

  abstract PipeType getType();

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

  public void setStatus(PipeStatus status) {
    this.status = status;
  }

  public PipeMessage.PipeMessageType getMessageType() {
    return messageType;
  }

  public void setMessageType(PipeMessage.PipeMessageType messageType) {
    this.messageType = messageType;
  }

  public long getCreateTime() {
    return createTime;
  }

  public void setCreateTime(long createTime) {
    this.createTime = createTime;
  }

  public abstract TShowPipeInfo getTShowPipeInfo();

  public void serialize(OutputStream outputStream) throws IOException {
    ReadWriteIOUtils.write((byte) getType().ordinal(), outputStream);
    ReadWriteIOUtils.write(pipeName, outputStream);
    ReadWriteIOUtils.write(pipeSinkName, outputStream);
    ReadWriteIOUtils.write(status.getType(), outputStream);
    ReadWriteIOUtils.write(createTime, outputStream);
    ReadWriteIOUtils.write((byte) messageType.ordinal(), outputStream);
  }

  protected void deserialize(InputStream inputStream) throws IOException {
    pipeName = ReadWriteIOUtils.readString(inputStream);
    pipeSinkName = ReadWriteIOUtils.readString(inputStream);
    status = PipeStatus.getPipeStatus(ReadWriteIOUtils.readByte(inputStream));
    createTime = ReadWriteIOUtils.readLong(inputStream);
    messageType = PipeMessage.PipeMessageType.values()[ReadWriteIOUtils.readByte(inputStream)];
  }

  protected void deserialize(ByteBuffer byteBuffer) {
    pipeName = ReadWriteIOUtils.readString(byteBuffer);
    pipeSinkName = ReadWriteIOUtils.readString(byteBuffer);
    status = PipeStatus.getPipeStatus(ReadWriteIOUtils.readByte(byteBuffer));
    createTime = ReadWriteIOUtils.readLong(byteBuffer);
    messageType = PipeMessage.PipeMessageType.values()[ReadWriteIOUtils.readByte(byteBuffer)];
  }

  public ByteBuffer serializeToByteBuffer() {
    try (PublicBAOS publicBAOS = new PublicBAOS();
        DataOutputStream dataOutputStream = new DataOutputStream(publicBAOS)) {
      serialize(dataOutputStream);
      return ByteBuffer.wrap(publicBAOS.getBuf(), 0, publicBAOS.size());
    } catch (IOException e) {
      LOGGER.error("Unexpected error occurred when serializing PipeInfo.");
      throw new SerializationRunTimeException(e);
    }
  }

  public static PipeInfo deserializePipeInfo(InputStream inputStream) throws IOException {
    PipeType pipeType = PipeType.values()[ReadWriteIOUtils.readByte(inputStream)];
    PipeInfo pipeInfo;
    switch (pipeType) {
      case TsFilePipe:
        pipeInfo = new TsFilePipeInfo();
        pipeInfo.deserialize(inputStream);
        break;
      case WALPipe:
      default:
        throw new UnsupportedOperationException(
            String.format("Can not recognize PipeType %s.", pipeType.name()));
    }
    return pipeInfo;
  }

  public static PipeInfo deserializePipeInfo(ByteBuffer byteBuffer) {
    PipeType pipeType = PipeType.values()[ReadWriteIOUtils.readByte(byteBuffer)];
    PipeInfo pipeInfo;
    switch (pipeType) {
      case TsFilePipe:
        pipeInfo = new TsFilePipeInfo();
        pipeInfo.deserialize(byteBuffer);
        break;
      case WALPipe:
      default:
        throw new UnsupportedOperationException(
            String.format("Can not recognize PipeType %s.", pipeType.name()));
    }
    return pipeInfo;
  }

  enum PipeType {
    TsFilePipe,
    WALPipe
  }
}
