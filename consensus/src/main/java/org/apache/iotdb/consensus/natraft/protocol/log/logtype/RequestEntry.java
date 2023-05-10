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

package org.apache.iotdb.consensus.natraft.protocol.log.logtype;

import org.apache.iotdb.consensus.common.request.ByteBufferConsensusRequest;
import org.apache.iotdb.consensus.common.request.IConsensusRequest;
import org.apache.iotdb.consensus.natraft.protocol.log.Entry;
import org.apache.iotdb.tsfile.utils.PublicBAOS;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

import static org.apache.iotdb.consensus.natraft.protocol.log.Entry.Types.CLIENT_REQUEST;

/** RequestLog contains a non-partitioned request like set storage group. */
public class RequestEntry extends Entry {

  private static final Logger logger = LoggerFactory.getLogger(RequestEntry.class);
  private volatile IConsensusRequest request;

  public RequestEntry() {}

  public RequestEntry(IConsensusRequest request) {
    setRequest(request);
  }

  @Override
  protected ByteBuffer serializeInternal() {
    PublicBAOS byteArrayOutputStream = new PublicBAOS(getDefaultSerializationBufferSize());
    int requestSize = 0;
    int requestPos = 0;
    try (DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream)) {
      dataOutputStream.writeByte((byte) CLIENT_REQUEST.ordinal());

      dataOutputStream.writeLong(getCurrLogIndex());
      dataOutputStream.writeLong(getCurrLogTerm());
      dataOutputStream.writeLong(getPrevTerm());

      requestPos = byteArrayOutputStream.size();
      dataOutputStream.writeInt(0);
      request.serializeTo(dataOutputStream);
      requestSize = byteArrayOutputStream.size() - requestPos - 4;
    } catch (IOException e) {
      logger.error("Unexpected IOException when serializing {}", this);
    }

    ByteBuffer wrap =
        ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size());
    wrap.position(requestPos);
    wrap.putInt(requestSize);
    wrap.position(0);
    return wrap;
  }

  @Override
  public void deserialize(ByteBuffer buffer) {
    setCurrLogIndex(buffer.getLong());
    setCurrLogTerm(buffer.getLong());
    setPrevTerm(buffer.getLong());
    int len = buffer.getInt();
    byte[] bytes = new byte[len];
    buffer.get(bytes);

    request = new ByteBufferConsensusRequest(ByteBuffer.wrap(bytes));
  }

  public IConsensusRequest getRequest() {
    return request;
  }

  public void setRequest(IConsensusRequest request) {
    this.request = request;
  }

  @Override
  public String toString() {
    return request
        + ",term:"
        + getCurrLogTerm()
        + ",index:"
        + getCurrLogIndex()
        + ",size:"
        + cachedSize();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    RequestEntry that = (RequestEntry) o;
    return Objects.equals(request, that.request);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), request);
  }

  @Override
  public long estimateSize() {
    return request.estimateSize();
  }
}
