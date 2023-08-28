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

import org.apache.iotdb.consensus.natraft.protocol.log.Entry;
import org.apache.iotdb.tsfile.utils.PublicBAOS;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class EmptyEntry extends Entry {

  public EmptyEntry() {}

  public EmptyEntry(long index, long term) {
    this.setCurrLogIndex(index);
    this.setCurrLogTerm(term);
  }

  @Override
  protected ByteBuffer serializeInternal(byte[] buffer) {
    ByteArrayOutputStream byteArrayOutputStream =
        buffer == null
            ? new PublicBAOS(getDefaultSerializationBufferSize())
            : new PublicBAOS(buffer);
    try (DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream)) {
      dataOutputStream.writeByte((byte) Types.EMPTY.ordinal());
      dataOutputStream.writeLong(getCurrLogIndex());
      dataOutputStream.writeLong(getCurrLogTerm());
      dataOutputStream.writeLong(getPrevTerm());
    } catch (IOException e) {
      // unreachable
    }
    return ByteBuffer.wrap(byteArrayOutputStream.toByteArray());
  }

  @Override
  public void deserialize(ByteBuffer buffer) {
    setCurrLogIndex(buffer.getLong());
    setCurrLogTerm(buffer.getLong());
    setPrevTerm(buffer.getLong());
  }

  @Override
  public long estimateSize() {
    return 0;
  }

  @Override
  public String toString() {
    return "term:" + getCurrLogTerm() + ",index:" + getCurrLogIndex();
  }
}
