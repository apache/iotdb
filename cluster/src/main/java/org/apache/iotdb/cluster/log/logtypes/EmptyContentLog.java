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

package org.apache.iotdb.cluster.log.logtypes;

import org.apache.iotdb.cluster.log.Log;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import static org.apache.iotdb.cluster.log.Log.Types.EMPTY_CONTENT;

public class EmptyContentLog extends Log {

  public EmptyContentLog() {}

  public EmptyContentLog(long index, long term) {
    this.setCurrLogIndex(index);
    this.setCurrLogTerm(term);
  }

  @Override
  public ByteBuffer serialize() {
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream(DEFAULT_BUFFER_SIZE);
    try (DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream)) {
      dataOutputStream.writeByte((byte) EMPTY_CONTENT.ordinal());
      dataOutputStream.writeLong(getCurrLogIndex());
      dataOutputStream.writeLong(getCurrLogTerm());
    } catch (IOException e) {
      // unreachable
    }
    return ByteBuffer.wrap(byteArrayOutputStream.toByteArray());
  }

  @Override
  public void deserialize(ByteBuffer buffer) {
    setCurrLogIndex(buffer.getLong());
    setCurrLogTerm(buffer.getLong());
  }

  @Override
  public String toString() {
    return "term:" + getCurrLogTerm() + ",index:" + getCurrLogIndex();
  }
}
