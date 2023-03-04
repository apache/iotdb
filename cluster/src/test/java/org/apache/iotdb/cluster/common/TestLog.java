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

package org.apache.iotdb.cluster.common;

import org.apache.iotdb.cluster.log.Log;

import java.nio.ByteBuffer;
import java.util.Objects;

public class TestLog extends Log {

  @Override
  public ByteBuffer serialize() {
    int totalSize = Long.BYTES * 2;
    byte[] buffer = new byte[totalSize];

    ByteBuffer byteBuffer = ByteBuffer.wrap(buffer);

    byteBuffer.putLong(getCurrLogIndex());
    byteBuffer.putLong(getCurrLogTerm());

    byteBuffer.flip();
    return byteBuffer;
  }

  @Override
  public void deserialize(ByteBuffer buffer) {
    setCurrLogIndex(buffer.getLong());
    setCurrLogTerm(buffer.getLong());
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof TestLog)) {
      return false;
    }
    TestLog obj1 = (TestLog) obj;
    return getCurrLogIndex() == obj1.getCurrLogIndex() && getCurrLogTerm() == obj1.getCurrLogTerm();
  }

  @Override
  public int hashCode() {
    return Objects.hash(getCurrLogIndex(), getCurrLogTerm());
  }

  @Override
  public String toString() {
    return "TestLog{" + getCurrLogIndex() + "-" + getCurrLogTerm() + "}";
  }
}
