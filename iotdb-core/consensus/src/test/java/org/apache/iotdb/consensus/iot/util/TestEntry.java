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

package org.apache.iotdb.consensus.iot.util;

import org.apache.iotdb.consensus.common.Peer;
import org.apache.iotdb.consensus.common.request.IoTConsensusRequest;
import org.apache.iotdb.tsfile.utils.PublicBAOS;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

public class TestEntry extends IoTConsensusRequest {

  private final int num;
  private final Peer peer;

  public TestEntry(int num, Peer peer) {
    super(ByteBuffer.allocate(Integer.BYTES));
    this.num = num;
    this.peer = peer;
    ByteBuffer buffer = super.serializeToByteBuffer();
    buffer.putInt(num);
    buffer.clear();
  }

  @Override
  public ByteBuffer serializeToByteBuffer() {
    try (PublicBAOS publicBAOS = new PublicBAOS();
        DataOutputStream outputStream = new DataOutputStream(publicBAOS)) {
      outputStream.writeInt(num);
      peer.serialize(outputStream);
      return ByteBuffer.wrap(publicBAOS.getBuf(), 0, publicBAOS.size());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    TestEntry testEntry = (TestEntry) o;
    return num == testEntry.num && Objects.equals(peer, testEntry.peer);
  }

  @Override
  public int hashCode() {
    return Objects.hash(num, peer);
  }

  @Override
  public String toString() {
    return "TestEntry{" + "num=" + num + ", peer=" + peer + '}';
  }
}
