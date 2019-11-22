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

package org.apache.iotdb.cluster.log.meta;

import java.nio.ByteBuffer;
import org.apache.iotdb.cluster.log.Log;

/**
 * AddNodeLog records the operation of adding a node into this cluster.
 */
public class AddNodeLog extends Log {
  private String ip;
  private int port;
  private int nodeIdentifier;

  public String getIp() {
    return ip;
  }

  public void setIp(String ip) {
    this.ip = ip;
  }

  public int getPort() {
    return port;
  }

  public void setPort(int port) {
    this.port = port;
  }

  public int getNodeIdentifier() {
    return nodeIdentifier;
  }

  public void setNodeIdentifier(int nodeIdentifier) {
    this.nodeIdentifier = nodeIdentifier;
  }

  @Override
  public ByteBuffer serialize() {
    byte[] ipBytes = ip.getBytes();

    // marker(byte), previous index(long), previous term(long), curr index(long), curr term(long)
    // ipLength(int), inBytes(byte[]), port(int), identifier(int)
    int totalSize =
              Byte.BYTES  + Long.BYTES + Long.BYTES + Long.BYTES + Long.BYTES +
              Integer.BYTES + ipBytes.length + Integer.BYTES + Integer.BYTES;
    byte[] buffer = new byte[totalSize];

    ByteBuffer byteBuffer = ByteBuffer.wrap(buffer);

    byteBuffer.put((byte) Types.ADD_NODE.ordinal());

    byteBuffer.putLong(getPreviousLogIndex());
    byteBuffer.putLong(getPreviousLogTerm());
    byteBuffer.putLong(getCurrLogIndex());
    byteBuffer.putLong(getCurrLogTerm());

    byteBuffer.putInt(ipBytes.length);
    byteBuffer.put(ipBytes);
    byteBuffer.putInt(port);
    byteBuffer.putInt(nodeIdentifier);

    byteBuffer.flip();
    return byteBuffer;
  }

  @Override
  public void deserialize(ByteBuffer buffer) {

    // marker is previously read
    // previous index(long), previous term(long), curr index(long), curr term(long)
    // ipLength(int), inBytes(byte[]), port(int), identifier(int)
    setPreviousLogIndex(buffer.getLong());
    setPreviousLogTerm(buffer.getLong());
    setCurrLogIndex(buffer.getLong());
    setCurrLogTerm(buffer.getLong());

    int ipLength = buffer.getInt();
    byte[] ipBytes = new byte[ipLength];
    buffer.get(ipBytes);
    ip = new String(ipBytes);
    port = buffer.getInt();
    nodeIdentifier = buffer.getInt();
  }

  @Override
  public int calculateSocket() {
    return 0;
  }
}
