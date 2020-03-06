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

import java.nio.ByteBuffer;
import java.util.Objects;
import org.apache.iotdb.cluster.log.Log;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.utils.SerializeUtils;

/**
 * AddNodeLog records the operation of adding a node into this cluster.
 */
public class AddNodeLog extends Log {

  private Node newNode;

  public Node getNewNode() {
    return newNode;
  }

  public void setNewNode(Node newNode) {
    this.newNode = newNode;
  }

  public AddNodeLog(){
    logType = LogType.ADD_NODE;
  }


  @Override
  public ByteBuffer serialize() {
    byte[] ipBytes = newNode.getIp().getBytes();

    // marker(byte), previous index(long), previous term(long), curr index(long), curr term(long)
    // ipLength(int), inBytes(byte[]), port(int), identifier(int), dataPort(int)
    int totalSize =
              Byte.BYTES  + Long.BYTES + Long.BYTES + Long.BYTES + Long.BYTES +
              Integer.BYTES + ipBytes.length + Integer.BYTES + Integer.BYTES + Integer.BYTES;
    byte[] buffer = new byte[totalSize];

    ByteBuffer byteBuffer = ByteBuffer.wrap(buffer);

    byteBuffer.put((byte) Types.ADD_NODE.ordinal());

    byteBuffer.putLong(getPreviousLogIndex());
    byteBuffer.putLong(getPreviousLogTerm());
    byteBuffer.putLong(getCurrLogIndex());
    byteBuffer.putLong(getCurrLogTerm());

    byteBuffer.putInt(ipBytes.length);
    byteBuffer.put(ipBytes);
    byteBuffer.putInt(newNode.getMetaPort());
    byteBuffer.putInt(newNode.getNodeIdentifier());
    byteBuffer.putInt(newNode.getDataPort());

    byteBuffer.flip();
    return byteBuffer;
  }

  @Override
  public void deserialize(ByteBuffer buffer) {

    // marker is previously read, remaining fields:
    // previous index(long), previous term(long), curr index(long), curr term(long)
    // ipLength(int), inBytes(byte[]), port(int), identifier(int), dataPort(int)
    setPreviousLogIndex(buffer.getLong());
    setPreviousLogTerm(buffer.getLong());
    setCurrLogIndex(buffer.getLong());
    setCurrLogTerm(buffer.getLong());

    newNode = new Node();
    SerializeUtils.deserialize(newNode, buffer);
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
    AddNodeLog that = (AddNodeLog) o;
    return Objects.equals(newNode, that.newNode);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), newNode);
  }
}
