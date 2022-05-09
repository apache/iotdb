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

package org.apache.iotdb.cluster.utils;

import org.apache.iotdb.cluster.rpc.thrift.Node;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class NodeSerializeUtils {

  private NodeSerializeUtils() {}

  public static void serialize(Node node, DataOutputStream dataOutputStream) {
    try {
      byte[] internalIpBytes = node.internalIp.getBytes();
      dataOutputStream.writeInt(internalIpBytes.length);
      dataOutputStream.write(internalIpBytes);
      dataOutputStream.writeInt(node.metaPort);
      dataOutputStream.writeInt(node.nodeIdentifier);
      dataOutputStream.writeInt(node.dataPort);
      dataOutputStream.writeInt(node.clientPort);
      byte[] clientIpBytes = node.clientIp.getBytes();
      dataOutputStream.writeInt(clientIpBytes.length);
      dataOutputStream.write(clientIpBytes);
    } catch (IOException e) {
      // unreachable
    }
  }

  public static void deserialize(Node node, ByteBuffer buffer) {
    int internalIpLength = buffer.getInt();
    byte[] internalIpBytes = new byte[internalIpLength];
    buffer.get(internalIpBytes);
    node.setInternalIp(new String(internalIpBytes));
    node.setMetaPort(buffer.getInt());
    node.setNodeIdentifier(buffer.getInt());
    node.setDataPort(buffer.getInt());
    node.setClientPort(buffer.getInt());
    int clientIpLength = buffer.getInt();
    byte[] clientIpBytes = new byte[clientIpLength];
    buffer.get(clientIpBytes);
    node.setClientIp(new String(clientIpBytes));
  }

  public static void deserialize(Node node, DataInputStream stream) throws IOException {
    int ipLength = stream.readInt();
    byte[] ipBytes = new byte[ipLength];
    int readIpSize = stream.read(ipBytes);
    if (readIpSize != ipLength) {
      throw new IOException(
          String.format(
              "No sufficient bytes read when deserializing the ip of a node: %d/%d",
              readIpSize, ipLength));
    }
    node.setInternalIp(new String(ipBytes));
    node.setMetaPort(stream.readInt());
    node.setNodeIdentifier(stream.readInt());
    node.setDataPort(stream.readInt());
    node.setClientPort(stream.readInt());

    int clientIpLength = stream.readInt();
    byte[] clientIpBytes = new byte[clientIpLength];
    int readClientIpSize = stream.read(clientIpBytes);
    if (readClientIpSize != clientIpLength) {
      throw new IOException(
          String.format(
              "No sufficient bytes read when deserializing the clientIp of a node: %d/%d",
              readClientIpSize, clientIpLength));
    }
    node.setClientIp(new String(clientIpBytes));
  }
}
