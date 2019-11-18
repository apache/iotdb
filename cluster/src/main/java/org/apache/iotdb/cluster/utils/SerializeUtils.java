/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at      http://www.apache.org/licenses/LICENSE-2.0  Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the License for the specific language governing permissions and limitations under the License.
 */

package org.apache.iotdb.cluster.utils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import org.apache.iotdb.cluster.rpc.thrift.Node;

public class SerializeUtils {

  private SerializeUtils() {
    // util class
  }

  public static void serialize(List<Integer> ints, DataOutputStream dataOutputStream) {
    try {
      dataOutputStream.writeInt(ints.size());
      for (Integer anInt : ints) {
        dataOutputStream.writeInt(anInt);
      }
    } catch (IOException e) {
      // unreachable
    }
  }

  public static void deserialize(List<Integer> ints, ByteBuffer buffer) {
    int length = buffer.getInt();
    for (int i = 0; i < length; i++) {
      ints.add(buffer.getInt());
    }
  }

  public static void serialize(Node node, DataOutputStream dataOutputStream) {
    try {
      byte[] ipBytes = node.ip.getBytes();
      dataOutputStream.writeInt(ipBytes.length);
      dataOutputStream.write(ipBytes);
      dataOutputStream.writeInt(node.port);
      dataOutputStream.writeInt(node.nodeIdentifier);
      dataOutputStream.writeInt(node.dataPort);
    } catch (IOException e) {
      // unreachable
    }
  }

  public static void deserialize(Node node, ByteBuffer buffer) {
    int ipLength = buffer.getInt();
    byte[] ipBytes = new byte[ipLength];
    buffer.get(ipBytes);
    node.ip = new String(ipBytes);
    node.port = buffer.getInt();
    node.nodeIdentifier = buffer.getInt();
    node.dataPort = buffer.getInt();
  }
}
