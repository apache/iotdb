/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at      http://www.apache.org/licenses/LICENSE-2.0  Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the License for the specific language governing permissions and limitations under the License.
 */

package org.apache.iotdb.cluster.utils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.tsfile.common.conf.TSFileConfig;

public class SerializeUtils {

  private SerializeUtils() {
    // util class
  }

  public static void serialize(String str, DataOutputStream dataOutputStream) {
    try {
      byte[] strBytes = str.getBytes(TSFileConfig.STRING_CHARSET);
      dataOutputStream.writeInt(strBytes.length);
      dataOutputStream.write(strBytes);
    } catch (IOException e) {
      // unreachable
    }
  }

  public static String deserializeString(ByteBuffer buffer) {
    int length = buffer.getInt();
    byte[] strBytes = new byte[length];
    buffer.get(strBytes);
    return new String(strBytes);
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
      dataOutputStream.writeInt(node.metaPort);
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
    node.setIp(new String(ipBytes));
    node.setMetaPort(buffer.getInt());
    node.setNodeIdentifier(buffer.getInt());
    node.setDataPort(buffer.getInt());
  }
}
