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

package org.apache.iotdb.db.metadata.mnode;

import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public enum MNodeType {
  ROOT((byte) 0, "ROOT"),
  SG_INTERNAL((byte) 1, "SG INTERNAL"),
  STORAGE_GROUP((byte) 2, "DATABASE"),
  INTERNAL((byte) 3, "INTERNAL"),
  DEVICE((byte) 4, "DEVICE"),
  MEASUREMENT((byte) 5, "TIMESERIES"),
  UNIMPLEMENT((byte) 6, "");

  private final byte nodeType;

  private final String typeName;

  MNodeType(byte nodeType, String typeName) {
    this.nodeType = nodeType;
    this.typeName = typeName;
  }

  public static MNodeType getMNodeType(byte type) {
    switch (type) {
      case 0:
        return MNodeType.ROOT;
      case 1:
        return MNodeType.SG_INTERNAL;
      case 2:
        return MNodeType.STORAGE_GROUP;
      case 3:
        return MNodeType.INTERNAL;
      case 4:
        return MNodeType.DEVICE;
      case 5:
        return MNodeType.MEASUREMENT;
      case 6:
        return MNodeType.UNIMPLEMENT;
      default:
        throw new IllegalArgumentException("Invalid input: " + type);
    }
  }

  public void serialize(ByteBuffer buffer) {
    ReadWriteIOUtils.write(nodeType, buffer);
  }

  public void serialize(DataOutputStream stream) throws IOException {
    ReadWriteIOUtils.write(nodeType, stream);
  }

  public byte getNodeType() {
    return nodeType;
  }

  public String getNodeTypeName() {
    return typeName;
  }
}
