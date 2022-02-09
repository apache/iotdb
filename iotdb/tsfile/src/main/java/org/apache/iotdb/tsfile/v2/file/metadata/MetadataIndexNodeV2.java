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
package org.apache.iotdb.tsfile.v2.file.metadata;

import org.apache.iotdb.tsfile.file.metadata.MetadataIndexEntry;
import org.apache.iotdb.tsfile.file.metadata.MetadataIndexNode;
import org.apache.iotdb.tsfile.file.metadata.enums.MetadataIndexNodeType;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class MetadataIndexNodeV2 {

  private MetadataIndexNodeV2() {}

  public static MetadataIndexNode deserializeFrom(ByteBuffer buffer) {
    List<MetadataIndexEntry> children = new ArrayList<>();
    int size = ReadWriteIOUtils.readInt(buffer);
    for (int i = 0; i < size; i++) {
      children.add(MetadataIndexEntryV2.deserializeFrom(buffer));
    }
    long offset = ReadWriteIOUtils.readLong(buffer);
    MetadataIndexNodeType nodeType =
        MetadataIndexNodeType.deserialize(ReadWriteIOUtils.readByte(buffer));
    return new MetadataIndexNode(children, offset, nodeType);
  }
}
