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

package org.apache.iotdb.tsfile.file.metadata;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import org.apache.iotdb.tsfile.file.metadata.enums.MetadataIndexNodeType;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

public class MetadataIndexNode {

  private String name;
  private long offset;
  private MetadataIndexNodeType metadataIndexNodeType;

  public MetadataIndexNode() {
  }

  public MetadataIndexNode(String name, long offset,
      MetadataIndexNodeType metadataIndexNodeType) {
    this.name = name;
    this.offset = offset;
    this.metadataIndexNodeType = metadataIndexNodeType;
  }

  public String getName() {
    return name;
  }

  public long getOffset() {
    return offset;
  }

  public MetadataIndexNodeType getMetadataIndexNodeType() {
    return metadataIndexNodeType;
  }

  public void setName(String name) {
    this.name = name;
  }

  public void setOffset(long offset) {
    this.offset = offset;
  }

  public void setMetadataIndexNodeType(
      MetadataIndexNodeType metadataIndexNodeType) {
    this.metadataIndexNodeType = metadataIndexNodeType;
  }

  public String toString() {
    return "<" + name + "," + offset + "," + metadataIndexNodeType + ">";
  }

  public int serializeTo(OutputStream outputStream) throws IOException {
    int byteLen = 0;
    byteLen += ReadWriteIOUtils.write(name, outputStream);
    byteLen += ReadWriteIOUtils.write(offset, outputStream);
    byteLen += ReadWriteIOUtils.write(metadataIndexNodeType.serialize(), outputStream);
    return byteLen;
  }

  public static MetadataIndexNode deserializeFrom(ByteBuffer buffer) {
    MetadataIndexNode metadataIndex = new MetadataIndexNode();
    metadataIndex.setName(ReadWriteIOUtils.readString(buffer));
    metadataIndex.setOffset(ReadWriteIOUtils.readLong(buffer));
    metadataIndex.setMetadataIndexNodeType(
        MetadataIndexNodeType.deserialize(ReadWriteIOUtils.readShort(buffer)));
    return metadataIndex;
  }
}
