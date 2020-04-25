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

package org.apache.iotdb.tsfile.utils;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import org.apache.iotdb.tsfile.file.metadata.enums.ChildMetadataIndexType;

public class MetadataIndex {

  private String name;
  private long offset;
  private ChildMetadataIndexType childMetadataIndexType;

  public MetadataIndex() {
  }

  public MetadataIndex(String name, long offset,
      ChildMetadataIndexType childMetadataIndexType) {
    this.name = name;
    this.offset = offset;
    this.childMetadataIndexType = childMetadataIndexType;
  }

  public String getName() {
    return name;
  }

  public long getOffset() {
    return offset;
  }

  public ChildMetadataIndexType getChildMetadataIndexType() {
    return childMetadataIndexType;
  }

  public void setName(String name) {
    this.name = name;
  }

  public void setOffset(long offset) {
    this.offset = offset;
  }

  public void setChildMetadataIndexType(
      ChildMetadataIndexType childMetadataIndexType) {
    this.childMetadataIndexType = childMetadataIndexType;
  }

  public String toString() {
    return "<" + name + "," + offset + "," + childMetadataIndexType + ">";
  }

  public int serializeTo(OutputStream outputStream) throws IOException {
    int byteLen = 0;
    byteLen += ReadWriteIOUtils.write(name, outputStream);
    byteLen += ReadWriteIOUtils.write(offset, outputStream);
    byteLen += ReadWriteIOUtils.write(childMetadataIndexType.serialize(), outputStream);
    return byteLen;
  }

  public static MetadataIndex deserializeFrom(ByteBuffer buffer) {
    MetadataIndex metadataIndex = new MetadataIndex();
    metadataIndex.setName(ReadWriteIOUtils.readString(buffer));
    metadataIndex.setOffset(ReadWriteIOUtils.readLong(buffer));
    metadataIndex.setChildMetadataIndexType(
        ChildMetadataIndexType.deserialize(ReadWriteIOUtils.readShort(buffer)));
    return metadataIndex;
  }
}
