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

import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

public class MetadataIndexEntry {

  private String name;
  private long offset;

  public MetadataIndexEntry(String name, long offset) {
    this.name = name;
    this.offset = offset;
  }

  public String getName() {
    return name;
  }

  public long getOffset() {
    return offset;
  }

  public void setName(String name) {
    this.name = name;
  }

  public void setOffset(long offset) {
    this.offset = offset;
  }

  public String toString() {
    return "<" + name + "," + offset + ">";
  }

  public int serializeTo(OutputStream outputStream) throws IOException {
    int byteLen = 0;
    byteLen += ReadWriteIOUtils.writeVar(name, outputStream);
    byteLen += ReadWriteIOUtils.write(offset, outputStream);
    return byteLen;
  }

  public static MetadataIndexEntry deserializeFrom(ByteBuffer buffer) {
    String name = ReadWriteIOUtils.readVarIntString(buffer);
    long offset = ReadWriteIOUtils.readLong(buffer);
    return new MetadataIndexEntry(name, offset);
  }
}
