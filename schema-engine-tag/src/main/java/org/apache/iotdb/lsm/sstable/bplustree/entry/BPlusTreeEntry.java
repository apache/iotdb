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
package org.apache.iotdb.lsm.sstable.bplustree.entry;

import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

public class BPlusTreeEntry implements IEntry {

  String name;

  long offset;

  public BPlusTreeEntry() {}

  public BPlusTreeEntry(String name, long offset) {
    this.name = name;
    this.offset = offset;
  }

  @Override
  public void serialize(DataOutputStream out) throws IOException {
    ReadWriteIOUtils.write(name, out);
    ReadWriteIOUtils.write(offset, out);
  }

  @Override
  public void serialize(ByteBuffer byteBuffer) {
    ReadWriteIOUtils.write(name, byteBuffer);
    ReadWriteIOUtils.write(offset, byteBuffer);
  }

  @Override
  public IEntry deserialize(DataInputStream input) throws IOException {
    name = ReadWriteIOUtils.readString(input);
    offset = ReadWriteIOUtils.readLong(input);
    return this;
  }

  @Override
  public IEntry deserialize(ByteBuffer byteBuffer) {
    name = ReadWriteIOUtils.readString(byteBuffer);
    offset = ReadWriteIOUtils.readLong(byteBuffer);
    return this;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    BPlusTreeEntry that = (BPlusTreeEntry) o;
    return offset == that.offset && Objects.equals(name, that.name);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, offset);
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public long getOffset() {
    return offset;
  }

  public void setOffset(long offset) {
    this.offset = offset;
  }

  @Override
  public String toString() {
    return "BPlusTreeEntry{" + "name='" + name + '\'' + ", offset=" + offset + '}';
  }
}
