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

import java.io.DataInput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class BPlusTreeHeader implements IEntry {

  String max;

  String min;

  long rootNodeoffset = -1;

  @Override
  public void serialize(DataOutputStream out) throws IOException {
    ReadWriteIOUtils.write(max, out);
    ReadWriteIOUtils.write(min, out);
    out.writeLong(rootNodeoffset);
  }

  @Override
  public void serialize(ByteBuffer byteBuffer) {
    ReadWriteIOUtils.write(max, byteBuffer);
    ReadWriteIOUtils.write(min, byteBuffer);
    byteBuffer.putLong(rootNodeoffset);
  }

  @Override
  public IEntry deserialize(DataInput input) throws IOException {
    return null;
  }

  @Override
  public IEntry deserialize(ByteBuffer byteBuffer) {
    return null;
  }

  public String getMax() {
    return max;
  }

  public void setMax(String max) {
    this.max = max;
  }

  public String getMin() {
    return min;
  }

  public void setMin(String min) {
    this.min = min;
  }

  public long getOffset() {
    return rootNodeoffset;
  }

  public void setOffset(long offset) {
    this.rootNodeoffset = offset;
  }
}
