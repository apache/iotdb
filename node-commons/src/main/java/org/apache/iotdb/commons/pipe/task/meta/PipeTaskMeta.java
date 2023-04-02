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

package org.apache.iotdb.commons.pipe.task.meta;

import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

public class PipeTaskMeta {

  // TODO: replace it with consensus index
  private long index;

  private int regionLeader;

  private PipeTaskMeta() {}

  public PipeTaskMeta(long index, int regionLeader) {
    this.index = index;
    this.regionLeader = regionLeader;
  }

  public long getIndex() {
    return index;
  }

  public int getRegionLeader() {
    return regionLeader;
  }

  public void setIndex(long index) {
    this.index = index;
  }

  public void setRegionLeader(int regionLeader) {
    this.regionLeader = regionLeader;
  }

  public void serialize(DataOutputStream outputStream) throws IOException {
    ReadWriteIOUtils.write(index, outputStream);
    ReadWriteIOUtils.write(regionLeader, outputStream);
  }

  public static PipeTaskMeta deserialize(ByteBuffer byteBuffer) {
    PipeTaskMeta PipeTaskMeta = new PipeTaskMeta();
    PipeTaskMeta.index = ReadWriteIOUtils.readLong(byteBuffer);
    PipeTaskMeta.regionLeader = ReadWriteIOUtils.readInt(byteBuffer);
    return PipeTaskMeta;
  }

  public static PipeTaskMeta deserialize(InputStream inputStream) throws IOException {
    return deserialize(
        ByteBuffer.wrap(ReadWriteIOUtils.readBytesWithSelfDescriptionLength(inputStream)));
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    PipeTaskMeta that = (PipeTaskMeta) obj;
    return index == that.index && regionLeader == that.regionLeader;
  }

  @Override
  public int hashCode() {
    return (int) (index * 31 + regionLeader);
  }

  @Override
  public String toString() {
    return "PipeTask{" + "index='" + index + '\'' + ", regionLeader='" + regionLeader + '\'' + '}';
  }
}
