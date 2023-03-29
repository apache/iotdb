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

public class DataRegionPipeTask {

  private long index;

  private long version;

  private int regionGroup;

  private int regionLeader;

  private DataRegionPipeTask() {}

  public DataRegionPipeTask(long index, long version, int regionGroup, int regionLeader) {
    this.index = index;
    this.version = version;
    this.regionGroup = regionGroup;
    this.regionLeader = regionLeader;
  }

  public long getIndex() {
    return index;
  }

  public long getVersion() {
    return version;
  }

  public int getRegionGroup() {
    return regionGroup;
  }

  public int getRegionLeader() {
    return regionLeader;
  }

  public void setIndex(long index) {
    this.index = index;
  }

  public void setVersion(long version) {
    this.version = version;
  }

  public void setRegionLeader(int regionLeader) {
    this.regionLeader = regionLeader;
  }

  public void serialize(DataOutputStream outputStream) throws IOException {
    ReadWriteIOUtils.write(index, outputStream);
    ReadWriteIOUtils.write(version, outputStream);
    ReadWriteIOUtils.write(regionGroup, outputStream);
    ReadWriteIOUtils.write(regionLeader, outputStream);
  }

  public static DataRegionPipeTask deserialize(ByteBuffer byteBuffer) {
    DataRegionPipeTask dataRegionPipeTask = new DataRegionPipeTask();
    dataRegionPipeTask.index = ReadWriteIOUtils.readLong(byteBuffer);
    dataRegionPipeTask.version = ReadWriteIOUtils.readLong(byteBuffer);
    dataRegionPipeTask.regionGroup = ReadWriteIOUtils.readInt(byteBuffer);
    dataRegionPipeTask.regionLeader = ReadWriteIOUtils.readInt(byteBuffer);
    return dataRegionPipeTask;
  }

  public static DataRegionPipeTask deserialize(InputStream inputStream) throws IOException {
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
    DataRegionPipeTask that = (DataRegionPipeTask) obj;
    return index == that.index
        && version == that.version
        && regionGroup == that.regionGroup
        && regionLeader == that.regionLeader;
  }

  @Override
  public int hashCode() {
    return regionGroup;
  }

  @Override
  public String toString() {
    return "DataRegionPipeTask{"
        + "regionGroup='"
        + regionGroup
        + '\''
        + ", index='"
        + index
        + '\''
        + ", version='"
        + version
        + '\''
        + ", regionLeader='"
        + regionLeader
        + '\''
        + '}';
  }
}
