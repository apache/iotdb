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
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class PipeTaskMeta {

  // TODO: replace it with consensus index
  private final AtomicLong index = new AtomicLong(0L);
  private final AtomicInteger regionLeader = new AtomicInteger(0);
  private final List<String> exceptionMessages = Collections.synchronizedList(new LinkedList<>());

  private PipeTaskMeta() {}

  public PipeTaskMeta(long index, int regionLeader) {
    this.index.set(index);
    this.regionLeader.set(regionLeader);
  }

  public long getIndex() {
    return index.get();
  }

  public int getRegionLeader() {
    return regionLeader.get();
  }

  public List<String> getExceptionMessages() {
    return exceptionMessages;
  }

  public void setIndex(long index) {
    this.index.set(index);
  }

  public void setRegionLeader(int regionLeader) {
    this.regionLeader.set(regionLeader);
  }

  public void serialize(DataOutputStream outputStream) throws IOException {
    ReadWriteIOUtils.write(index.get(), outputStream);
    ReadWriteIOUtils.write(regionLeader.get(), outputStream);
    ReadWriteIOUtils.write(exceptionMessages.size(), outputStream);
    for (String exceptionMessage : exceptionMessages) {
      ReadWriteIOUtils.write(exceptionMessage, outputStream);
    }
  }

  public static PipeTaskMeta deserialize(ByteBuffer byteBuffer) {
    final PipeTaskMeta PipeTaskMeta = new PipeTaskMeta();
    PipeTaskMeta.index.set(ReadWriteIOUtils.readLong(byteBuffer));
    PipeTaskMeta.regionLeader.set(ReadWriteIOUtils.readInt(byteBuffer));
    int size = ReadWriteIOUtils.readInt(byteBuffer);
    for (int i = 0; i < size; ++i) {
      PipeTaskMeta.exceptionMessages.add(ReadWriteIOUtils.readString(byteBuffer));
    }
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
    return index.get() == that.index.get()
        && exceptionMessages.equals(that.exceptionMessages)
        && regionLeader.get() == that.regionLeader.get();
  }

  @Override
  public int hashCode() {
    return Objects.hash(index, exceptionMessages, regionLeader);
  }

  @Override
  public String toString() {
    return "PipeTask{"
        + "index='"
        + index
        + '\''
        + ", regionLeader='"
        + regionLeader
        + '\''
        + ", exceptionMessages="
        + exceptionMessages
        + '}';
  }
}
