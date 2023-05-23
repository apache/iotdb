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

import org.apache.iotdb.pipe.api.exception.PipeRuntimeCriticalException;
import org.apache.iotdb.pipe.api.exception.PipeRuntimeException;
import org.apache.iotdb.pipe.api.exception.PipeRuntimeNonCriticalException;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class PipeTaskMeta {

  // TODO: replace it with consensus index
  private final AtomicLong progressIndex = new AtomicLong(0L);
  private final AtomicInteger regionLeader = new AtomicInteger(0);
  private final Queue<PipeRuntimeException> exceptionMessages = new ConcurrentLinkedQueue<>();

  private PipeTaskMeta() {}

  public PipeTaskMeta(long progressIndex, int regionLeader) {
    this.progressIndex.set(progressIndex);
    this.regionLeader.set(regionLeader);
  }

  public long getProgressIndex() {
    return progressIndex.get();
  }

  public int getRegionLeader() {
    return regionLeader.get();
  }

  public Iterable<PipeRuntimeException> getExceptionMessages() {
    return exceptionMessages;
  }

  public void setExceptionMessages(Collection<? extends PipeRuntimeException> exceptionMessages) {
    this.exceptionMessages.clear();
    this.exceptionMessages.addAll(exceptionMessages);
  }

  public void mergeExceptionMessages(
      Collection<? extends PipeRuntimeException> newExceptionMessages) {
    exceptionMessages.addAll(newExceptionMessages);
  }

  public void trackException(boolean critical, String message) {
    exceptionMessages.add(
        critical
            ? new PipeRuntimeCriticalException(message)
            : new PipeRuntimeNonCriticalException(message));
  }

  public void setProgressIndex(long progressIndex) {
    this.progressIndex.set(progressIndex);
  }

  public void setRegionLeader(int regionLeader) {
    this.regionLeader.set(regionLeader);
  }

  public void serialize(DataOutputStream outputStream) throws IOException {
    ReadWriteIOUtils.write(progressIndex.get(), outputStream);
    ReadWriteIOUtils.write(regionLeader.get(), outputStream);
    ReadWriteIOUtils.write(exceptionMessages.size(), outputStream);
    for (final PipeRuntimeException exceptionMessage : exceptionMessages) {
      ReadWriteIOUtils.write(
          exceptionMessage instanceof PipeRuntimeCriticalException, outputStream);
      ReadWriteIOUtils.write(exceptionMessage.getMessage(), outputStream);
    }
  }

  public void serialize(FileOutputStream outputStream) throws IOException {
    ReadWriteIOUtils.write(progressIndex.get(), outputStream);
    ReadWriteIOUtils.write(regionLeader.get(), outputStream);
    ReadWriteIOUtils.write(exceptionMessages.size(), outputStream);
    for (final PipeRuntimeException exceptionMessage : exceptionMessages) {
      ReadWriteIOUtils.write(
          exceptionMessage instanceof PipeRuntimeCriticalException, outputStream);
      ReadWriteIOUtils.write(exceptionMessage.getMessage(), outputStream);
    }
  }

  public static PipeTaskMeta deserialize(ByteBuffer byteBuffer) {
    final PipeTaskMeta PipeTaskMeta = new PipeTaskMeta();
    PipeTaskMeta.progressIndex.set(ReadWriteIOUtils.readLong(byteBuffer));
    PipeTaskMeta.regionLeader.set(ReadWriteIOUtils.readInt(byteBuffer));
    final int size = ReadWriteIOUtils.readInt(byteBuffer);
    for (int i = 0; i < size; ++i) {
      final boolean critical = ReadWriteIOUtils.readBool(byteBuffer);
      final String message = ReadWriteIOUtils.readString(byteBuffer);
      PipeTaskMeta.exceptionMessages.add(
          critical
              ? new PipeRuntimeCriticalException(message)
              : new PipeRuntimeNonCriticalException(message));
    }
    return PipeTaskMeta;
  }

  public static PipeTaskMeta deserialize(InputStream inputStream) throws IOException {
    final PipeTaskMeta PipeTaskMeta = new PipeTaskMeta();
    PipeTaskMeta.progressIndex.set(ReadWriteIOUtils.readLong(inputStream));
    PipeTaskMeta.regionLeader.set(ReadWriteIOUtils.readInt(inputStream));
    final int size = ReadWriteIOUtils.readInt(inputStream);
    for (int i = 0; i < size; ++i) {
      final boolean critical = ReadWriteIOUtils.readBool(inputStream);
      final String message = ReadWriteIOUtils.readString(inputStream);
      PipeTaskMeta.exceptionMessages.add(
          critical
              ? new PipeRuntimeCriticalException(message)
              : new PipeRuntimeNonCriticalException(message));
    }
    return PipeTaskMeta;
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
    return progressIndex.get() == that.progressIndex.get()
        && regionLeader.get() == that.regionLeader.get()
        && Arrays.equals(exceptionMessages.toArray(), that.exceptionMessages.toArray());
  }

  @Override
  public int hashCode() {
    return Objects.hash(progressIndex, regionLeader, exceptionMessages);
  }

  @Override
  public String toString() {
    return "PipeTask{"
        + "progressIndex='"
        + progressIndex
        + '\''
        + ", regionLeader='"
        + regionLeader
        + '\''
        + ", exceptionMessages="
        + exceptionMessages
        + '}';
  }
}
