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

import org.apache.iotdb.commons.consensus.index.ProgressIndex;
import org.apache.iotdb.commons.consensus.index.ProgressIndexType;
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
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class PipeTaskMeta {

  private final AtomicReference<ProgressIndex> progressIndex = new AtomicReference<>();
  private final AtomicInteger leaderDataNodeId = new AtomicInteger(0);
  private final Queue<PipeRuntimeException> exceptionMessages = new ConcurrentLinkedQueue<>();

  public PipeTaskMeta(ProgressIndex progressIndex, int leaderDataNodeId) {
    this.progressIndex.set(progressIndex);
    this.leaderDataNodeId.set(leaderDataNodeId);
  }

  public ProgressIndex getProgressIndex() {
    return progressIndex.get();
  }

  public void updateProgressIndex(ProgressIndex updateIndex) {
    progressIndex.updateAndGet(index -> index.updateToMaximum(updateIndex));
  }

  public int getLeaderDataNodeId() {
    return leaderDataNodeId.get();
  }

  public void setLeaderDataNodeId(int leaderDataNodeId) {
    this.leaderDataNodeId.set(leaderDataNodeId);
  }

  public Iterable<PipeRuntimeException> getExceptionMessages() {
    return exceptionMessages;
  }

  public void trackExceptionMessage(PipeRuntimeException exceptionMessage) {
    exceptionMessages.add(exceptionMessage);
  }

  public void clearExceptionMessages() {
    exceptionMessages.clear();
  }

  public void serialize(DataOutputStream outputStream) throws IOException {
    progressIndex.get().serialize(outputStream);
    ReadWriteIOUtils.write(leaderDataNodeId.get(), outputStream);
    ReadWriteIOUtils.write(exceptionMessages.size(), outputStream);
    for (final PipeRuntimeException exceptionMessage : exceptionMessages) {
      ReadWriteIOUtils.write(
          exceptionMessage instanceof PipeRuntimeCriticalException, outputStream);
      ReadWriteIOUtils.write(exceptionMessage.getMessage(), outputStream);
    }
  }

  public void serialize(FileOutputStream outputStream) throws IOException {
    progressIndex.get().serialize(outputStream);
    ReadWriteIOUtils.write(leaderDataNodeId.get(), outputStream);
    ReadWriteIOUtils.write(exceptionMessages.size(), outputStream);
    for (final PipeRuntimeException exceptionMessage : exceptionMessages) {
      ReadWriteIOUtils.write(
          exceptionMessage instanceof PipeRuntimeCriticalException, outputStream);
      ReadWriteIOUtils.write(exceptionMessage.getMessage(), outputStream);
    }
  }

  public static PipeTaskMeta deserialize(ByteBuffer byteBuffer) {
    final PipeTaskMeta PipeTaskMeta =
        new PipeTaskMeta(
            ProgressIndexType.deserializeFrom(byteBuffer), ReadWriteIOUtils.readInt(byteBuffer));
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
    final PipeTaskMeta PipeTaskMeta =
        new PipeTaskMeta(
            ProgressIndexType.deserializeFrom(inputStream), ReadWriteIOUtils.readInt(inputStream));
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
    return progressIndex.get().equals(that.progressIndex.get())
        && leaderDataNodeId.get() == that.leaderDataNodeId.get()
        && Arrays.equals(exceptionMessages.toArray(), that.exceptionMessages.toArray());
  }

  @Override
  public int hashCode() {
    return Objects.hash(progressIndex, leaderDataNodeId, exceptionMessages);
  }

  @Override
  public String toString() {
    return "PipeTask{"
        + "progressIndex='"
        + progressIndex
        + '\''
        + ", leaderDataNodeId='"
        + leaderDataNodeId
        + '\''
        + ", exceptionMessages="
        + exceptionMessages
        + '}';
  }
}
