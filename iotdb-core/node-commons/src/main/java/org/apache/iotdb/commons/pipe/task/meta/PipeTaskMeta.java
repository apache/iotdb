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
import org.apache.iotdb.commons.exception.pipe.PipeRuntimeException;
import org.apache.iotdb.commons.exception.pipe.PipeRuntimeExceptionType;
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

  public PipeTaskMeta(/* @NotNull */ ProgressIndex progressIndex, int leaderDataNodeId) {
    this.progressIndex.set(progressIndex);
    this.leaderDataNodeId.set(leaderDataNodeId);
  }

  public ProgressIndex getProgressIndex() {
    return progressIndex.get();
  }

  public ProgressIndex updateProgressIndex(ProgressIndex updateIndex) {
    return progressIndex.updateAndGet(
        index -> index.updateToMinimumIsAfterProgressIndex(updateIndex));
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
    for (final PipeRuntimeException pipeRuntimeException : exceptionMessages) {
      pipeRuntimeException.serialize(outputStream);
    }
  }

  public void serialize(FileOutputStream outputStream) throws IOException {
    progressIndex.get().serialize(outputStream);
    ReadWriteIOUtils.write(leaderDataNodeId.get(), outputStream);
    ReadWriteIOUtils.write(exceptionMessages.size(), outputStream);
    for (final PipeRuntimeException pipeRuntimeException : exceptionMessages) {
      pipeRuntimeException.serialize(outputStream);
    }
  }

  public static PipeTaskMeta deserialize(ByteBuffer byteBuffer) {
    final ProgressIndex progressIndex = ProgressIndexType.deserializeFrom(byteBuffer);
    final int leaderDataNodeId = ReadWriteIOUtils.readInt(byteBuffer);
    final PipeTaskMeta pipeTaskMeta = new PipeTaskMeta(progressIndex, leaderDataNodeId);
    final int size = ReadWriteIOUtils.readInt(byteBuffer);
    for (int i = 0; i < size; ++i) {
      final PipeRuntimeException pipeRuntimeException =
          PipeRuntimeExceptionType.deserializeFrom(byteBuffer);
      pipeTaskMeta.exceptionMessages.add(pipeRuntimeException);
    }
    return pipeTaskMeta;
  }

  public static PipeTaskMeta deserialize(InputStream inputStream) throws IOException {
    final ProgressIndex progressIndex = ProgressIndexType.deserializeFrom(inputStream);
    final int leaderDataNodeId = ReadWriteIOUtils.readInt(inputStream);
    final PipeTaskMeta pipeTaskMeta = new PipeTaskMeta(progressIndex, leaderDataNodeId);
    final int size = ReadWriteIOUtils.readInt(inputStream);
    for (int i = 0; i < size; ++i) {
      final PipeRuntimeException pipeRuntimeException =
          PipeRuntimeExceptionType.deserializeFrom(inputStream);
      pipeTaskMeta.exceptionMessages.add(pipeRuntimeException);
    }
    return pipeTaskMeta;
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
        + "', leaderDataNodeId="
        + leaderDataNodeId
        + ", exceptionMessages='"
        + exceptionMessages
        + "'}";
  }
}
