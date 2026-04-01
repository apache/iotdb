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

package org.apache.iotdb.db.storageengine.load.splitter;

import org.apache.iotdb.common.rpc.thrift.TTimePartitionSlot;
import org.apache.iotdb.db.storageengine.load.splitter.LoadTsFileObjectFileBatch.ObjectFileChunk;

import org.apache.tsfile.utils.Pair;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;

public class LoadTsFileObjectFileBatchIterator
    implements Iterator<LoadTsFileObjectFileBatch>, AutoCloseable {

  private final Iterator<Pair<File, String>> fileIterator;
  private final int maxBatchSize;
  private final TTimePartitionSlot timePartitionSlot;

  private FileChannel currentChannel;
  private String currentRelativePath;
  private long currentTotalLength;
  private long currentOffset;

  private LoadTsFileObjectFileBatch nextBatch;

  public LoadTsFileObjectFileBatchIterator(
      final Set<Pair<File, String>> fileSet,
      final int maxBatchSize,
      final TTimePartitionSlot timePartitionSlot) {
    this.fileIterator = fileSet.iterator();
    this.maxBatchSize = maxBatchSize;
    this.timePartitionSlot = timePartitionSlot;
  }

  @Override
  public boolean hasNext() {
    if (nextBatch == null) {
      try {
        prepareNextBatch();
      } catch (IOException e) {
        throw new UncheckedIOException(
            "Failed to read object file while preparing batch for RPC dispatch", e);
      }
    }
    return nextBatch != null;
  }

  @Override
  public LoadTsFileObjectFileBatch next() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    LoadTsFileObjectFileBatch batch = nextBatch;
    nextBatch = null;
    return batch;
  }

  private void prepareNextBatch() throws IOException {
    List<ObjectFileChunk> chunks = new ArrayList<>();
    int currentBatchBytes = 0;

    while (true) {
      if (currentChannel == null && !openNextFile()) {
        if (!chunks.isEmpty()) {
          nextBatch = new LoadTsFileObjectFileBatch(chunks, timePartitionSlot);
        }
        return;
      }

      int availableSpace = maxBatchSize - currentBatchBytes;
      long remainingInFile = currentTotalLength - currentOffset;

      int readSize = (int) Math.min(availableSpace, remainingInFile);
      byte[] buffer = new byte[readSize];
      ByteBuffer byteBuffer = ByteBuffer.wrap(buffer);
      int bytesRead = 0;

      while (bytesRead < readSize) {
        int r = currentChannel.read(byteBuffer, currentOffset + bytesRead);
        if (r < 0) {
          break;
        }
        bytesRead += r;
      }

      if (bytesRead > 0) {
        boolean isLast = (currentOffset + bytesRead) >= currentTotalLength;
        chunks.add(
            new ObjectFileChunk(
                currentRelativePath, currentOffset, currentTotalLength, isLast, buffer));

        currentBatchBytes += bytesRead;
        currentOffset += bytesRead;
      }

      if (currentOffset >= currentTotalLength || bytesRead == 0) {
        closeCurrentFile();
      }

      if (currentBatchBytes >= maxBatchSize) {
        nextBatch = new LoadTsFileObjectFileBatch(chunks, timePartitionSlot);
        return;
      }
    }
  }

  private boolean openNextFile() throws IOException {
    while (fileIterator.hasNext()) {
      Pair<File, String> pair = fileIterator.next();
      File file = new File(pair.left, pair.right);

      if (file.exists() && file.isFile() && file.length() > 0) {
        currentChannel = FileChannel.open(file.toPath(), StandardOpenOption.READ);
        currentRelativePath = pair.right;
        currentTotalLength = file.length();
        currentOffset = 0;
        return true;
      }
    }
    return false;
  }

  private void closeCurrentFile() {
    if (currentChannel != null) {
      try {
        currentChannel.close();
      } catch (IOException ignored) {
      } finally {
        currentChannel = null;
      }
    }
  }

  @Override
  public void close() {
    closeCurrentFile();
  }
}
