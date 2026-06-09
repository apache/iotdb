/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.storageengine.load.splitter;

import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.utils.RamUsageEstimator;

import java.io.File;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;

public abstract class AbstractChunkData implements ChunkData {

  /**
   * {@code OBJECT} column payload files referenced by values in this chunk: each entry is {@code
   * (searchRoot, sourceRelativePath)} to resolve the on-disk file encoded in the source OBJECT
   * binary.
   */
  private final Set<Pair<File, String>> objectFiles = new LinkedHashSet<>();

  private final Set<ObjectFileReference> objectFileReferences = new LinkedHashSet<>();

  private long objectMetadataSizeInBytes = 0L;

  protected final void copyObjectSidecarFrom(final AbstractChunkData other) {
    objectFiles.addAll(other.objectFiles);
    objectFileReferences.addAll(other.objectFileReferences);
    objectMetadataSizeInBytes += other.objectMetadataSizeInBytes;
  }

  @Override
  public Set<Pair<File, String>> getObjectFiles() {
    return objectFiles;
  }

  @Override
  public void addObjectRelativePath(final File parentDir, final String relativePath) {
    addObjectRelativePath(parentDir, relativePath, relativePath);
  }

  @Override
  public void addObjectRelativePath(
      final File parentDir, final String sourceRelativePath, final String targetRelativePath) {
    Objects.requireNonNull(parentDir, "parentDir");
    Objects.requireNonNull(sourceRelativePath, "sourceRelativePath");
    Objects.requireNonNull(targetRelativePath, "targetRelativePath");

    File resolvedObjectFile = new File(parentDir, sourceRelativePath);
    if (!resolvedObjectFile.isFile()) {
      throw new IllegalArgumentException(
          String.format(
              "Object file path does not point to a regular file: %s (parentDir=%s, relativePath=%s)",
              resolvedObjectFile.getAbsolutePath(),
              parentDir.getAbsolutePath(),
              sourceRelativePath));
    }

    final ObjectFileReference reference =
        new ObjectFileReference(parentDir, sourceRelativePath, targetRelativePath);
    if (objectFileReferences.add(reference)) {
      objectFiles.add(new Pair<>(parentDir, sourceRelativePath));
      objectMetadataSizeInBytes += RamUsageEstimator.sizeOf(sourceRelativePath);
      if (!sourceRelativePath.equals(targetRelativePath)) {
        objectMetadataSizeInBytes += RamUsageEstimator.sizeOf(targetRelativePath);
      }
    }
  }

  @Override
  public long getObjectMetadataSizeInBytes() {
    return objectMetadataSizeInBytes;
  }

  @Override
  public LoadTsFileObjectFileBatchIterator getObjectFileBatchIterator(final int maxBatchSize) {
    return new LoadTsFileObjectFileBatchIterator(
        objectFileReferences, maxBatchSize, getTimePartitionSlot());
  }
}
