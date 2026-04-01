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
   * (searchRoot, relativePath)} to resolve the on-disk file encoded in the OBJECT binary.
   */
  private final Set<Pair<File, String>> objectFiles = new LinkedHashSet<>();

  protected final void copyObjectSidecarFrom(final AbstractChunkData other) {
    objectFiles.addAll(other.objectFiles);
  }

  @Override
  public Set<Pair<File, String>> getObjectFiles() {
    return objectFiles;
  }

  @Override
  public void addObjectRelativePath(final File parentDir, final String relativePath) {
    Objects.requireNonNull(parentDir, "parentDir");
    Objects.requireNonNull(relativePath, "relativePath");

    File resolvedObjectFile = new File(parentDir, relativePath);
    if (!resolvedObjectFile.isFile()) {
      throw new IllegalArgumentException(
          String.format(
              "Object file path does not point to a regular file: %s (parentDir=%s, relativePath=%s)",
              resolvedObjectFile.getAbsolutePath(), parentDir.getAbsolutePath(), relativePath));
    }

    objectFiles.add(new Pair<>(parentDir, relativePath));
  }

  @Override
  public long getObjectMetadataSizeInBytes() {
    if (objectFiles.isEmpty()) {
      return 0L;
    }

    long size = 0L;
    for (Pair<File, String> pair : objectFiles) {
      if (pair.right != null) {
        size += RamUsageEstimator.sizeOf(pair.right);
      }
    }

    return size;
  }
}
