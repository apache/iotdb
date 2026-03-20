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
 * software distributed under this work for additional information
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

package org.apache.iotdb.db.schemaengine.schemaregion.write.resp;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathDeserializeUtil;

import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Result of constructSchemaBlackList operation, containing: 1. Pre-deleted series count and whether
 * all are logical views (original {@code Pair<Long, Boolean>}) 2. Whether all matched series are
 * invalid series 3. Referenced invalid paths information list (associated invalid series that need
 * to be deleted but don't need pre-delete)
 */
public class ConstructSchemaBlackListResult {

  private final long preDeletedNum;
  private final boolean isAllLogicalView;
  private final boolean isAllInvalidSeries;
  private final List<ReferencedInvalidPathInfo> referencedInvalidPaths;
  private final List<PartialPath> preDeletedPaths; // Active deletion paths (non-invalid series)
  private final boolean hasInvalidSeries; // Whether there are any invalid series

  public ConstructSchemaBlackListResult(
      final long preDeletedNum,
      final boolean isAllLogicalView,
      final boolean isAllInvalidSeries,
      final boolean hasInvalidSeries,
      final List<ReferencedInvalidPathInfo> referencedInvalidPaths,
      final List<PartialPath> preDeletedPaths) {
    this.preDeletedNum = preDeletedNum;
    this.isAllLogicalView = isAllLogicalView;
    this.isAllInvalidSeries = isAllInvalidSeries;
    this.referencedInvalidPaths =
        referencedInvalidPaths != null ? referencedInvalidPaths : new ArrayList<>();
    this.preDeletedPaths = preDeletedPaths != null ? preDeletedPaths : new ArrayList<>();
    this.hasInvalidSeries = hasInvalidSeries;
  }

  public ConstructSchemaBlackListResult(final Pair<Long, Boolean> pair) {
    this.preDeletedNum = pair.left;
    this.isAllLogicalView = pair.right;
    this.isAllInvalidSeries = false;
    this.referencedInvalidPaths = new ArrayList<>();
    this.preDeletedPaths = new ArrayList<>();
    this.hasInvalidSeries = false;
  }

  public long getPreDeletedNum() {
    return preDeletedNum;
  }

  public boolean isAllLogicalView() {
    return isAllLogicalView;
  }

  public boolean isAllInvalidSeries() {
    return isAllInvalidSeries;
  }

  public List<ReferencedInvalidPathInfo> getReferencedInvalidPaths() {
    return referencedInvalidPaths;
  }

  public List<PartialPath> getPreDeletedPaths() {
    return preDeletedPaths;
  }

  public boolean hasInvalidSeries() {
    return hasInvalidSeries;
  }

  public Pair<Long, Boolean> toPair() {
    return new Pair<>(preDeletedNum, isAllLogicalView);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ConstructSchemaBlackListResult that = (ConstructSchemaBlackListResult) o;
    return preDeletedNum == that.preDeletedNum
        && isAllLogicalView == that.isAllLogicalView
        && isAllInvalidSeries == that.isAllInvalidSeries
        && hasInvalidSeries == that.hasInvalidSeries
        && referencedInvalidPaths.equals(that.referencedInvalidPaths)
        && preDeletedPaths.equals(that.preDeletedPaths);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        preDeletedNum,
        isAllLogicalView,
        isAllInvalidSeries,
        hasInvalidSeries,
        referencedInvalidPaths,
        preDeletedPaths);
  }

  /**
   * Serialize this result into the given ByteBuffer.
   *
   * @param buffer the buffer to write to
   */
  public void serialize(ByteBuffer buffer) {
    ReadWriteIOUtils.write(preDeletedNum, buffer);
    ReadWriteIOUtils.write(isAllLogicalView, buffer);
    ReadWriteIOUtils.write(isAllInvalidSeries, buffer);
    ReadWriteIOUtils.write(hasInvalidSeries, buffer);
    ReadWriteIOUtils.write(referencedInvalidPaths.size(), buffer);
    for (ReferencedInvalidPathInfo pathInfo : referencedInvalidPaths) {
      pathInfo.serialize(buffer);
    }
    ReadWriteIOUtils.write(preDeletedPaths.size(), buffer);
    for (PartialPath path : preDeletedPaths) {
      path.serialize(buffer);
    }
  }

  /**
   * Serialize this result into a new ByteBuffer.
   *
   * @return a new ByteBuffer containing the serialized bytes
   */
  public ByteBuffer serialize() {
    ByteBuffer buffer = ByteBuffer.allocate(estimateSerializedSize());
    serialize(buffer);
    buffer.flip();
    return buffer;
  }

  /** Conservative estimate of path serialized size in bytes for allocation. */
  private static final int ESTIMATE_PATH_BYTES = 256;

  /** Estimate the serialized size in bytes for allocation. May over-estimate. */
  private int estimateSerializedSize() {
    int size = Long.BYTES + 1 + 1 + 1 + Integer.BYTES * 2;
    for (ReferencedInvalidPathInfo pathInfo : referencedInvalidPaths) {
      size += pathInfo.estimateSerializedSize();
    }
    size += preDeletedPaths.size() * ESTIMATE_PATH_BYTES;
    return size;
  }

  /**
   * Deserialize a ConstructSchemaBlackListResult from the given ByteBuffer.
   *
   * @param buffer the buffer to read from (position will be advanced)
   * @return the deserialized result
   */
  public static ConstructSchemaBlackListResult deserialize(ByteBuffer buffer) {
    long preDeletedNum = ReadWriteIOUtils.readLong(buffer);
    boolean isAllLogicalView = ReadWriteIOUtils.readBool(buffer);
    boolean isAllInvalidSeries = ReadWriteIOUtils.readBool(buffer);
    boolean hasInvalidSeries = ReadWriteIOUtils.readBool(buffer);
    int refInvalidSize = ReadWriteIOUtils.readInt(buffer);
    List<ReferencedInvalidPathInfo> referencedInvalidPaths = new ArrayList<>(refInvalidSize);
    for (int i = 0; i < refInvalidSize; i++) {
      referencedInvalidPaths.add(ReferencedInvalidPathInfo.deserialize(buffer));
    }
    int preDeletedSize = ReadWriteIOUtils.readInt(buffer);
    List<PartialPath> preDeletedPaths = new ArrayList<>(preDeletedSize);
    for (int i = 0; i < preDeletedSize; i++) {
      preDeletedPaths.add((PartialPath) PathDeserializeUtil.deserialize(buffer));
    }
    return new ConstructSchemaBlackListResult(
        preDeletedNum,
        isAllLogicalView,
        isAllInvalidSeries,
        hasInvalidSeries,
        referencedInvalidPaths,
        preDeletedPaths);
  }

  /**
   * Information about a referenced invalid path that needs to be deleted. Referenced invalid paths
   * are associated with the series being deleted (e.g., if deleting an alias series, its
   * corresponding invalid physical series should also be deleted). Invalid series don't need
   * pre-delete because they are already invalid.
   */
  public static class ReferencedInvalidPathInfo {
    private final PartialPath path; // The invalid series path
    private final PartialPath originalPath; // ORIGINAL_PATH if this is an alias series
    private final boolean isRenamed; // IS_RENAMED flag

    public ReferencedInvalidPathInfo(
        final PartialPath path, final PartialPath originalPath, final boolean isRenamed) {
      this.path = path;
      this.originalPath = originalPath;
      this.isRenamed = isRenamed;
    }

    public PartialPath getPath() {
      return path;
    }

    public PartialPath getOriginalPath() {
      return originalPath;
    }

    public boolean isRenamed() {
      return isRenamed;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      ReferencedInvalidPathInfo that = (ReferencedInvalidPathInfo) o;
      return isRenamed == that.isRenamed
          && Objects.equals(path, that.path)
          && Objects.equals(originalPath, that.originalPath);
    }

    @Override
    public int hashCode() {
      return Objects.hash(path, originalPath, isRenamed);
    }

    /** Serialize this info into the given ByteBuffer. */
    public void serialize(ByteBuffer buffer) {
      path.serialize(buffer);
      ReadWriteIOUtils.write(originalPath != null, buffer);
      if (originalPath != null) {
        originalPath.serialize(buffer);
      }
      ReadWriteIOUtils.write(isRenamed, buffer);
    }

    private static final int ESTIMATE_PATH_BYTES = 256;

    /** Estimate serialized size in bytes (for allocation). May over-estimate. */
    int estimateSerializedSize() {
      int size = ESTIMATE_PATH_BYTES + 1 + 1;
      if (originalPath != null) {
        size += ESTIMATE_PATH_BYTES;
      }
      return size;
    }

    /**
     * Deserialize a ReferencedInvalidPathInfo from the given ByteBuffer.
     *
     * @param buffer the buffer to read from (position will be advanced)
     * @return the deserialized info
     */
    public static ReferencedInvalidPathInfo deserialize(ByteBuffer buffer) {
      PartialPath path = (PartialPath) PathDeserializeUtil.deserialize(buffer);
      boolean hasOriginalPath = ReadWriteIOUtils.readBool(buffer);
      PartialPath originalPath =
          hasOriginalPath ? (PartialPath) PathDeserializeUtil.deserialize(buffer) : null;
      boolean isRenamed = ReadWriteIOUtils.readBool(buffer);
      return new ReferencedInvalidPathInfo(path, originalPath, isRenamed);
    }
  }
}
