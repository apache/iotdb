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

package org.apache.iotdb.db.storageengine.dataregion.read;

import org.apache.iotdb.calc.execution.filter.TopKRuntimeFilter;
import org.apache.iotdb.db.i18n.StorageEngineMessages;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;

import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.read.filter.basic.Filter;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.TreeMap;

/**
 * The QueryDataSource contains all the seq and unseq TsFileResources for one timeseries in one
 * read.
 */
public class QueryDataSource implements IQueryDataSource {

  /**
   * TsFileResources used by read job.
   *
   * <p>Note: Sequences under the same data region share two lists of TsFileResources (seq and
   * unseq).
   */
  private final List<TsFileResource> seqResources;

  private int curSeqIndex = -1;

  // asc: startTime, will be Long.MIN_VALUE if current tsfile resource is degraded
  // desc: endTime, will be Long.MAX_VALUE if current tsfile resource is degraded
  // if current tsfile resource is degraded, it will always be considered to be overlapping with
  // current point
  private long curSeqOrderTime = 0;

  private Boolean curSeqSatisfied = null;

  private final List<TsFileResource> unseqResources;

  private int curUnSeqIndex = -1;

  // asc: startTime; desc: endTime
  private long curUnSeqOrderTime = 0;

  private Boolean curUnSeqSatisfied = null;

  private boolean isSingleDevice;

  /* The traversal order of unseqResources (different for each device) */
  private int[] unSeqFileOrderIndex;

  private String databaseName = null;

  /**
   * Remaining seq/unseq TsFileResources that may still contain rows qualifying for TopK runtime
   * filter. Initialized to list sizes and decremented when a file is skipped at resource level.
   * When both counters reach zero, the scan can exit early.
   */
  private int seqValidSize;

  private int unseqValidSize;

  private static final Comparator<Long> descendingComparator = (o1, o2) -> Long.compare(o2, o1);

  public QueryDataSource(List<TsFileResource> seqResources, List<TsFileResource> unseqResources) {
    this.seqResources = seqResources;
    this.unseqResources = unseqResources;
    initValidSize();
  }

  public QueryDataSource(
      List<TsFileResource> seqResources, List<TsFileResource> unseqResources, String databaseName) {
    this.seqResources = seqResources;
    this.unseqResources = unseqResources;
    this.databaseName = databaseName;
    initValidSize();
  }

  // used for compaction, because in compaction task(unlike query, each QueryDataSource only serve
  // for one series), we will reuse this object for multi series
  public QueryDataSource(QueryDataSource other) {
    this.seqResources = other.seqResources;
    this.unseqResources = other.unseqResources;
    this.unSeqFileOrderIndex = other.unSeqFileOrderIndex;
    this.databaseName = other.databaseName;
    this.seqValidSize = other.seqValidSize;
    this.unseqValidSize = other.unseqValidSize;
  }

  /** Initializes seq/unseq valid resource counters from list sizes. */
  private void initValidSize() {
    seqValidSize = getSeqResourcesSize();
    unseqValidSize = getUnseqResourcesSize();
  }

  public int getSeqValidSize() {
    return seqValidSize;
  }

  public int getUnseqValidSize() {
    return unseqValidSize;
  }

  /** Returns true if either seq or unseq may still contain RF-qualifying resources. */
  public boolean hasValidResource() {
    return seqValidSize > 0 || unseqValidSize > 0;
  }

  /**
   * Decrements the remaining file count when a TsFileResource is pruned by TopK runtime filter at
   * resource level. Counters are shared across all devices in this scan; when both reach zero, the
   * entire scan can stop without switching to subsequent devices.
   */
  public void decreaseValidSize(boolean isSeq) {
    if (isSeq) {
      if (seqValidSize > 0) {
        seqValidSize--;
      }
    } else if (unseqValidSize > 0) {
      unseqValidSize--;
    }
  }

  /**
   * Decrements validSize when a TsFileResource is first pruned by TopK runtime filter at resource
   * level. File lists are time-ordered, so RF pruning is monotonic from one end: ASC prunes a
   * suffix, DESC prunes a prefix. {@link #seqValidSize} / {@link #unseqValidSize} then double as
   * the cross-device scan watermark.
   */
  public void decreaseValidSizeForRuntimeFilter(boolean isSeq, int index, boolean ascending) {
    if (isRuntimeFilterPruned(isSeq, index, ascending)) {
      return;
    }
    decreaseValidSize(isSeq);
  }

  /**
   * Truncates seq validSize when RF fails at {@code index}. Seq files are time-ordered, so all
   * files on the pruned side of {@code index} can be excluded at once: ASC suffix {@code [index,
   * size)}, DESC prefix {@code [0, index]}.
   */
  public void truncateSeqValidSizeForRuntimeFilter(int index, boolean ascending) {
    int size = getSeqResourcesSize();
    if (ascending) {
      seqValidSize = Math.min(seqValidSize, index);
    } else {
      seqValidSize = Math.min(seqValidSize, size - index - 1);
    }
  }

  /**
   * Returns true if this file index was already pruned by RF at resource level on a prior device.
   */
  public boolean isRuntimeFilterPruned(boolean isSeq, int index, boolean ascending) {
    int validSize = isSeq ? seqValidSize : unseqValidSize;
    int size = isSeq ? getSeqResourcesSize() : getUnseqResourcesSize();
    if (ascending) {
      // ASC: pruned files form suffix [validSize, size)
      return index >= validSize;
    }
    // DESC: pruned files form prefix [0, size - validSize)
    return index < size - validSize;
  }

  public List<TsFileResource> getSeqResources() {
    return seqResources;
  }

  public List<TsFileResource> getUnseqResources() {
    return unseqResources;
  }

  public boolean isEmpty() {
    return (seqResources == null || seqResources.isEmpty())
        && (unseqResources == null || unseqResources.isEmpty());
  }

  @Override
  public IQueryDataSource clone() {
    QueryDataSource queryDataSource =
        new QueryDataSource(getSeqResources(), getUnseqResources(), databaseName);
    queryDataSource.setSingleDevice(isSingleDevice());
    return queryDataSource;
  }

  public boolean hasNextSeqResource(int curIndex, boolean ascending, IDeviceID deviceID) {
    boolean res = ascending ? curIndex < seqResources.size() : curIndex >= 0;
    if (res && curIndex != this.curSeqIndex) {
      this.curSeqIndex = curIndex;
      this.curSeqOrderTime = seqResources.get(curIndex).getOrderTimeForSeq(deviceID, ascending);
      this.curSeqSatisfied = null;
    }
    return res;
  }

  public boolean isSeqSatisfied(
      IDeviceID deviceID, int curIndex, Filter timeFilter, boolean debug) {
    if (curIndex != this.curSeqIndex) {
      throw new IllegalArgumentException(
          String.format(
              StorageEngineMessages
                  .STORAGE_EXCEPTION_CURINDEX_D_IS_NOT_EQUAL_TO_CURSEQINDEX_D_6B9B1134,
              curIndex,
              this.curSeqIndex));
    }
    if (curSeqSatisfied == null) {
      TsFileResource tsFileResource = seqResources.get(curSeqIndex);
      curSeqSatisfied =
          tsFileResource != null
              && (isSingleDevice || tsFileResource.isSatisfied(deviceID, timeFilter, true, debug));
    }

    return curSeqSatisfied;
  }

  public boolean isSeqSatisfiedByRuntimeFilter(
      IDeviceID deviceID, int curIndex, TopKRuntimeFilter filter, boolean debug) {
    return isResourceSatisfiedByRuntimeFilter(curIndex, filter, true, debug);
  }

  public long getCurrentSeqOrderTime(int curIndex) {
    if (curIndex != this.curSeqIndex) {
      throw new IllegalArgumentException(
          String.format(
              StorageEngineMessages
                  .STORAGE_EXCEPTION_CURINDEX_D_IS_NOT_EQUAL_TO_CURSEQINDEX_D_6B9B1134,
              curIndex,
              this.curSeqIndex));
    }
    return this.curSeqOrderTime;
  }

  public TsFileResource getSeqResourceByIndex(int curIndex) {
    if (curIndex < seqResources.size()) {
      return seqResources.get(curIndex);
    }
    return null;
  }

  public boolean hasNextUnseqResource(int curIndex, boolean ascending, IDeviceID deviceID) {
    boolean res = curIndex < unseqResources.size();
    if (res && curIndex != this.curUnSeqIndex) {
      this.curUnSeqIndex = curIndex;
      this.curUnSeqOrderTime =
          unseqResources
              .get(unSeqFileOrderIndex[curIndex])
              .getOrderTimeForUnseq(deviceID, ascending);
      this.curUnSeqSatisfied = null;
    }
    return res;
  }

  public boolean isUnSeqSatisfied(
      IDeviceID deviceID, int curIndex, Filter timeFilter, boolean debug) {
    if (curIndex != this.curUnSeqIndex) {
      throw new IllegalArgumentException(
          String.format(
              StorageEngineMessages
                  .STORAGE_EXCEPTION_CURINDEX_D_IS_NOT_EQUAL_TO_CURUNSEQINDEX_D_AB32F71D,
              curIndex,
              this.curUnSeqIndex));
    }
    if (curUnSeqSatisfied == null) {
      TsFileResource tsFileResource = unseqResources.get(unSeqFileOrderIndex[curIndex]);
      curUnSeqSatisfied =
          tsFileResource != null
              && (isSingleDevice || tsFileResource.isSatisfied(deviceID, timeFilter, false, debug));
    }

    return curUnSeqSatisfied;
  }

  public boolean isUnSeqSatisfiedByRuntimeFilter(
      int curIndex, TopKRuntimeFilter filter, boolean debug) {
    return isResourceSatisfiedByRuntimeFilter(curIndex, filter, false, debug);
  }

  public long getCurrentUnSeqOrderTime(int curIndex) {
    if (curIndex != this.curUnSeqIndex) {
      throw new IllegalArgumentException(
          String.format(
              StorageEngineMessages
                  .STORAGE_EXCEPTION_CURINDEX_D_IS_NOT_EQUAL_TO_CURSEQINDEX_D_6B9B1134,
              curIndex,
              this.curUnSeqIndex));
    }
    return this.curUnSeqOrderTime;
  }

  public TsFileResource getUnseqResourceByIndex(int curIndex) {
    int actualIndex = unSeqFileOrderIndex[curIndex];
    if (actualIndex < unseqResources.size()) {
      return unseqResources.get(actualIndex);
    }
    return null;
  }

  public int getSeqResourcesSize() {
    return seqResources.size();
  }

  public int getUnseqResourcesSize() {
    return unseqResources.size();
  }

  public void fillOrderIndexes(IDeviceID deviceId, boolean ascending) {
    if (unseqResources == null || unseqResources.isEmpty()) {
      return;
    }
    TreeMap<Long, List<Integer>> orderTimeToIndexMap =
        ascending ? new TreeMap<>() : new TreeMap<>(descendingComparator);
    int index = 0;
    for (TsFileResource resource : unseqResources) {
      orderTimeToIndexMap
          .computeIfAbsent(
              resource.getOrderTimeForUnseq(deviceId, ascending), key -> new ArrayList<>())
          .add(index++);
    }

    index = 0;
    int[] unSeqFileOrderIndexArray = new int[unseqResources.size()];
    for (List<Integer> orderIndexes : orderTimeToIndexMap.values()) {
      for (Integer orderIndex : orderIndexes) {
        unSeqFileOrderIndexArray[index++] = orderIndex;
      }
    }
    this.unSeqFileOrderIndex = unSeqFileOrderIndexArray;
  }

  public boolean isSingleDevice() {
    return isSingleDevice;
  }

  public void setSingleDevice(boolean singleDevice) {
    isSingleDevice = singleDevice;
  }

  public void reset() {
    curSeqIndex = -1;
    curSeqOrderTime = 0;
    curSeqSatisfied = null;
    curUnSeqIndex = -1;
    curUnSeqOrderTime = 0;
    curUnSeqSatisfied = null;
  }

  private boolean isResourceSatisfiedByRuntimeFilter(
      int curIndex, TopKRuntimeFilter filter, boolean isSeq, boolean debug) {
    if (filter == null) {
      return true;
    }
    TsFileResource tsFileResource =
        isSeq ? seqResources.get(curIndex) : unseqResources.get(unSeqFileOrderIndex[curIndex]);
    if (tsFileResource == null) {
      return false;
    }
    // Resource-level RF uses the TsFile's global time range, not per-device bounds.
    long startTime = tsFileResource.getFileStartTime();
    long endTime = tsFileResource.isClosed() ? tsFileResource.getFileEndTime() : Long.MAX_VALUE;
    return filter.mayQualifyRange(startTime, endTime);
  }

  public String getDatabaseName() {
    if (databaseName == null) {
      List<TsFileResource> resources = !seqResources.isEmpty() ? seqResources : unseqResources;
      databaseName = resources.isEmpty() ? null : resources.get(0).getDatabaseName();
    }
    return databaseName;
  }
}
