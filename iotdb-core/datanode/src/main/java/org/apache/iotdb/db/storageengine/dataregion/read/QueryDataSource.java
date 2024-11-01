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

  // asc: startTime; desc: endTime
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

  private static final Comparator<Long> descendingComparator = (o1, o2) -> Long.compare(o2, o1);

  public QueryDataSource(List<TsFileResource> seqResources, List<TsFileResource> unseqResources) {
    this.seqResources = seqResources;
    this.unseqResources = unseqResources;
  }

  public QueryDataSource(
      List<TsFileResource> seqResources, List<TsFileResource> unseqResources, String databaseName) {
    this.seqResources = seqResources;
    this.unseqResources = unseqResources;
    this.databaseName = databaseName;
  }

  // used for compaction, because in compaction task(unlike query, each QueryDataSource only serve
  // for one series), we will reuse this object for multi series
  public QueryDataSource(QueryDataSource other) {
    this.seqResources = other.seqResources;
    this.unseqResources = other.unseqResources;
    this.unSeqFileOrderIndex = other.unSeqFileOrderIndex;
    this.databaseName = other.databaseName;
  }

  public List<TsFileResource> getSeqResources() {
    return seqResources;
  }

  public List<TsFileResource> getUnseqResources() {
    return unseqResources;
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
      this.curSeqOrderTime = seqResources.get(curIndex).getOrderTime(deviceID, ascending);
      this.curSeqSatisfied = null;
    }
    return res;
  }

  public boolean isSeqSatisfied(
      IDeviceID deviceID, int curIndex, Filter timeFilter, boolean debug) {
    if (curIndex != this.curSeqIndex) {
      throw new IllegalArgumentException(
          String.format("curIndex %d is not equal to curSeqIndex %d", curIndex, this.curSeqIndex));
    }
    if (curSeqSatisfied == null) {
      TsFileResource tsFileResource = seqResources.get(curSeqIndex);
      curSeqSatisfied =
          tsFileResource != null
              && (isSingleDevice || tsFileResource.isSatisfied(deviceID, timeFilter, true, debug));
    }

    return curSeqSatisfied;
  }

  public long getCurrentSeqOrderTime(int curIndex) {
    if (curIndex != this.curSeqIndex) {
      throw new IllegalArgumentException(
          String.format("curIndex %d is not equal to curSeqIndex %d", curIndex, this.curSeqIndex));
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
          unseqResources.get(unSeqFileOrderIndex[curIndex]).getOrderTime(deviceID, ascending);
      this.curUnSeqSatisfied = null;
    }
    return res;
  }

  public boolean isUnSeqSatisfied(
      IDeviceID deviceID, int curIndex, Filter timeFilter, boolean debug) {
    if (curIndex != this.curUnSeqIndex) {
      throw new IllegalArgumentException(
          String.format(
              "curIndex %d is not equal to curUnSeqIndex %d", curIndex, this.curUnSeqIndex));
    }
    if (curUnSeqSatisfied == null) {
      TsFileResource tsFileResource = unseqResources.get(unSeqFileOrderIndex[curIndex]);
      curUnSeqSatisfied =
          tsFileResource != null
              && (isSingleDevice || tsFileResource.isSatisfied(deviceID, timeFilter, false, debug));
    }

    return curUnSeqSatisfied;
  }

  public long getCurrentUnSeqOrderTime(int curIndex) {
    if (curIndex != this.curUnSeqIndex) {
      throw new IllegalArgumentException(
          String.format(
              "curIndex %d is not equal to curSeqIndex %d", curIndex, this.curUnSeqIndex));
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
          .computeIfAbsent(resource.getOrderTime(deviceId, ascending), key -> new ArrayList<>())
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

  public String getDatabaseName() {
    if (databaseName == null) {
      List<TsFileResource> resources = !seqResources.isEmpty() ? seqResources : unseqResources;
      databaseName = resources.isEmpty() ? null : resources.get(0).getDatabaseName();
    }
    return databaseName;
  }
}
