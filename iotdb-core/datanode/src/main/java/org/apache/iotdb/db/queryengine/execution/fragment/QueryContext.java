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

package org.apache.iotdb.db.queryengine.execution.fragment;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.AlignedPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PatternTreeMap;
import org.apache.iotdb.db.storageengine.dataregion.modification.Modification;
import org.apache.iotdb.db.storageengine.dataregion.modification.ModificationFile;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileID;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.utils.datastructure.PatternTreeMapFactory;
import org.apache.iotdb.db.utils.datastructure.PatternTreeMapFactory.ModsSerializer;

import org.apache.tsfile.file.metadata.IChunkMetadata;
import org.apache.tsfile.file.metadata.IDeviceID;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;

/** QueryContext contains the shared information with in a query. */
public class QueryContext {

  private QueryStatistics queryStatistics = new QueryStatistics();

  /**
   * The key is the path of a ModificationFile and the value is all Modifications in this file. We
   * use this field because each call of Modification.getModifications() return a copy of the
   * Modifications, and we do not want it to create multiple copies within a query.
   */
  private final Map<String, PatternTreeMap<Modification, ModsSerializer>> fileModCache =
      new ConcurrentHashMap<>();

  protected long queryId;

  private long queryTimeLowerBound = Long.MIN_VALUE;

  private boolean debug;

  private long startTime;
  private long timeout;

  private volatile boolean isInterrupted = false;

  private final Set<TsFileID> nonExistentModFiles = new CopyOnWriteArraySet<>();

  public QueryContext() {}

  public QueryContext(long queryId) {
    this(queryId, false, System.currentTimeMillis(), 0);
  }

  /** Every time we generate the queryContext, register it to queryTimeManager. */
  public QueryContext(long queryId, boolean debug, long startTime, long timeout) {
    this.queryId = queryId;
    this.debug = debug;
    this.startTime = startTime;
    this.timeout = timeout;
  }

  private boolean checkIfModificationExists(TsFileResource tsFileResource) {
    if (nonExistentModFiles.contains(tsFileResource.getTsFileID())) {
      return false;
    }

    ModificationFile modFile = tsFileResource.getModFile();
    if (!modFile.exists()) {
      nonExistentModFiles.add(tsFileResource.getTsFileID());
      return false;
    }
    return true;
  }

  private PatternTreeMap<Modification, ModsSerializer> getAllModifications(
      ModificationFile modFile) {
    return fileModCache.computeIfAbsent(
        modFile.getFilePath(),
        k -> {
          PatternTreeMap<Modification, ModsSerializer> modifications =
              PatternTreeMapFactory.getModsPatternTreeMap();
          for (Modification modification : modFile.getModificationsIter()) {
            modifications.append(modification.getPath(), modification);
          }
          return modifications;
        });
  }

  public List<Modification> getPathModifications(
      TsFileResource tsFileResource, IDeviceID deviceID, String measurement)
      throws IllegalPathException {
    // if the mods file does not exist, do not add it to the cache
    if (!checkIfModificationExists(tsFileResource)) {
      return Collections.emptyList();
    }

    return ModificationFile.sortAndMerge(
        getAllModifications(tsFileResource.getModFile())
            .getOverlapped(new PartialPath(deviceID, measurement)));
  }

  public List<Modification> getPathModifications(TsFileResource tsFileResource, IDeviceID deviceID)
      throws IllegalPathException {
    // if the mods file does not exist, do not add it to the cache
    if (!checkIfModificationExists(tsFileResource)) {
      return Collections.emptyList();
    }

    return ModificationFile.sortAndMerge(
        getAllModifications(tsFileResource.getModFile())
            .getDeviceOverlapped(new PartialPath(deviceID)));
  }

  /**
   * Find the modifications of timeseries 'path' in 'modFile'. If they are not in the cache, read
   * them from 'modFile' and put then into the cache.
   */
  public List<Modification> getPathModifications(TsFileResource tsFileResource, PartialPath path) {
    // if the mods file does not exist, do not add it to the cache
    if (!checkIfModificationExists(tsFileResource)) {
      return Collections.emptyList();
    }

    return ModificationFile.sortAndMerge(
        getAllModifications(tsFileResource.getModFile()).getOverlapped(path));
  }

  /**
   * Find the modifications of all aligned 'paths' in 'modFile'. If they are not in the cache, read
   * them from 'modFile' and put then into the cache.
   */
  public List<List<Modification>> getPathModifications(
      TsFileResource tsFileResource, AlignedPath path) {
    int n = path.getMeasurementList().size();
    List<List<Modification>> ans = new ArrayList<>(n);
    for (int i = 0; i < n; i++) {
      ans.add(getPathModifications(tsFileResource, path.getPathWithMeasurement(i)));
    }
    return ans;
  }

  public long getQueryId() {
    return queryId;
  }

  public boolean isDebug() {
    return debug;
  }

  public long getQueryTimeLowerBound() {
    return queryTimeLowerBound;
  }

  public void setQueryTimeLowerBound(long queryTimeLowerBound) {
    this.queryTimeLowerBound = queryTimeLowerBound;
  }

  public boolean chunkNotSatisfy(IChunkMetadata chunkMetaData) {
    return chunkMetaData.getEndTime() < queryTimeLowerBound;
  }

  public long getStartTime() {
    return startTime;
  }

  public QueryContext setStartTime(long startTime) {
    this.startTime = startTime;
    return this;
  }

  public long getTimeout() {
    return timeout;
  }

  public QueryContext setTimeout(long timeout) {
    this.timeout = timeout;
    return this;
  }

  public boolean isInterrupted() {
    return isInterrupted;
  }

  public void setInterrupted(boolean interrupted) {
    isInterrupted = interrupted;
  }

  public QueryStatistics getQueryStatistics() {
    return this.queryStatistics;
  }

  public void setQueryStatistics(QueryStatistics queryStatistics) {
    this.queryStatistics = queryStatistics;
  }
}
