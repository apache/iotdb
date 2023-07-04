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

import org.apache.iotdb.commons.path.AlignedPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PatternTreeMap;
import org.apache.iotdb.db.storageengine.dataregion.modification.Modification;
import org.apache.iotdb.db.storageengine.dataregion.modification.ModificationFile;
import org.apache.iotdb.db.utils.datastructure.PatternTreeMapFactory;
import org.apache.iotdb.db.utils.datastructure.PatternTreeMapFactory.ModsSerializer;
import org.apache.iotdb.tsfile.file.metadata.IChunkMetadata;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/** QueryContext contains the shared information with in a query. */
public class QueryContext {

  /**
   * The outer key is the path of a ModificationFile, the inner key in the name of a timeseries and
   * the value is the Modifications of a timeseries in this file.
   */
  private final Map<String, Map<String, List<Modification>>> filePathModCache =
      new ConcurrentHashMap<>();
  /**
   * The key is the path of a ModificationFile and the value is all Modifications in this file. We
   * use this field because each call of Modification.getModifications() return a copy of the
   * Modifications, and we do not want it to create multiple copies within a query.
   */
  private final Map<String, PatternTreeMap<Modification, ModsSerializer>> fileModCache =
      new HashMap<>();

  protected long queryId;

  private long queryTimeLowerBound = Long.MIN_VALUE;

  private boolean debug;

  private long startTime;
  private long timeout;

  private volatile boolean isInterrupted = false;

  public QueryContext() {}

  public QueryContext(long queryId) {
    this(queryId, false, System.currentTimeMillis(), "", 0);
  }

  /** Every time we generate the queryContext, register it to queryTimeManager. */
  public QueryContext(long queryId, boolean debug, long startTime, String statement, long timeout) {
    this.queryId = queryId;
    this.debug = debug;
    this.startTime = startTime;
    this.timeout = timeout;
  }

  /**
   * Find the modifications of timeseries 'path' in 'modFile'. If they are not in the cache, read
   * them from 'modFile' and put then into the cache.
   */
  public List<Modification> getPathModifications(ModificationFile modFile, PartialPath path) {
    // if the mods file does not exist, do not add it to the cache
    if (!modFile.exists()) {
      return Collections.emptyList();
    }
    Map<String, List<Modification>> fileModifications =
        filePathModCache.computeIfAbsent(modFile.getFilePath(), k -> new ConcurrentHashMap<>());
    return fileModifications.computeIfAbsent(
        path.getFullPath(),
        k -> {
          PatternTreeMap<Modification, ModsSerializer> allModifications =
              fileModCache.get(modFile.getFilePath());
          if (allModifications == null) {
            allModifications = PatternTreeMapFactory.getModsPatternTreeMap();
            for (Modification modification : modFile.getModifications()) {
              allModifications.append(modification.getPath(), modification);
            }
            fileModCache.put(modFile.getFilePath(), allModifications);
          }
          return ModificationFile.sortAndMerge(allModifications.getOverlapped(path));
        });
  }

  /**
   * Find the modifications of all aligned 'paths' in 'modFile'. If they are not in the cache, read
   * them from 'modFile' and put then into the cache.
   */
  public List<List<Modification>> getPathModifications(ModificationFile modFile, AlignedPath path) {
    int n = path.getMeasurementList().size();
    List<List<Modification>> ans = new ArrayList<>(n);
    for (int i = 0; i < n; i++) {
      ans.add(getPathModifications(modFile, path.getPathWithMeasurement(i)));
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

  public void setInterrupted(boolean interrupted) {
    isInterrupted = interrupted;
  }

  public boolean isInterrupted() {
    return isInterrupted;
  }
}
