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
import org.apache.iotdb.commons.path.PatternTreeMap;
import org.apache.iotdb.db.storageengine.dataregion.modification.ModEntry;
import org.apache.iotdb.db.storageengine.dataregion.modification.TableDeletionEntry;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileID;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.utils.ModificationUtils;
import org.apache.iotdb.db.utils.datastructure.PatternTreeMapFactory;
import org.apache.iotdb.db.utils.datastructure.PatternTreeMapFactory.ModsSerializer;
import org.apache.iotdb.db.utils.datastructure.TVList;

import org.apache.tsfile.file.metadata.IDeviceID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/** QueryContext contains the shared information with in a query. */
public class QueryContext {

  private static final Logger LOGGER = LoggerFactory.getLogger(QueryContext.class);
  private QueryStatistics queryStatistics = new QueryStatistics();

  /**
   * The key is TsFileID and the value is all Modifications in this file. We use this field because
   * each call of Modification.getModifications() return a copy of the Modifications, and we do not
   * want it to create multiple copies within a query.
   */
  protected Map<TsFileID, PatternTreeMap<ModEntry, ModsSerializer>> fileModCache =
      new ConcurrentHashMap<>();

  protected AtomicLong cachedModEntriesSize = new AtomicLong(0);

  protected long queryId;

  private boolean debug;

  private long startTime;
  private long timeout;

  private volatile boolean isInterrupted = false;

  // for table model, it will be false
  // for tree model, it will be true
  private boolean ignoreAllNullRows = true;

  // referenced TVLists for the query
  protected final Set<TVList> tvListSet = new HashSet<>();

  protected Set<String> tables;

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

  // Only used for query with table data(Tree view is not included)
  public boolean collectTable(String table) {
    // In the current version (2025.08.14), there is only one table under one FI
    // Therefore, SingletonSet is initially used here for better performance
    if (tables == null) {
      tables = Collections.singleton(table);
      return true;
    }
    if (!(tables instanceof HashSet)) {
      tables = new HashSet<>(tables);
    }
    return tables.add(table);
  }

  // if the mods file does not exist, do not add it to the cache
  protected boolean checkIfModificationExists(TsFileResource tsFileResource) {
    // The exists state of ModificationFile is maintained in memory, and ModificationFile instance
    // is set to the related TsFileResource instance after it is constructed.
    return tsFileResource.anyModFileExists();
  }

  protected PatternTreeMap<ModEntry, ModsSerializer> getAllModifications(TsFileResource resource) {
    return fileModCache.computeIfAbsent(
        resource.getTsFileID(), k -> loadAllModificationsFromDisk(resource));
  }

  public PatternTreeMap<ModEntry, ModsSerializer> loadAllModificationsFromDisk(
      TsFileResource resource) {
    PatternTreeMap<ModEntry, ModsSerializer> modifications =
        PatternTreeMapFactory.getModsPatternTreeMap();
    TsFileResource.ModIterator modEntryIterator = resource.getModEntryIterator();
    while (modEntryIterator.hasNext()) {
      ModEntry modification = modEntryIterator.next();
      if (tables != null && modification instanceof TableDeletionEntry) {
        String tableName = ((TableDeletionEntry) modification).getTableName();
        if (!tables.contains(tableName)) {
          continue;
        }
      }
      modifications.append(modification.keyOfPatternTree(), modification);
    }
    return modifications;
  }

  public List<ModEntry> getPathModifications(
      TsFileResource tsFileResource, IDeviceID deviceID, String measurement) {
    // if the mods file does not exist, do not add it to the cache
    if (!checkIfModificationExists(tsFileResource)) {
      return Collections.emptyList();
    }

    return getPathModifications(getAllModifications(tsFileResource), deviceID, measurement);
  }

  public List<ModEntry> getPathModifications(
      PatternTreeMap<ModEntry, ModsSerializer> fileModEntries,
      IDeviceID deviceID,
      String measurement) {
    if (fileModEntries == null) {
      return Collections.emptyList();
    }
    List<ModEntry> modEntries = fileModEntries.getOverlapped(deviceID, measurement);
    if (deviceID.isTableModel()) {
      // the pattern tree has false-positive for table model deletion, so we do a further
      //     filtering
      modEntries =
          modEntries.stream()
              .filter(mod -> mod.affects(deviceID) && mod.affects(measurement))
              .collect(Collectors.toList());
    }
    modEntries = ModificationUtils.sortAndMerge(modEntries);

    return modEntries;
  }

  public List<ModEntry> getPathModifications(TsFileResource tsFileResource, IDeviceID deviceID)
      throws IllegalPathException {
    // if the mods file does not exist, do not add it to the cache
    if (!checkIfModificationExists(tsFileResource)) {
      return Collections.emptyList();
    }
    return getPathModifications(getAllModifications(tsFileResource), deviceID);
  }

  public List<ModEntry> getPathModifications(
      PatternTreeMap<ModEntry, ModsSerializer> fileModEntries, IDeviceID deviceID)
      throws IllegalPathException {
    if (fileModEntries == null) {
      return Collections.emptyList();
    }
    List<ModEntry> modEntries =
        fileModEntries.getOverlapped(deviceID, AlignedPath.VECTOR_PLACEHOLDER);
    if (deviceID.isTableModel()) {
      // the pattern tree has false-positive for table model deletion, so we do a further
      //     filtering
      modEntries =
          modEntries.stream().filter(mod -> mod.affects(deviceID)).collect(Collectors.toList());
    }
    modEntries = ModificationUtils.sortAndMerge(modEntries);
    return modEntries;
  }

  /**
   * Find the modifications of all aligned 'paths' in 'modFile'. If they are not in the cache, read
   * them from 'modFile' and put then into the cache.
   */
  public List<List<ModEntry>> getPathModifications(
      TsFileResource tsFileResource, IDeviceID deviceID, List<String> measurementList) {
    int n = measurementList.size();
    List<List<ModEntry>> ans = new ArrayList<>(n);
    for (String s : measurementList) {
      ans.add(getPathModifications(tsFileResource, deviceID, s));
    }
    return ans;
  }

  public long getQueryId() {
    return queryId;
  }

  public boolean isDebug() {
    return debug;
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

  public boolean isIgnoreAllNullRows() {
    return ignoreAllNullRows;
  }

  public void setIgnoreAllNullRows(boolean ignoreAllNullRows) {
    this.ignoreAllNullRows = ignoreAllNullRows;
  }

  public void addTVListToSet(Map<TVList, Integer> tvListMap) {
    tvListSet.addAll(tvListMap.keySet());
  }
}
