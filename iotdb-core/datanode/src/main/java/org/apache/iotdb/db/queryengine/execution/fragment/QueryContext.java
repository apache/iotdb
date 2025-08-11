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
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PatternTreeMap;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.storageengine.dataregion.modification.ModEntry;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileID;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.utils.ModificationUtils;
import org.apache.iotdb.db.utils.datastructure.PatternTreeMapFactory;
import org.apache.iotdb.db.utils.datastructure.PatternTreeMapFactory.ModsSerializer;
import org.apache.iotdb.db.utils.datastructure.TVList;

import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.utils.RamUsageEstimator;
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
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/** QueryContext contains the shared information with in a query. */
public class QueryContext {

  private static final Logger LOGGER = LoggerFactory.getLogger(QueryContext.class);
  protected static IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  private QueryStatistics queryStatistics = new QueryStatistics();

  /**
   * The key is the path of a ModificationFile and the value is all Modifications in this file. We
   * use this field because each call of Modification.getModifications() return a copy of the
   * Modifications, and we do not want it to create multiple copies within a query.
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

  // if the mods file does not exist, do not add it to the cache
  protected boolean checkIfModificationExists(TsFileResource tsFileResource) {
    // The exists state of ModificationFile is maintained in memory, and ModificationFile instance
    // is set to the related TsFileResource instance after it is constructed.
    return tsFileResource.anyModFileExists();
  }

  private PatternTreeMap<ModEntry, ModsSerializer> getAllModifications(TsFileResource resource) {
    if (!(this instanceof FragmentInstanceContext)) {
      return fileModCache.computeIfAbsent(
          resource.getTsFileID(), k -> loadAllModifications(resource));
    }
    FragmentInstanceContext fragmentInstanceContext = (FragmentInstanceContext) this;
    if (fragmentInstanceContext.getSourcePaths().size() == 1) {
      return loadAllModifications(resource);
    }

    AtomicReference<PatternTreeMap<ModEntry, ModsSerializer>> atomicReference =
        new AtomicReference<>();
    PatternTreeMap<ModEntry, ModsSerializer> cachedResult =
        fileModCache.computeIfAbsent(
            resource.getTsFileID(),
            k -> {
              PatternTreeMap<ModEntry, ModsSerializer> allMods = loadAllModifications(resource);
              atomicReference.set(allMods);
              if (cachedModEntriesSize.get() >= config.getModsCacheSizeLimitPerFI()) {
                return null;
              }
              long memCost = RamUsageEstimator.sizeOfObject(allMods);
              long alreadyUsedMemoryForCachedModEntries = cachedModEntriesSize.get();
              while (alreadyUsedMemoryForCachedModEntries + memCost
                  < config.getModsCacheSizeLimitPerFI()) {
                if (cachedModEntriesSize.compareAndSet(
                    alreadyUsedMemoryForCachedModEntries,
                    alreadyUsedMemoryForCachedModEntries + memCost)) {
                  fragmentInstanceContext
                      .getMemoryReservationContext()
                      .reserveMemoryCumulatively(memCost);
                  return allMods;
                }
                alreadyUsedMemoryForCachedModEntries = cachedModEntriesSize.get();
              }
              return null;
            });
    return cachedResult == null ? atomicReference.get() : cachedResult;
  }

  private PatternTreeMap<ModEntry, ModsSerializer> loadAllModifications(TsFileResource resource) {
    PatternTreeMap<ModEntry, ModsSerializer> modifications =
        PatternTreeMapFactory.getModsPatternTreeMap();
    for (ModEntry modification : resource.getAllModEntries()) {
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

    List<ModEntry> modEntries =
        getAllModifications(tsFileResource).getOverlapped(deviceID, measurement);
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
    List<ModEntry> modEntries =
        getAllModifications(tsFileResource).getOverlapped(new PartialPath(deviceID));
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
