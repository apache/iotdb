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

package org.apache.iotdb.db.query.context;

import org.apache.iotdb.db.engine.modification.Modification;
import org.apache.iotdb.db.engine.modification.ModificationFile;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.query.control.QueryTimeManager;
import org.apache.iotdb.tsfile.file.metadata.IChunkMetadata;

import java.util.ArrayList;
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
  private Map<String, Map<String, List<Modification>>> filePathModCache = new ConcurrentHashMap<>();
  /**
   * The key is the path of a ModificationFile and the value is all Modifications in this file. We
   * use this field because each call of Modification.getModifications() return a copy of the
   * Modifications, and we do not want it to create multiple copies within a query.
   */
  private Map<String, List<Modification>> fileModCache = new HashMap<>();

  private long queryId;

  private long queryTimeLowerBound = Long.MIN_VALUE;

  private boolean debug;
  private boolean enableTracing = false;

  /**
   * To reduce the cost of memory, we only keep the a certain size statement. For statement whose
   * length is over this, we keep its head and tail.
   */
  private static final int MAX_STATEMENT_LENGTH = 64;

  private long startTime;

  private String statement;

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
    this.statement = statement;
    this.timeout = timeout;
    QueryTimeManager.getInstance().registerQuery(this);
  }

  /**
   * Find the modifications of timeseries 'path' in 'modFile'. If they are not in the cache, read
   * them from 'modFile' and put then into the cache.
   */
  public List<Modification> getPathModifications(ModificationFile modFile, PartialPath path) {
    Map<String, List<Modification>> fileModifications =
        filePathModCache.computeIfAbsent(modFile.getFilePath(), k -> new ConcurrentHashMap<>());
    return fileModifications.computeIfAbsent(
        path.getFullPath(),
        k -> {
          List<Modification> allModifications = fileModCache.get(modFile.getFilePath());
          if (allModifications == null) {
            allModifications = (List<Modification>) modFile.getModifications();
            fileModCache.put(modFile.getFilePath(), allModifications);
          }
          List<Modification> finalPathModifications = new ArrayList<>();
          if (!allModifications.isEmpty()) {
            allModifications.forEach(
                modification -> {
                  if (modification.getPath().matchFullPath(path)) {
                    finalPathModifications.add(modification);
                  }
                });
          }
          return finalPathModifications;
        });
  }

  public long getQueryId() {
    return queryId;
  }

  public boolean isDebug() {
    return debug;
  }

  public boolean isEnableTracing() {
    return enableTracing;
  }

  public void setEnableTracing(boolean enableTracing) {
    this.enableTracing = enableTracing;
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

  public String getStatement() {
    return statement;
  }

  public QueryContext setStartTime(long startTime) {
    this.startTime = startTime;
    return this;
  }

  public void getStatement(String statement) {
    this.statement = statement;
  }

  public long getTimeout() {
    return timeout;
  }

  public QueryContext setTimeout(long timeout) {
    this.timeout = timeout;
    return this;
  }

  public QueryContext setStatement(String statement) {
    if (statement.length() <= 64) {
      this.statement = statement;
    } else {
      this.statement =
          statement.substring(0, MAX_STATEMENT_LENGTH / 2)
              + "..."
              + statement.substring(statement.length() - MAX_STATEMENT_LENGTH / 2);
    }
    return this;
  }

  public void setInterrupted(boolean interrupted) {
    isInterrupted = interrupted;
  }

  public boolean isInterrupted() {
    return isInterrupted;
  }
}
