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
package org.apache.iotdb.db.query.control;

import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * QueryFileManager records the paths of files that every query uses for QueryResourceManager.
 *
 * <p>
 */
public class QueryFileManager {

  /** Map<queryId, Set<filePaths>> */
  private Map<Long, Set<TsFileResource>> sealedFilePathsMap;

  private Map<Long, Set<TsFileResource>> unsealedFilePathsMap;

  QueryFileManager() {
    sealedFilePathsMap = new ConcurrentHashMap<>();
    unsealedFilePathsMap = new ConcurrentHashMap<>();
  }

  /**
   * Set job id for current request thread. When a query request is created firstly, this method
   * must be invoked.
   */
  void addQueryId(long queryId) {
    sealedFilePathsMap.computeIfAbsent(queryId, x -> new HashSet<>());
    unsealedFilePathsMap.computeIfAbsent(queryId, x -> new HashSet<>());
  }

  /** Add the unique file paths to sealedFilePathsMap and unsealedFilePathsMap. */
  public void addUsedFilesForQuery(long queryId, QueryDataSource dataSource) {

    // sequence data
    addUsedFilesForQuery(queryId, dataSource.getSeqResources());

    // unsequence data
    addUsedFilesForQuery(queryId, dataSource.getUnseqResources());
  }

  private void addUsedFilesForQuery(long queryId, List<TsFileResource> resources) {
    Iterator<TsFileResource> iterator = resources.iterator();
    while (iterator.hasNext()) {
      TsFileResource tsFileResource = iterator.next();
      boolean isClosed = tsFileResource.isClosed();
      addFilePathToMap(queryId, tsFileResource, isClosed);

      // this file may be deleted just before we lock it
      if (tsFileResource.isDeleted()) {
        Map<Long, Set<TsFileResource>> pathMap =
            !isClosed ? unsealedFilePathsMap : sealedFilePathsMap;
        // This resource may be removed by other threads of this query.
        if (pathMap.get(queryId).remove(tsFileResource)) {
          FileReaderManager.getInstance().decreaseFileReaderReference(tsFileResource, isClosed);
        }
        iterator.remove();
      }
    }
  }

  /**
   * Whenever the jdbc request is closed normally or abnormally, this method must be invoked. All
   * file paths used by this jdbc request must be cleared and thus the usage reference must be
   * decreased.
   */
  void removeUsedFilesForQuery(long queryId) {
    sealedFilePathsMap.computeIfPresent(
        queryId,
        (k, v) -> {
          for (TsFileResource tsFile : v) {
            FileReaderManager.getInstance().decreaseFileReaderReference(tsFile, true);
          }
          return null;
        });
    unsealedFilePathsMap.computeIfPresent(
        queryId,
        (k, v) -> {
          for (TsFileResource tsFile : v) {
            FileReaderManager.getInstance().decreaseFileReaderReference(tsFile, false);
          }
          return null;
        });
  }

  /**
   * Increase the usage reference of filePath of job id. Before the invoking of this method, <code>
   * this.setqueryIdForCurrentRequestThread</code> has been invoked, so <code>
   * sealedFilePathsMap.get(queryId)</code> or <code>unsealedFilePathsMap.get(queryId)</code> must
   * not return null.
   */
  void addFilePathToMap(long queryId, TsFileResource tsFile, boolean isClosed) {
    Map<Long, Set<TsFileResource>> pathMap = isClosed ? sealedFilePathsMap : unsealedFilePathsMap;
    // TODO this is not an atomic operation, is there concurrent problem?
    if (!pathMap.get(queryId).contains(tsFile)) {
      pathMap.get(queryId).add(tsFile);
      FileReaderManager.getInstance().increaseFileReaderReference(tsFile, isClosed);
    }
  }
}
