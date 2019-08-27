/**
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

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.engine.querycontext.QueryDataSource;

/**
 * <p>
 * JobFileManager records the paths of files that every query job uses for QueryResourceManager.
 * <p>
 */
public class JobFileManager {

  /**
   * Map<jobId, Set<filePaths>>
   */
  private ConcurrentHashMap<Long, Set<TsFileResource>> sealedFilePathsMap;
  private ConcurrentHashMap<Long, Set<TsFileResource>> unsealedFilePathsMap;

  public JobFileManager() {
    sealedFilePathsMap = new ConcurrentHashMap<>();
    unsealedFilePathsMap = new ConcurrentHashMap<>();
  }

  /**
   * Set job id for current request thread. When a query request is created firstly,
   * this method must be invoked.
   */
  public void addJobId(long jobId) {
    sealedFilePathsMap.computeIfAbsent(jobId, x -> new HashSet<>());
    unsealedFilePathsMap.computeIfAbsent(jobId, x -> new HashSet<>());
  }


  /**
   * Add the unique file paths to sealedFilePathsMap and unsealedFilePathsMap.
   */
  public void addUsedFilesForGivenJob(long jobId, QueryDataSource dataSource) {

    //sequence data
    for(TsFileResource tsFileResource : dataSource.getSeqResources()){
      addFilePathToMap(jobId, tsFileResource, tsFileResource.isClosed());
    }

    //unsequence data
    for(TsFileResource tsFileResource : dataSource.getUnseqResources()){
      addFilePathToMap(jobId, tsFileResource, tsFileResource.isClosed());
    }
  }

  /**
   * Whenever the jdbc request is closed normally or abnormally, this method must be invoked. All file paths used by
   * this jdbc request must be cleared and thus the usage reference must be decreased.
   */
  void removeUsedFilesForGivenJob(long jobId) {
    Set<TsFileResource> tsFiles = sealedFilePathsMap.get(jobId);
    if (tsFiles != null) {
      for (TsFileResource tsFile : sealedFilePathsMap.get(jobId)) {
        FileReaderManager.getInstance().decreaseFileReaderReference(tsFile, true);
      }
      sealedFilePathsMap.remove(jobId);
    }
    tsFiles = unsealedFilePathsMap.get(jobId);
    if (tsFiles != null) {
      for (TsFileResource tsFile : unsealedFilePathsMap.get(jobId)) {
        FileReaderManager.getInstance().decreaseFileReaderReference(tsFile, false);
      }
      unsealedFilePathsMap.remove(jobId);
    }
  }

  /**
   * Increase the usage reference of filePath of job id. Before the invoking of this method,
   * <code>this.setJobIdForCurrentRequestThread</code> has been invoked,
   * so <code>sealedFilePathsMap.get(jobId)</code> or <code>unsealedFilePathsMap.get(jobId)</code>
   * must not return null.
   */
  void addFilePathToMap(long jobId, TsFileResource tsFile, boolean isClosed) {
    ConcurrentHashMap<Long, Set<TsFileResource>> pathMap = !isClosed ? unsealedFilePathsMap :
        sealedFilePathsMap;
    //TODO this is not an atomic operation, is there concurrent problem?
    if (!pathMap.get(jobId).contains(tsFile)) {
      pathMap.get(jobId).add(tsFile);
      FileReaderManager.getInstance().increaseFileReaderReference(tsFile, isClosed);
    }
  }
}
