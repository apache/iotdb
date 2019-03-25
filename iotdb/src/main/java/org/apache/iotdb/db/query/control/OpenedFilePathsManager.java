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
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.iotdb.db.engine.filenode.TsFileResource;
import org.apache.iotdb.db.engine.querycontext.OverflowInsertFile;
import org.apache.iotdb.db.engine.querycontext.QueryDataSource;

/**
 * <p>
 * Singleton pattern, to manage all query tokens. Each jdbc request has an unique job id, in this jdbc request,
 * OpenedFilePathsManager manage all the opened files, and store in the set of current job id.
 */
public class OpenedFilePathsManager {

  /**
   * Each jdbc request has an unique jod id, job id is stored in thread local variable jobIdContainer.
   */
  private ThreadLocal<Long> jobIdContainer;

  /**
   * Map<jobId, Set<filePaths>>
   */
  private ConcurrentHashMap<Long, Set<String>> closedFilePathsMap;
  private ConcurrentHashMap<Long, Set<String>> unclosedFilePathsMap;

  private OpenedFilePathsManager() {
    jobIdContainer = new ThreadLocal<>();
    closedFilePathsMap = new ConcurrentHashMap<>();
    unclosedFilePathsMap = new ConcurrentHashMap<>();
  }

  public static OpenedFilePathsManager getInstance() {
    return OpenedFilePathsManagerHelper.INSTANCE;
  }

  /**
   * Set job id for current request thread. When a query request is created firstly, this method must be invoked.
   */
  public void setJobIdForCurrentRequestThread(long jobId) {
    jobIdContainer.set(jobId);
    closedFilePathsMap.put(jobId, new HashSet<>());
    unclosedFilePathsMap.put(jobId, new HashSet<>());
  }

  /**
   * Add the unique file paths to closedFilePathsMap and unclosedFilePathsMap.
   */
  public void addUsedFilesForCurrentRequestThread(long jobId, QueryDataSource dataSource) {
    for (TsFileResource tsFileResource : dataSource.getSeqDataSource().getSealedTsFiles()) {
      String sealedFilePath = tsFileResource.getFilePath();
      addFilePathToMap(jobId, sealedFilePath, true);
    }

    if (dataSource.getSeqDataSource().hasUnsealedTsFile()) {
      String unSealedFilePath = dataSource.getSeqDataSource().getUnsealedTsFile().getFilePath();
      addFilePathToMap(jobId, unSealedFilePath, false);
    }

    for (OverflowInsertFile overflowInsertFile : dataSource.getOverflowSeriesDataSource()
        .getOverflowInsertFileList()) {
      String overflowFilePath = overflowInsertFile.getFilePath();
      // overflow is unclosed by default
      addFilePathToMap(jobId, overflowFilePath, false);
    }
  }

  /**
   * Whenever the jdbc request is closed normally or abnormally, this method must be invoked. All file paths used by
   * this jdbc request must be cleared and thus the usage reference must be decreased.
   */
  public void removeUsedFilesForCurrentRequestThread() {
    if (jobIdContainer.get() != null) {
      long jobId = jobIdContainer.get();
      jobIdContainer.remove();

      for (String filePath : closedFilePathsMap.get(jobId)) {
        FileReaderManager.getInstance().decreaseFileReaderReference(filePath, false);
      }
      closedFilePathsMap.remove(jobId);
      for (String filePath : unclosedFilePathsMap.get(jobId)) {
        FileReaderManager.getInstance().decreaseFileReaderReference(filePath, true);
      }
      unclosedFilePathsMap.remove(jobId);
    }
  }

  /**
   * Increase the usage reference of filePath of job id. Before the invoking of this method,
   * <code>this.setJobIdForCurrentRequestThread</code> has been invoked,
   * so <code>closedFilePathsMap.get(jobId)</code> or <code>unclosedFilePathsMap.get(jobId)</code>
   * must not return null.
   */
  public void addFilePathToMap(long jobId, String filePath, boolean isClosed) {
    ConcurrentHashMap<Long, Set<String>> pathMap = !isClosed ? unclosedFilePathsMap :
        closedFilePathsMap;
    if (!pathMap.get(jobId).contains(filePath)) {
      pathMap.get(jobId).add(filePath);
      FileReaderManager.getInstance().increaseFileReaderReference(filePath, isClosed);
    }
  }

  private static class OpenedFilePathsManagerHelper {
    private static final OpenedFilePathsManager INSTANCE = new OpenedFilePathsManager();

    private OpenedFilePathsManagerHelper() {

    }
  }
}
