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

import org.apache.iotdb.db.engine.filenode.FileNodeManager;
import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
import org.apache.iotdb.db.exception.FileNodeManagerException;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.expression.impl.SingleSeriesExpression;

/**
 * <p>
 * This class is used to get query data source of a given path. See the component of <code>QueryDataSource</code>
 */
public class QueryDataSourceManager {

  private static FileNodeManager fileNodeManager = FileNodeManager.getInstance();

  private QueryDataSourceManager() {
  }

  public static QueryDataSource getQueryDataSource(long jobId, Path selectedPath)
      throws FileNodeManagerException {

    SingleSeriesExpression singleSeriesExpression = new SingleSeriesExpression(selectedPath, null);
    QueryDataSource queryDataSource = fileNodeManager.query(singleSeriesExpression);

    // add used files to current thread request cached map
    OpenedFilePathsManager.getInstance()
        .addUsedFilesForCurrentRequestThread(jobId, queryDataSource);

    return queryDataSource;
  }
}
