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

import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.expression.ExpressionType;
import org.apache.iotdb.tsfile.read.expression.IBinaryExpression;
import org.apache.iotdb.tsfile.read.expression.IExpression;
import org.apache.iotdb.tsfile.read.expression.impl.SingleSeriesExpression;

/**
 * <p>
 * QueryResourceManager manages resource (file streams) used by each query job, and assign Ids to the jobs.
 * During the life cycle of a query, the following methods must be called in strict order:
 * 1. assignJobId - get an Id for the new job.
 * 2. getQueryDataSource - open files for the job or reuse existing readers.
 * 3. endQueryForGivenJob - release the resource used by this job.
 * </p>
 */
public class QueryResourceManager {

  private JobFileManager filePathsManager;
  private AtomicLong maxJobId;
  private QueryResourceManager() {
    filePathsManager = new JobFileManager();
    maxJobId = new AtomicLong(0);
  }

  public static QueryResourceManager getInstance() {
    return QueryTokenManagerHelper.INSTANCE;
  }

  /**
   * Assign a jobId for a new query job. When a query request is created firstly, this method
   * must be invoked.
   */
  public long assignJobId() {
    long jobId = maxJobId.incrementAndGet();
    filePathsManager.addJobId(jobId);
    return jobId;
  }


  public QueryDataSource getQueryDataSource(Path selectedPath,
      QueryContext context) throws StorageEngineException {

    SingleSeriesExpression singleSeriesExpression = new SingleSeriesExpression(selectedPath, null);
    return StorageEngine
        .getInstance().query(singleSeriesExpression, context, filePathsManager);
  }

  /**
   * Whenever the jdbc request is closed normally or abnormally, this method must be invoked. All
   * query tokens created by this jdbc request must be cleared.
   */
  public void endQueryForGivenJob(long jobId) {
    // remove usage of opened file paths of current thread
    filePathsManager.removeUsedFilesForGivenJob(jobId);
  }

  private void getUniquePaths(IExpression expression, Set<String> deviceIdSet) {
    if (expression.getType() == ExpressionType.AND || expression.getType() == ExpressionType.OR) {
      getUniquePaths(((IBinaryExpression) expression).getLeft(), deviceIdSet);
      getUniquePaths(((IBinaryExpression) expression).getRight(), deviceIdSet);
    } else if (expression.getType() == ExpressionType.SERIES) {
      SingleSeriesExpression singleSeriesExp = (SingleSeriesExpression) expression;
      deviceIdSet.add(singleSeriesExp.getSeriesPath().getDevice());
    }
  }

  private static class QueryTokenManagerHelper {

    private static final QueryResourceManager INSTANCE = new QueryResourceManager();

    private QueryTokenManagerHelper() {
    }
  }
}
