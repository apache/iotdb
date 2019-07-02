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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
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
 * 2. beginQueryOfGivenQueryPaths - remind StorageEngine that some files are being used
 * 3. (if using filter)beginQueryOfGivenExpression
 *     - remind StorageEngine that some files are being used
 * 4. getQueryDataSource - open files for the job or reuse existing readers.
 * 5. endQueryForGivenJob - putBack the resource used by this job.
 * </p>
 */
public class QueryResourceManager {

  /**
   * Map&lt;jobId, Map&lt;deviceId, List&lt;token&gt;&gt;&gt;.
   *
   * <p>
   * Key of queryTokensMap is job id, value of queryTokensMap is a deviceId-tokenList map, key of
   * the deviceId-tokenList map is device id, value of deviceId-tokenList map is a list of tokens.
   * </p>
   *
   * <p>
   * For example, during a query process Q1, given a query sql <sql>select device_1.sensor_1,
   * device_1.sensor_2, device_2.sensor_1, device_2.sensor_2</sql>, we will invoke
   * <code>StorageEngine.getInstance().beginQuery(device_1)</code> and
   * <code>StorageEngine.getInstance().beginQuery(device_2)</code> both once. Although there
   * exists four paths, but the unique devices are only `device_1` and `device_2`. When invoking
   * <code>StorageEngine.getInstance().beginQuery(device_1)</code>, it returns result token `1`.
   * Similarly,
   * <code>StorageEngine.getInstance().beginQuery(device_2)</code> returns result token `2`.
   *
   * In the meanwhile, another query process Q2 aroused by other client is triggered, whose sql
   * statement is same to Q1. Although <code>StorageEngine.getInstance().beginQuery(device_1)
   * </code>
   * and
   * <code>StorageEngine.getInstance().beginQuery(device_2)</code> will be invoked again, it
   * returns result token `3` and `4` .
   *
   * <code>StorageEngine.getInstance().endQueryForGivenJob(device_1, 1)</code> and
   * <code>StorageEngine.getInstance().endQueryForGivenJob(device_2, 2)</code> must be invoked no matter how
   * query process Q1 exits normally or abnormally. So is Q2,
   * <code>StorageEngine.getInstance().endQueryForGivenJob(device_1, 3)</code> and
   * <code>StorageEngine.getInstance().endQueryForGivenJob(device_2, 4)</code> must be invoked
   *
   * Last but no least, to ensure the correctness of insert process and query process of IoTDB,
   * <code>StorageEngine.getInstance().beginQuery()</code> and
   * <code>StorageEngine.getInstance().endQueryForGivenJob()</code> must be executed rightly.
   * </p>
   */
  private ConcurrentHashMap<Long, ConcurrentHashMap<String, List<Integer>>> queryTokensMap;
  private JobFileManager filePathsManager;
  private AtomicLong maxJobId;
  private QueryResourceManager() {
    queryTokensMap = new ConcurrentHashMap<>();
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
    queryTokensMap.computeIfAbsent(jobId, x -> new ConcurrentHashMap<>());
    filePathsManager.addJobId(jobId);
    return jobId;
  }

  /**
   * Begin query and set query tokens of queryPaths. This method is used for projection
   * calculation.
   */
  public void beginQueryOfGivenQueryPaths(long jobId, List<Path> queryPaths)
      throws StorageEngineException {
    Set<String> deviceIdSet = new HashSet<>();
    queryPaths.forEach(path -> deviceIdSet.add(path.getDevice()));

    for (String deviceId : deviceIdSet) {
      putQueryTokenForCurrentRequestThread(jobId, deviceId,
          StorageEngine.getInstance().beginQuery(deviceId));
    }
  }

  /**
   * Begin query and set query tokens of all paths in expression. This method is used in filter
   * calculation.
   */
  public void beginQueryOfGivenExpression(long jobId, IExpression expression)
      throws StorageEngineException {
    Set<String> deviceIdSet = new HashSet<>();
    getUniquePaths(expression, deviceIdSet);
    for (String deviceId : deviceIdSet) {
      putQueryTokenForCurrentRequestThread(jobId, deviceId,
          StorageEngine.getInstance().beginQuery(deviceId));
    }
  }

  /**
   * Begin query and set query tokens of all filter paths in expression. This method is used in
   * filter calculation.
   * @param remoteDeviceIdSet device id set which can not handle locally
   * Note : the method is for cluster
   */
  public void beginQueryOfGivenExpression(long jobId, IExpression expression,
      Set<String> remoteDeviceIdSet) throws StorageEngineException {
    Set<String> deviceIdSet = new HashSet<>();
    getUniquePaths(expression, deviceIdSet);
    deviceIdSet.removeAll(remoteDeviceIdSet);
    for (String deviceId : deviceIdSet) {
      putQueryTokenForCurrentRequestThread(jobId, deviceId,
          StorageEngine.getInstance().beginQuery(deviceId));
    }
  }


  public QueryDataSource getQueryDataSource(Path selectedPath,
      QueryContext context) throws StorageEngineException {

    SingleSeriesExpression singleSeriesExpression = new SingleSeriesExpression(selectedPath, null);
    QueryDataSource queryDataSource = StorageEngine
        .getInstance().query(singleSeriesExpression, context);

    // add used files to current thread request cached map
    filePathsManager.addUsedFilesForGivenJob(context.getJobId(), queryDataSource);

    return queryDataSource;
  }

  /**
   * Whenever the jdbc request is closed normally or abnormally, this method must be invoked. All
   * query tokens created by this jdbc request must be cleared.
   */
  public void endQueryForGivenJob(long jobId) throws StorageEngineException {
    if (queryTokensMap.get(jobId) == null) {
      // no resource need to be released.
      return;
    }
    for (Map.Entry<String, List<Integer>> entry : queryTokensMap.get(jobId).entrySet()) {
      for (int token : entry.getValue()) {
        StorageEngine.getInstance().endQuery(entry.getKey(), token);
      }
    }
    queryTokensMap.remove(jobId);
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

  private void putQueryTokenForCurrentRequestThread(long jobId, String deviceId, int queryToken) {
    queryTokensMap.get(jobId).computeIfAbsent(deviceId, x -> new ArrayList<>()).add(queryToken);
  }

  private static class QueryTokenManagerHelper {

    private static final QueryResourceManager INSTANCE = new QueryResourceManager();

    private QueryTokenManagerHelper() {
    }
  }
}
