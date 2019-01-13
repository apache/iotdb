/**
 * Copyright © 2019 Apache IoTDB(incubating) (dev@iotdb.apache.org)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.iotdb.db.query.control;

import org.apache.iotdb.db.engine.filenode.FileNodeManager;
import org.apache.iotdb.db.exception.FileNodeManagerException;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.expression.ExpressionType;
import org.apache.iotdb.tsfile.read.expression.IBinaryExpression;
import org.apache.iotdb.tsfile.read.expression.IExpression;
import org.apache.iotdb.tsfile.read.expression.impl.SingleSeriesExpression;
import org.apache.iotdb.db.engine.filenode.FileNodeManager;
import org.apache.iotdb.db.exception.FileNodeManagerException;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.ArrayList;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * <p>
 * Singleton pattern, to manage all query tokens. Each jdbc query request can query multiple series, in the processing
 * of querying different device id, the <code>FileNodeManager.getInstance().beginQuery</code> and
 * <code>FileNodeManager.getInstance().endQuery</code> must be invoked in the beginning and ending of jdbc request.
 *
 */
public class QueryTokenManager {

    /**
     * Each jdbc request has unique jod id, job id is stored in thread local variable jobContainer.
     */
    private ThreadLocal<Long> jobContainer;

    /**
     * Map<jobId, Map<deviceId, List<token>>
     *
     * <p>
     * Key of queryTokensMap is job id, value of queryTokensMap is a deviceId-tokenList map, key of the
     * deviceId-tokenList map is device id, value of deviceId-tokenList map is a list of tokens.
     * </p>
     *
     * <p>
     * For example, during a query process Q1, given a query sql <sql>select device_1.sensor_1, device_1.sensor_2,
     * device_2.sensor_1, device_2.sensor_2</sql>, we will invoke
     * <code>FileNodeManager.getInstance().beginQuery(device_1)</code> and
     * <code>FileNodeManager.getInstance().beginQuery(device_2)</code> both once. Although there exists four paths, but
     * the unique devices are only `device_1` and `device_2`. When invoking
     * <code>FileNodeManager.getInstance().beginQuery(device_1)</code>, it returns result token `1`. Similarly,
     * <code>FileNodeManager.getInstance().beginQuery(device_2)</code> returns result token `2`.
     *
     * In the meanwhile, another query process Q2 aroused by other client is triggered, whose sql statement is same to
     * Q1. Although <code>FileNodeManager.getInstance().beginQuery(device_1)</code> and
     * <code>FileNodeManager.getInstance().beginQuery(device_2)</code> will be invoked again, it returns result token
     * `3` and `4` .
     *
     * <code>FileNodeManager.getInstance().endQuery(device_1, 1)</code> and
     * <code>FileNodeManager.getInstance().endQuery(device_2, 2)</code> must be invoked no matter how query process Q1
     * exits normally or abnormally. So is Q2, <code>FileNodeManager.getInstance().endQuery(device_1, 3)</code> and
     * <code>FileNodeManager.getInstance().endQuery(device_2, 4)</code> must be invoked
     *
     * Last but no least, to ensure the correctness of write process and query process of IoTDB,
     * <code>FileNodeManager.getInstance().beginQuery()</code> and <code>FileNodeManager.getInstance().endQuery() must
     * be executed rightly.
     * </p>
     */
    private ConcurrentHashMap<Long, ConcurrentHashMap<String, List<Integer>>> queryTokensMap;

    /**
     * Set job id for current request thread. When a query request is created firstly, this method must be invoked.
     */
    public void setJobIdForCurrentRequestThread(long jobId) {
        jobContainer.set(jobId);
        queryTokensMap.put(jobId, new ConcurrentHashMap<>());
    }

    /**
     * Begin query and set query tokens of queryPaths. This method is used for projection calculation.
     */
    public void beginQueryOfGivenQueryPaths(long jobId, List<Path> queryPaths) throws FileNodeManagerException {
        Set<String> deviceIdSet = new HashSet<>();
        queryPaths.forEach((path) -> deviceIdSet.add(path.getDevice()));

        for (String deviceId : deviceIdSet) {
            putQueryTokenForCurrentRequestThread(jobId, deviceId, FileNodeManager.getInstance().beginQuery(deviceId));
        }
    }

    /**
     * Begin query and set query tokens of all paths in expression. This method is used in filter calculation.
     */
    public void beginQueryOfGivenExpression(long jobId, IExpression expression) throws FileNodeManagerException {
        Set<String> deviceIdSet = new HashSet<>();
        getUniquePaths(expression, deviceIdSet);
        for (String deviceId : deviceIdSet) {
            putQueryTokenForCurrentRequestThread(jobId, deviceId, FileNodeManager.getInstance().beginQuery(deviceId));
        }
    }

    /**
     * Whenever the jdbc request is closed normally or abnormally, this method must be invoked. All query tokens created
     * by this jdbc request must be cleared.
     */
    public void endQueryForCurrentRequestThread() throws FileNodeManagerException {
        if (jobContainer.get() != null) {
            long jobId = jobContainer.get();
            jobContainer.remove();

            for (Map.Entry<String, List<Integer>> entry : queryTokensMap.get(jobId).entrySet()) {
                for (int token : entry.getValue()) {
                    FileNodeManager.getInstance().endQuery(entry.getKey(), token);
                }
            }
            queryTokensMap.remove(jobId);
        }
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
        if (!queryTokensMap.get(jobId).containsKey(deviceId)) {
            queryTokensMap.get(jobId).put(deviceId, new ArrayList<>());
        }
        queryTokensMap.get(jobId).get(deviceId).add(queryToken);
    }

    private QueryTokenManager() {
        jobContainer = new ThreadLocal<>();
        queryTokensMap = new ConcurrentHashMap<>();
    }

    private static class QueryTokenManagerHelper {
        public static QueryTokenManager INSTANCE = new QueryTokenManager();
    }

    public static QueryTokenManager getInstance() {
        return QueryTokenManagerHelper.INSTANCE;
    }
}
