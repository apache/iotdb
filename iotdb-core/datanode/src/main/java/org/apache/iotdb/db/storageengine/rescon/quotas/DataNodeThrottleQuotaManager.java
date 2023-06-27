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

package org.apache.iotdb.db.storageengine.rescon.quotas;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.common.rpc.thrift.TSetThrottleQuotaReq;
import org.apache.iotdb.commons.exception.RpcThrottlingException;
import org.apache.iotdb.confignode.rpc.thrift.TThrottleQuotaResp;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.queryengine.plan.execution.config.executor.ClusterConfigTaskExecutor;
import org.apache.iotdb.db.queryengine.plan.statement.Statement;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataNodeThrottleQuotaManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(DataNodeThrottleQuotaManager.class);

  private ThrottleQuotaLimit throttleQuotaLimit;

  public DataNodeThrottleQuotaManager() {
    throttleQuotaLimit = new ThrottleQuotaLimit();
    recover();
  }

  /** Singleton */
  private static class DataNodeThrottleQuotaManagerHolder {
    private static final DataNodeThrottleQuotaManager INSTANCE = new DataNodeThrottleQuotaManager();

    private DataNodeThrottleQuotaManagerHolder() {}
  }

  public static DataNodeThrottleQuotaManager getInstance() {
    return DataNodeThrottleQuotaManager.DataNodeThrottleQuotaManagerHolder.INSTANCE;
  }

  public TSStatus setThrottleQuota(TSetThrottleQuotaReq req) {
    throttleQuotaLimit.setQuotas(req);
    return RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS);
  }

  public ThrottleQuotaLimit getThrottleQuotaLimit() {
    return throttleQuotaLimit;
  }

  public void setThrottleQuotaLimit(ThrottleQuotaLimit throttleQuotaLimit) {
    this.throttleQuotaLimit = throttleQuotaLimit;
  }

  /**
   * Check the quota for the current (rpc-context) user. Returns the OperationQuota used to get the
   * available quota and to report the data/usage of the operation.
   *
   * @param userName the region where the operation will be performed
   * @return the OperationQuota
   * @throws RpcThrottlingException if the operation cannot be executed due to quota exceeded.
   */
  public OperationQuota checkQuota(String userName, Statement s) throws RpcThrottlingException {
    if (!IoTDBDescriptor.getInstance().getConfig().isQuotaEnable()) {
      return NoopOperationQuota.get();
    }
    switch (s.getType()) {
      case INSERT:
      case BATCH_INSERT:
      case BATCH_INSERT_ONE_DEVICE:
      case BATCH_INSERT_ROWS:
      case MULTI_BATCH_INSERT:
        return checkQuota(userName, 1, 0, s);
      case QUERY:
      case GROUP_BY_TIME:
      case QUERY_INDEX:
      case AGGREGATION:
      case UDAF:
      case UDTF:
      case LAST:
      case FILL:
      case GROUP_BY_FILL:
      case SELECT_INTO:
        return checkQuota(userName, 0, 1, s);
      default:
        return NoopOperationQuota.get();
    }
  }

  /**
   * Check the quota for the current (rpc-context) user. Returns the OperationQuota used to get the
   * available quota and to report the data/usage of the operation.
   *
   * @param userName userName of the current user
   * @param numWrites number of writes to perform
   * @param numReads number of short-reads to perform
   * @return the OperationQuota
   * @throws RpcThrottlingException if the operation cannot be executed due to quota exceeded.
   */
  private OperationQuota checkQuota(String userName, int numWrites, int numReads, Statement s)
      throws RpcThrottlingException {
    OperationQuota quota = getQuota(userName);
    quota.checkQuota(numWrites, numReads, s);
    return quota;
  }

  /**
   * Returns the quota for an operation.
   *
   * @param userName login user
   * @return the OperationQuota
   */
  private OperationQuota getQuota(String userName) {
    QuotaLimiter userLimiter = throttleQuotaLimit.getUserLimiter(userName);
    if (userLimiter != null) {
      return new DefaultOperationQuota(userLimiter);
    }
    return NoopOperationQuota.get();
  }

  private void recover() {
    TThrottleQuotaResp throttleQuota = ClusterConfigTaskExecutor.getInstance().getThrottleQuota();
    if (throttleQuota.getStatus() != null) {
      if (throttleQuota.getStatus().getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()
          && throttleQuota.getThrottleQuota() != null) {
        for (String userName : throttleQuota.getThrottleQuota().keySet()) {
          TSetThrottleQuotaReq req = new TSetThrottleQuotaReq();
          req.setUserName(userName);
          req.setThrottleQuota(throttleQuota.getThrottleQuota().get(userName));
          setThrottleQuota(req);
        }
      }
      LOGGER.info("Throttle quota limit restored successfully. " + throttleQuota);
    } else {
      LOGGER.info("Throttle quota limit restored failed. " + throttleQuota);
    }
  }
}
