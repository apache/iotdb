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
import org.apache.iotdb.common.rpc.thrift.TThrottleQuota;
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.exception.RpcThrottlingException;
import org.apache.iotdb.commons.i18n.AuthMessages;
import org.apache.iotdb.confignode.rpc.thrift.TThrottleQuotaResp;
import org.apache.iotdb.db.auth.AuthorityChecker;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.i18n.StorageEngineMessages;
import org.apache.iotdb.db.queryengine.plan.execution.config.executor.ClusterConfigTaskExecutor;
import org.apache.iotdb.db.queryengine.plan.statement.Statement;
import org.apache.iotdb.db.utils.memory.WriteMemoryEstimator;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

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
    if (!req.isSetUserId()) {
      // Rolling-upgrade fallback: an older ConfigNode broadcasts only userName.
      long resolvedUserId = AuthorityChecker.getUserId(req.getUserName()).orElse(-1L);
      if (resolvedUserId < 0) {
        return RpcUtils.getStatus(
            TSStatusCode.USER_NOT_EXIST,
            String.format(AuthMessages.NO_SUCH_USER, req.getUserName()));
      }
      req.setUserId(resolvedUserId);
    }
    // DROP USER broadcasts an empty throttle (no rate limits, mem=0, cpu=0). That must remove the
    // entry; ordinary SET THROTTLE / USER-QUOTA sync must not, or empty derived fields would wipe
    // existing rate limiters (see ThrottleQuotaLimit.setQuotas).
    if (isDropClearRequest(req)) {
      throttleQuotaLimit.removeQuota(req.getUserId());
    } else {
      throttleQuotaLimit.setQuotas(req);
    }
    return RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS);
  }

  /**
   * True only for the intentional empty broadcast from ConfigNode {@code onUserDropped}. Exact
   * zeros are required so UNLIMITED (-1) or USER-QUOTA sync with unset cpu/mem cannot match.
   */
  static boolean isDropClearRequest(TSetThrottleQuotaReq req) {
    if (req.getThrottleQuota() == null) {
      return true;
    }
    TThrottleQuota quota = req.getThrottleQuota();
    boolean throttleEmpty =
        !quota.isSetThrottleLimit()
            || quota.getThrottleLimit() == null
            || quota.getThrottleLimit().isEmpty();
    return throttleEmpty && quota.getMemLimit() == 0 && quota.getCpuLimit() == 0;
  }

  public ThrottleQuotaLimit getThrottleQuotaLimit() {
    return throttleQuotaLimit;
  }

  public void setThrottleQuotaLimit(ThrottleQuotaLimit throttleQuotaLimit) {
    this.throttleQuotaLimit = throttleQuotaLimit;
  }

  /**
   * Check the quota for the current (rpc-context) user. Returns the {@link OperationQuota} used to
   * get the available quota and to report the data/usage of the operation.
   *
   * @param userName current userName (logging only)
   * @param userId immutable user id (throttle + user resource quota key)
   * @return the {@link OperationQuota}
   * @throws RpcThrottlingException if the operation cannot be executed due to quota exceeded.
   */
  public OperationQuota checkQuota(String userName, long userId, Statement s)
      throws RpcThrottlingException, UserResourceQuotaExceededException {
    if (!IoTDBDescriptor.getInstance().getConfig().isQuotaEnable()) {
      return NoopOperationQuota.get();
    }
    switch (s.getType()) {
      case INSERT:
      case BATCH_INSERT:
      case BATCH_INSERT_ONE_DEVICE:
      case BATCH_INSERT_ROWS:
      case MULTI_BATCH_INSERT:
      case PIPE_ENRICHED:
        return checkQuota(userName, userId, 1, 0, s);
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
        return checkQuota(userName, userId, 0, 1, s);
      default:
        return NoopOperationQuota.get();
    }
  }

  /**
   * Table-model counterpart of {@link #checkQuota(String, long, Statement)}. Inserts use the inner
   * tree statement; {@code Query}/{@code Explain}/{@code ExplainAnalyze} consume read throttle
   * quota (e.g. {@code read_disk_io}); other DDL/DCL statements are not throttled here (same as
   * tree model).
   */
  public OperationQuota checkQuota(
      String userName,
      long userId,
      org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Statement tableStatement)
      throws RpcThrottlingException, UserResourceQuotaExceededException {
    if (!IoTDBDescriptor.getInstance().getConfig().isQuotaEnable() || tableStatement == null) {
      return NoopOperationQuota.get();
    }
    if (tableStatement
        instanceof org.apache.iotdb.db.queryengine.plan.relational.sql.ast.WrappedInsertStatement) {
      return checkQuota(
          userName,
          userId,
          ((org.apache.iotdb.db.queryengine.plan.relational.sql.ast.WrappedInsertStatement)
                  tableStatement)
              .getInnerTreeStatement());
    }
    if (tableStatement
        instanceof org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Query) {
      return checkQuota(userName, userId, 0, 1);
    }
    if (tableStatement instanceof org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Explain) {
      return checkQuota(
          userName,
          userId,
          ((org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Explain) tableStatement)
              .getStatement());
    }
    if (tableStatement
        instanceof org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ExplainAnalyze) {
      return checkQuota(
          userName,
          userId,
          ((org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ExplainAnalyze) tableStatement)
              .getStatement());
    }
    if (tableStatement
        instanceof org.apache.iotdb.db.queryengine.plan.relational.sql.ast.PipeEnriched) {
      return checkQuota(
          userName,
          userId,
          ((org.apache.iotdb.db.queryengine.plan.relational.sql.ast.PipeEnriched) tableStatement)
              .getInnerStatement());
    }
    return NoopOperationQuota.get();
  }

  /** Read-only table Query path; write memory estimate is unused when numWrites==0. */
  private OperationQuota checkQuota(String userName, long userId, int numWrites, int numReads)
      throws RpcThrottlingException, UserResourceQuotaExceededException {
    return checkQuota(userName, userId, numWrites, numReads, null);
  }

  private OperationQuota checkQuota(
      String userName, long userId, int numWrites, int numReads, Statement s)
      throws RpcThrottlingException, UserResourceQuotaExceededException {
    OperationQuota quota = getQuota(userId);
    quota.checkQuota(numWrites, numReads, s);
    if (numWrites > 0
        && IoTDBDescriptor.getInstance().getConfig().isQuotaEnable()
        && userId != IoTDBConstant.SUPER_USER_ID) {
      AcquireContext ctx =
          new AcquireContext()
              .setRequestId(String.valueOf(System.nanoTime()))
              .setStatementType(s == null ? "WRITE" : s.getType().name());
      QuotaTokenBundle bundle =
          UserResourceQuotaManager.getInstance()
              .acquireWriteResources(
                  userId,
                  s == null ? 0L : WriteMemoryEstimator.estimate(s),
                  ctx,
                  AcquirePolicy.defaults());
      return new ResourceAwareOperationQuota(quota, bundle);
    }
    return quota;
  }

  private OperationQuota getQuota(long userId) {
    QuotaLimiter userLimiter = throttleQuotaLimit.getUserLimiter(userId);
    if (userLimiter != null) {
      return new DefaultOperationQuota(userLimiter);
    }
    return NoopOperationQuota.get();
  }

  /** Reload throttle from ConfigNode; prefer userId map, fall back to legacy userName map. */
  private void recover() {
    TThrottleQuotaResp throttleQuota = ClusterConfigTaskExecutor.getInstance().getThrottleQuota();
    if (throttleQuota.getStatus() != null) {
      if (throttleQuota.getStatus().getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        Map<Long, String> nameMap =
            throttleQuota.isSetUserNameMap() && throttleQuota.getUserNameMap() != null
                ? throttleQuota.getUserNameMap()
                : java.util.Collections.emptyMap();
        if (throttleQuota.isSetThrottleQuotaByUserId()
            && throttleQuota.getThrottleQuotaByUserId() != null) {
          for (Map.Entry<Long, TThrottleQuota> entry :
              throttleQuota.getThrottleQuotaByUserId().entrySet()) {
            TSetThrottleQuotaReq req = new TSetThrottleQuotaReq();
            req.setUserId(entry.getKey());
            req.setUserName(nameMap.getOrDefault(entry.getKey(), String.valueOf(entry.getKey())));
            req.setThrottleQuota(entry.getValue());
            setThrottleQuota(req);
          }
        } else if (throttleQuota.getThrottleQuota() != null) {
          // Rolling-upgrade fallback for an older ConfigNode response.
          for (Map.Entry<String, TThrottleQuota> entry :
              throttleQuota.getThrottleQuota().entrySet()) {
            TSetThrottleQuotaReq req = new TSetThrottleQuotaReq();
            req.setUserName(entry.getKey());
            req.setThrottleQuota(entry.getValue());
            setThrottleQuota(req);
          }
        }
      }
      LOGGER.info(StorageEngineMessages.THROTTLE_QUOTA_RESTORED_SUCCESSFULLY + throttleQuota);
    } else {
      LOGGER.info(StorageEngineMessages.THROTTLE_QUOTA_RESTORED_FAILED + throttleQuota);
    }
  }
}
