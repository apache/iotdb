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

package org.apache.iotdb.db.queryengine.plan.execution;

import org.apache.iotdb.commons.queryengine.common.SqlDialect;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.queryengine.common.FragmentInstanceId;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.common.header.DatasetHeader;
import org.apache.iotdb.db.queryengine.execution.exchange.MPPDataExchangeService;
import org.apache.iotdb.db.queryengine.execution.memory.LocalMemoryManager;
import org.apache.iotdb.db.queryengine.execution.memory.MemoryPool;
import org.apache.iotdb.db.queryengine.execution.memory.MemorySide;
import org.apache.iotdb.db.queryengine.plan.analyze.QueryType;
import org.apache.iotdb.mpp.rpc.thrift.TFragmentInstanceId;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.read.common.block.TsBlock;

import java.nio.ByteBuffer;
import java.util.Optional;

/**
 * An already materialized serialized result (e.g. a Fast LAST cache hit) registered as a query
 * execution, so that {@code fetchResultsV2} can continue the fragmented transfer of an oversized
 * block. Like other pending fragments, the serialized bytes are reserved from the query memory pool
 * (best effort) until the last fragment is sent or the query is cleaned up.
 */
public class MaterializedResultExecution implements IQueryExecution {

  private static final String PLAN_NODE_ID = "lastCacheResult";

  private final long queryId;
  private final DatasetHeader datasetHeader;
  private final String statementType;
  private final long startExecutionTime = System.currentTimeMillis();
  private final long timeout;
  private final String user;
  private final String clientHostname;

  private final LocalMemoryManager localMemoryManager;
  private final TFragmentInstanceId fragmentInstanceId;
  private final String fullFragmentInstanceId;

  private final PendingResultFragment pendingResultFragment =
      new PendingResultFragment(this::freeReservedMemory);

  /** The serialized result, handed over once. */
  private ByteBuffer serializedResult;

  private long reservedBytes;

  private volatile long startTimeOfCurrentRpc = System.nanoTime();
  private long totalExecutionTime;
  private boolean cleanedUp;

  public MaterializedResultExecution(
      long queryId,
      ByteBuffer serializedResult,
      DatasetHeader datasetHeader,
      String statementType,
      long timeout,
      String user,
      String clientHostname) {
    this(
        queryId,
        serializedResult,
        datasetHeader,
        statementType,
        timeout,
        user,
        clientHostname,
        MPPDataExchangeService.getInstance().getMPPDataExchangeManager().getLocalMemoryManager());
  }

  @TestOnly
  MaterializedResultExecution(
      long queryId,
      ByteBuffer serializedResult,
      DatasetHeader datasetHeader,
      String statementType,
      long timeout,
      String user,
      String clientHostname,
      LocalMemoryManager localMemoryManager) {
    this.queryId = queryId;
    this.serializedResult = serializedResult;
    this.datasetHeader = datasetHeader;
    this.statementType = statementType;
    this.timeout = timeout;
    this.user = user;
    this.clientHostname = clientHostname;

    this.localMemoryManager = localMemoryManager;
    this.fragmentInstanceId = new TFragmentInstanceId(Long.toString(queryId), 0, PLAN_NODE_ID);
    this.fullFragmentInstanceId =
        FragmentInstanceId.createFragmentInstanceIdFromTFragmentInstanceId(fragmentInstanceId);
    reserveResult();
  }

  /** Best-effort accounting: a result too large for one reservation still gets transferred. */
  private void reserveResult() {
    MemoryPool queryPool = localMemoryManager.getQueryPool();
    queryPool.registerPlanNodeIdToQueryMemoryMap(
        Long.toString(queryId), fullFragmentInstanceId, PLAN_NODE_ID, MemorySide.OUTPUT);
    long maxBytesCanReserve = queryPool.getMaxBytes();
    long bytesToReserve = Math.min(serializedResult.remaining(), maxBytesCanReserve);
    if (queryPool.tryReserve(
        Long.toString(queryId),
        fullFragmentInstanceId,
        PLAN_NODE_ID,
        bytesToReserve,
        maxBytesCanReserve)) {
      reservedBytes = bytesToReserve;
    }
  }

  private synchronized void freeReservedMemory() {
    if (reservedBytes > 0) {
      localMemoryManager
          .getQueryPool()
          .free(Long.toString(queryId), fullFragmentInstanceId, PLAN_NODE_ID, reservedBytes);
      reservedBytes = 0;
    }
  }

  private synchronized void cleanup() {
    if (cleanedUp) {
      return;
    }
    cleanedUp = true;
    pendingResultFragment.clear();
    freeReservedMemory();
    localMemoryManager
        .getQueryPool()
        .deRegisterFragmentInstanceFromQueryMemoryMap(
            Long.toString(queryId), fullFragmentInstanceId, false);
  }

  @Override
  public void start() {
    // nothing to start: the result is already materialized
  }

  @Override
  public void stop(Throwable t) {
    cleanup();
  }

  @Override
  public void stopAndCleanup(Throwable t) {
    cleanup();
  }

  @Override
  public void cancel() {
    cleanup();
  }

  @Override
  public ExecutionResult getStatus() {
    return new ExecutionResult(
        new QueryId(Long.toString(queryId)), RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS));
  }

  @Override
  public Optional<TsBlock> getBatchResult() {
    return Optional.empty();
  }

  @Override
  public Optional<ByteBuffer> getByteBufferBatchResult(boolean retainMemory) {
    ByteBuffer result = serializedResult;
    serializedResult = null;
    return Optional.ofNullable(result);
  }

  @Override
  public PendingResultFragment getPendingResultFragment() {
    return pendingResultFragment;
  }

  @Override
  public boolean hasNextResult() {
    return serializedResult != null;
  }

  @Override
  public int getOutputValueColumnCount() {
    return datasetHeader.getOutputValueColumnCount();
  }

  @Override
  public DatasetHeader getDatasetHeader() {
    return datasetHeader;
  }

  @Override
  public QueryType getQueryType() {
    return QueryType.READ;
  }

  @Override
  public boolean isQuery() {
    return true;
  }

  @Override
  public boolean isUserQuery() {
    return false;
  }

  @Override
  public String getQueryId() {
    return Long.toString(queryId);
  }

  @Override
  public long getStartExecutionTime() {
    return startExecutionTime;
  }

  @Override
  public synchronized void recordExecutionTime(long executionTime) {
    totalExecutionTime += executionTime;
    this.startTimeOfCurrentRpc = -1;
  }

  @Override
  public synchronized void updateCurrentRpcStartTime(long startTime) {
    this.startTimeOfCurrentRpc = startTime;
  }

  @Override
  public synchronized boolean isActive() {
    return startTimeOfCurrentRpc == -1;
  }

  @Override
  public synchronized long getTotalExecutionTime() {
    return totalExecutionTime
        + (startTimeOfCurrentRpc == -1 ? 0 : System.nanoTime() - startTimeOfCurrentRpc);
  }

  @Override
  public long getTimeout() {
    return timeout;
  }

  @Override
  public Optional<String> getExecuteSQL() {
    return Optional.empty();
  }

  @Override
  public String getStatementType() {
    return statementType;
  }

  @Override
  public SqlDialect getSQLDialect() {
    return SqlDialect.TREE;
  }

  @Override
  public String getUser() {
    return user;
  }

  @Override
  public String getClientHostname() {
    return clientHostname;
  }

  @Override
  public boolean isDebug() {
    return false;
  }
}
