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

import org.apache.iotdb.commons.exception.IoTDBException;
import org.apache.iotdb.commons.queryengine.common.SqlDialect;
import org.apache.iotdb.db.queryengine.common.header.DatasetHeader;
import org.apache.iotdb.db.queryengine.plan.analyze.QueryType;

import org.apache.tsfile.read.common.block.TsBlock;

import java.nio.ByteBuffer;
import java.util.Optional;

public interface IQueryExecution {

  void start();

  void stop(Throwable t);

  void stopAndCleanup(Throwable t);

  void cancel();

  ExecutionResult getStatus();

  Optional<TsBlock> getBatchResult() throws IoTDBException;

  Optional<ByteBuffer> getByteBufferBatchResult() throws IoTDBException;

  boolean hasNextResult();

  int getOutputValueColumnCount();

  DatasetHeader getDatasetHeader();

  QueryType getQueryType();

  boolean isQuery();

  boolean isUserQuery();

  String getQueryId();

  // time unit is ms
  long getStartExecutionTime();

  /**
   * @param executionTime time unit should be ns
   */
  void recordExecutionTime(long executionTime);

  /**
   * update current rpc start time, which is used to calculate rpc execution time and update total
   * execution time
   *
   * @param startTime start time of current rpc, time unit is ns
   */
  void updateCurrentRpcStartTime(long startTime);

  /**
   * Check if there is an active RPC for this query. If {@code startTimeOfCurrentRpc == -1}, it
   * means there is no active RPC, otherwise there is an active RPC. An active RPC means that the
   * client is still fetching results and the QueryExecution should not be cleaned up until the RPC
   * finishes. On the other hand, if there is no active RPC, it means that the client has finished
   * fetching results or has not started fetching results yet, and the QueryExecution can be safely
   * cleaned up.
   */
  boolean isActive();

  /**
   * @return cost time in ns
   */
  long getTotalExecutionTime();

  /** the max executing time of query in ms. Unit: millisecond */
  long getTimeout();

  Optional<String> getExecuteSQL();

  String getStatementType();

  SqlDialect getSQLDialect();

  String getUser();

  /** return ip for a thrift-based client, client-id for MQTT/REST client */
  String getClientHostname();

  boolean isDebug();
}
