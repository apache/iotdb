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
import org.apache.iotdb.db.queryengine.common.header.DatasetHeader;

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

  boolean isQuery();

  boolean isUserQuery();

  String getQueryId();

  long getStartExecutionTime();

  void recordExecutionTime(long executionTime);

  /**
   * update current rpc start time, which is used to calculate rpc execution time and update total
   * execution time
   *
   * @param startTime start time of current rpc, time unit is ns
   */
  void updateCurrentRpcStartTime(long startTime);

  /**
   * @return cost time in ns
   */
  long getTotalExecutionTime();

  /** return ip for a thrift-based client, client-id for MQTT/REST client */
  String getClientHostname();

  long getTimeout();

  Optional<String> getExecuteSQL();

  String getStatementType();
}
