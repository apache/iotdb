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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iotdb.db.protocol.thrift.impl;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.db.i18n.DataNodeMiscMessages;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.function.Function;

final class InternalSchemaTaskExecutor {
  private static final Logger LOGGER = LoggerFactory.getLogger(InternalSchemaTaskExecutor.class);

  private InternalSchemaTaskExecutor() {}

  static TSStatus executeSchemaBlackListTask(
      final List<TConsensusGroupId> consensusGroupIdList,
      final Function<TConsensusGroupId, TSStatus> executeOnOneRegion,
      final ExecutorService executor) {
    final List<TSStatus> statusList =
        executeTasksInOrder(consensusGroupIdList, executeOnOneRegion, executor);
    if (statusList.stream()
        .anyMatch(
            status ->
                status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()
                    && status.getCode() != TSStatusCode.ONLY_LOGICAL_VIEW.getStatusCode())) {
      return RpcUtils.getStatus(statusList);
    }
    return statusList.stream()
        .filter(status -> status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode())
        .findFirst()
        .orElse(new TSStatus(TSStatusCode.ONLY_LOGICAL_VIEW.getStatusCode()));
  }

  static TSStatus executeInternalSchemaTask(
      final List<TConsensusGroupId> consensusGroupIdList,
      final Function<TConsensusGroupId, TSStatus> executeOnOneRegion,
      final ExecutorService executor) {
    final List<TSStatus> statusList =
        executeTasksInOrder(consensusGroupIdList, executeOnOneRegion, executor);
    return statusList.stream()
            .allMatch(status -> status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode())
        ? RpcUtils.SUCCESS_STATUS
        : RpcUtils.getStatus(statusList);
  }

  private static List<TSStatus> executeTasksInOrder(
      final List<TConsensusGroupId> consensusGroupIdList,
      final Function<TConsensusGroupId, TSStatus> executeOnOneRegion,
      final ExecutorService executor) {
    final List<Future<TSStatus>> schemaFutures = new ArrayList<>(consensusGroupIdList.size());
    for (final TConsensusGroupId consensusGroupId : consensusGroupIdList) {
      schemaFutures.add(
          executor.submit(
              () -> Objects.requireNonNull(executeOnOneRegion.apply(consensusGroupId))));
    }

    final List<TSStatus> statusList = new ArrayList<>(schemaFutures.size());
    boolean interrupted = false;
    for (final Future<TSStatus> future : schemaFutures) {
      while (true) {
        try {
          statusList.add(future.get());
          break;
        } catch (final InterruptedException e) {
          interrupted = true;
        } catch (final ExecutionException e) {
          LOGGER.warn(DataNodeMiscMessages.EXCEPTION_EXECUTING_INTERNAL_SCHEMA_TASK, e);
          statusList.add(
              new TSStatus(TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode())
                  .setMessage(e.toString()));
          break;
        }
      }
    }
    if (interrupted) {
      Thread.currentThread().interrupt();
    }
    return statusList;
  }
}
