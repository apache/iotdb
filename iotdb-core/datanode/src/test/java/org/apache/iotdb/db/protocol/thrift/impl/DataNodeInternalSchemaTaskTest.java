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
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

public class DataNodeInternalSchemaTaskTest {
  @Test
  public void preservesRegionOrderWhenTasksCompleteOutOfOrder() throws Exception {
    final TConsensusGroupId firstRegion = new TConsensusGroupId(TConsensusGroupType.DataRegion, 1);
    final TConsensusGroupId secondRegion = new TConsensusGroupId(TConsensusGroupType.DataRegion, 2);
    final CountDownLatch secondFinished = new CountDownLatch(1);
    final ExecutorService executor = Executors.newFixedThreadPool(2);
    try {
      final TSStatus status =
          InternalSchemaTaskExecutor.executeInternalSchemaTask(
              Arrays.asList(firstRegion, secondRegion),
              region -> {
                if (region.equals(firstRegion)) {
                  try {
                    if (!secondFinished.await(5, TimeUnit.SECONDS)) {
                      throw new IllegalStateException("Second region did not finish");
                    }
                  } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException(e);
                  }
                  return new TSStatus(TSStatusCode.WRITE_PROCESS_ERROR.getStatusCode());
                }
                secondFinished.countDown();
                return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
              },
              executor);

      assertEquals(TSStatusCode.MULTIPLE_ERROR.getStatusCode(), status.getCode());
      final List<TSStatus> subStatus = status.getSubStatus();
      assertEquals(2, subStatus.size());
      assertEquals(TSStatusCode.WRITE_PROCESS_ERROR.getStatusCode(), subStatus.get(0).getCode());
      assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), subStatus.get(1).getCode());
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  public void taskExceptionIsReportedAsRegionFailure() {
    final TConsensusGroupId firstRegion = new TConsensusGroupId(TConsensusGroupType.DataRegion, 1);
    final TConsensusGroupId secondRegion = new TConsensusGroupId(TConsensusGroupType.DataRegion, 2);
    final ExecutorService executor = Executors.newFixedThreadPool(2);
    try {
      final TSStatus status =
          InternalSchemaTaskExecutor.executeInternalSchemaTask(
              Arrays.asList(firstRegion, secondRegion),
              region -> {
                if (region.equals(firstRegion)) {
                  throw new IllegalStateException("Injected region failure");
                }
                return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
              },
              executor);

      assertEquals(TSStatusCode.MULTIPLE_ERROR.getStatusCode(), status.getCode());
      final List<TSStatus> subStatus = status.getSubStatus();
      assertEquals(2, subStatus.size());
      assertEquals(TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode(), subStatus.get(0).getCode());
      assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), subStatus.get(1).getCode());
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  public void schemaBlacklistTaskExceptionIsReportedAsFailure() {
    final TConsensusGroupId firstRegion = new TConsensusGroupId(TConsensusGroupType.DataRegion, 1);
    final TConsensusGroupId secondRegion = new TConsensusGroupId(TConsensusGroupType.DataRegion, 2);
    final ExecutorService executor = Executors.newFixedThreadPool(2);
    try {
      final TSStatus status =
          InternalSchemaTaskExecutor.executeSchemaBlackListTask(
              Arrays.asList(firstRegion, secondRegion),
              region -> {
                if (region.equals(firstRegion)) {
                  throw new IllegalStateException("Injected region failure");
                }
                return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
              },
              executor);

      assertEquals(TSStatusCode.MULTIPLE_ERROR.getStatusCode(), status.getCode());
      final List<TSStatus> subStatus = status.getSubStatus();
      assertEquals(2, subStatus.size());
      assertEquals(TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode(), subStatus.get(0).getCode());
      assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), subStatus.get(1).getCode());
    } finally {
      executor.shutdownNow();
    }
  }
}
