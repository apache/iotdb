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

package org.apache.iotdb.db.mpp.plan.execution.config;

import org.apache.iotdb.common.rpc.thrift.TFlushReq;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.consensus.PartitionRegionId;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.client.ConfigNodeClient;
import org.apache.iotdb.db.client.ConfigNodeInfo;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.mpp.plan.statement.sys.FlushStatement;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.rpc.TSStatusCode;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class FlushTask implements IConfigTask {

  private static final Logger logger = LoggerFactory.getLogger(FlushTask.class);

  private FlushStatement flushStatement;

  public FlushTask(FlushStatement flushStatement) {
    this.flushStatement = flushStatement;
  }

  @Override
  public ListenableFuture<ConfigTaskResult> execute(
      IClientManager<PartitionRegionId, ConfigNodeClient> clientManager)
      throws InterruptedException {
    SettableFuture<ConfigTaskResult> future = SettableFuture.create();
    TSStatus tsStatus = new TSStatus();
    TFlushReq tFlushReq = new TFlushReq();
    List<String> storageGroups = new ArrayList<>();
    if (flushStatement.getStorageGroupPartitionIds() != null) {
      for (PartialPath partialPath : flushStatement.getStorageGroupPartitionIds().keySet()) {
        storageGroups.add(partialPath.getFullPath());
      }
      tFlushReq.setStorageGroups(storageGroups);
    }
    if (flushStatement.isSeq() != null) {
      tFlushReq.setIsSeq(flushStatement.isSeq().toString());
    }
    IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
    if (flushStatement.isLocal()) {
      tFlushReq.setDataNodeId(config.getDataNodeId());
    } else {
      tFlushReq.setDataNodeId(-1);
    }
    try (ConfigNodeClient client = clientManager.borrowClient(ConfigNodeInfo.partitionRegionId)) {
      // Send request to some API server
      tsStatus = client.flush(tFlushReq);
      // Get response or throw exception
    } catch (IOException | TException e) {
      logger.error("Failed to connect to config node.");
      future.setException(e);
    }
    if (tsStatus.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      future.set(new ConfigTaskResult(TSStatusCode.SUCCESS_STATUS));
    } else {
      future.setException(new StatementExecutionException(tsStatus));
    }
    // If the action is executed successfully, return the Future.
    // If your operation is async, you can return the corresponding future directly.
    return future;
  }
}
