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

<<<<<<< HEAD
<<<<<<< HEAD
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.consensus.PartitionRegionId;
import org.apache.iotdb.confignode.rpc.thrift.TSetStorageGroupReq;
import org.apache.iotdb.confignode.rpc.thrift.TStorageGroupSchema;
import org.apache.iotdb.db.client.ConfigNodeClient;
import org.apache.iotdb.db.client.ConfigNodeInfo;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.localconfignode.LocalConfigNode;
import org.apache.iotdb.db.mpp.plan.statement.metadata.SetStorageGroupStatement;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.rpc.TSStatusCode;

=======
>>>>>>> abbe779bb7 (add interface)
import com.google.common.util.concurrent.ListenableFuture;
=======
import org.apache.iotdb.confignode.rpc.thrift.TStorageGroupSchema;
>>>>>>> 5ff2b3fc1c (move configTask method to ClusterConfigTaskFetcher and StandsloneConfigTaskFetcher)
import org.apache.iotdb.db.mpp.plan.execution.config.fetcher.IConfigTaskFetcher;
import org.apache.iotdb.db.mpp.plan.statement.metadata.SetStorageGroupStatement;

import com.google.common.util.concurrent.ListenableFuture;

public class SetStorageGroupTask implements IConfigTask {

  private final SetStorageGroupStatement setStorageGroupStatement;

  public SetStorageGroupTask(SetStorageGroupStatement setStorageGroupStatement) {
    this.setStorageGroupStatement = setStorageGroupStatement;
  }

  @Override
<<<<<<< HEAD
  public ListenableFuture<ConfigTaskResult> execute(
<<<<<<< HEAD
      IClientManager<PartitionRegionId, ConfigNodeClient> clientManager) {
=======
<<<<<<< HEAD
      IClientManager<PartitionRegionId, DataNodeToConfigNodeClient> clientManager) {
>>>>>>> 8e3e7554e5 (add interface)
    SettableFuture<ConfigTaskResult> future = SettableFuture.create();
    // TODO:(this judgement needs to be integrated in a high level framework)
    if (config.isClusterMode()) {
      // Construct request using statement
      TStorageGroupSchema storageGroupSchema = constructStorageGroupSchema();
      TSetStorageGroupReq req = new TSetStorageGroupReq(storageGroupSchema);
      try (ConfigNodeClient configNodeClient =
          clientManager.borrowClient(ConfigNodeInfo.partitionRegionId)) {
        // Send request to some API server
        TSStatus tsStatus = configNodeClient.setStorageGroup(req);
        // Get response or throw exception
        if (TSStatusCode.SUCCESS_STATUS.getStatusCode() != tsStatus.getCode()) {
          LOGGER.error(
              "Failed to execute set storage group {} in config node, status is {}.",
              setStorageGroupStatement.getStorageGroupPath(),
              tsStatus);
          future.setException(new StatementExecutionException(tsStatus));
        } else {
          future.set(new ConfigTaskResult(TSStatusCode.SUCCESS_STATUS));
        }
      } catch (TException | IOException e) {
        LOGGER.error("Failed to connect to config node.");
        future.setException(e);
      }
    } else {
      try {
        LocalConfigNode localConfigNode = LocalConfigNode.getInstance();
        localConfigNode.setStorageGroup(setStorageGroupStatement.getStorageGroupPath());
        if (setStorageGroupStatement.getTTL() != null) {
          localConfigNode.setTTL(
              setStorageGroupStatement.getStorageGroupPath(), setStorageGroupStatement.getTTL());
        }
        // schemaReplicationFactor, dataReplicationFactor, timePartitionInterval are ignored
        future.set(new ConfigTaskResult(TSStatusCode.SUCCESS_STATUS));
      } catch (Exception e) {
        LOGGER.error("Failed to set storage group, caused by ", e);
        future.setException(e);
      }
    }
=======
          IConfigTaskFetcher configTaskFetcher) {
>>>>>>> abbe779bb7 (add interface)
=======
  public ListenableFuture<ConfigTaskResult> execute(IConfigTaskFetcher configTaskFetcher) {
>>>>>>> 5ff2b3fc1c (move configTask method to ClusterConfigTaskFetcher and StandsloneConfigTaskFetcher)
    // If the action is executed successfully, return the Future.
    // If your operation is async, you can return the corresponding future directly.
    return configTaskFetcher.setStorageGroup(setStorageGroupStatement);
  }

  /** construct set storage group schema according to statement */
  public static TStorageGroupSchema constructStorageGroupSchema(
      SetStorageGroupStatement setStorageGroupStatement) {
    TStorageGroupSchema storageGroupSchema = new TStorageGroupSchema();
    storageGroupSchema.setName(setStorageGroupStatement.getStorageGroupPath().getFullPath());
    if (setStorageGroupStatement.getTTL() != null) {
      storageGroupSchema.setTTL(setStorageGroupStatement.getTTL());
    }
    if (setStorageGroupStatement.getSchemaReplicationFactor() != null) {
      storageGroupSchema.setSchemaReplicationFactor(
          setStorageGroupStatement.getSchemaReplicationFactor());
    }
    if (setStorageGroupStatement.getDataReplicationFactor() != null) {
      storageGroupSchema.setDataReplicationFactor(
          setStorageGroupStatement.getDataReplicationFactor());
    }
    if (setStorageGroupStatement.getTimePartitionInterval() != null) {
      storageGroupSchema.setTimePartitionInterval(
          setStorageGroupStatement.getTimePartitionInterval());
    }
    return storageGroupSchema;
  }
}
