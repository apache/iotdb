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
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.confignode.rpc.thrift.TDeleteStorageGroupsReq;
import org.apache.iotdb.db.client.ConfigNodeClient;
import org.apache.iotdb.db.client.ConfigNodeInfo;
<<<<<<< HEAD
=======
import org.apache.iotdb.db.client.DataNodeToConfigNodeClient;
=======
>>>>>>> 5ff2b3fc1c (move configTask method to ClusterConfigTaskFetcher and StandsloneConfigTaskFetcher)
>>>>>>> e2a8c6743a (move configTask method to ClusterConfigTaskFetcher and StandsloneConfigTaskFetcher)
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
=======
>>>>>>> ac78689436 (move configTask method to ClusterConfigTaskFetcher and StandsloneConfigTaskFetcher)
import org.apache.iotdb.db.mpp.plan.execution.config.fetcher.IConfigTaskFetcher;
import org.apache.iotdb.db.mpp.plan.statement.metadata.DeleteStorageGroupStatement;

import com.google.common.util.concurrent.ListenableFuture;

public class DeleteStorageGroupTask implements IConfigTask {

  private final DeleteStorageGroupStatement deleteStorageGroupStatement;

  public DeleteStorageGroupTask(DeleteStorageGroupStatement deleteStorageGroupStatement) {
    this.deleteStorageGroupStatement = deleteStorageGroupStatement;
  }

  @Override
<<<<<<< HEAD
  public ListenableFuture<ConfigTaskResult> execute(
      IClientManager<PartitionRegionId, ConfigNodeClient> clientManager) {
    SettableFuture<ConfigTaskResult> future = SettableFuture.create();
    if (config.isClusterMode()) {
      TDeleteStorageGroupsReq req =
          new TDeleteStorageGroupsReq(deleteStorageGroupStatement.getPrefixPath());
      try (ConfigNodeClient client = clientManager.borrowClient(ConfigNodeInfo.partitionRegionId)) {
        TSStatus tsStatus = client.deleteStorageGroups(req);
        if (TSStatusCode.SUCCESS_STATUS.getStatusCode() != tsStatus.getCode()) {
          LOGGER.error(
              "Failed to execute delete storage group {} in config node, status is {}.",
              deleteStorageGroupStatement.getPrefixPath(),
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
        List<PartialPath> deletePathList =
            deleteStorageGroupStatement.getPrefixPath().stream()
                .map(
                    path -> {
                      try {
                        return new PartialPath(path);
                      } catch (IllegalPathException e) {
                        return null;
                      }
                    })
                .collect(Collectors.toList());
        LocalConfigNode.getInstance().deleteStorageGroups(deletePathList);
      } catch (MetadataException e) {
        future.setException(e);
      }
      future.set(new ConfigTaskResult(TSStatusCode.SUCCESS_STATUS));
    }
=======
  public ListenableFuture<ConfigTaskResult> execute(IConfigTaskFetcher configTaskFetcher) {
>>>>>>> 5ff2b3fc1c (move configTask method to ClusterConfigTaskFetcher and StandsloneConfigTaskFetcher)
    // If the action is executed successfully, return the Future.
    // If your operation is async, you can return the corresponding future directly.
    return configTaskFetcher.deleteStorageGroup(deleteStorageGroupStatement);
  }
}
