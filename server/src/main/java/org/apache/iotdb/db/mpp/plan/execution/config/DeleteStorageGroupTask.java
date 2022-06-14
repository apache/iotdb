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

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.consensus.PartitionRegionId;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.confignode.rpc.thrift.TDeleteStorageGroupsReq;
import org.apache.iotdb.db.client.ConfigNodeInfo;
import org.apache.iotdb.db.client.DataNodeToConfigNodeClient;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.localconfignode.LocalConfigNode;
import org.apache.iotdb.db.mpp.plan.statement.metadata.DeleteStorageGroupStatement;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.rpc.TSStatusCode;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

public class DeleteStorageGroupTask implements IConfigTask {
  private static final Logger LOGGER = LoggerFactory.getLogger(DeleteStorageGroupTask.class);

  private static IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  private final DeleteStorageGroupStatement deleteStorageGroupStatement;

  public DeleteStorageGroupTask(DeleteStorageGroupStatement deleteStorageGroupStatement) {
    this.deleteStorageGroupStatement = deleteStorageGroupStatement;
  }

  @Override
  public ListenableFuture<ConfigTaskResult> execute(
      IClientManager<PartitionRegionId, DataNodeToConfigNodeClient> clientManager) {
    SettableFuture<ConfigTaskResult> future = SettableFuture.create();
    if (config.isClusterMode()) {
      TDeleteStorageGroupsReq req =
          new TDeleteStorageGroupsReq(deleteStorageGroupStatement.getPrefixPath());
      try (DataNodeToConfigNodeClient client =
          clientManager.borrowClient(ConfigNodeInfo.partitionRegionId)) {
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
    // If the action is executed successfully, return the Future.
    // If your operation is async, you can return the corresponding future directly.
    return future;
  }
}
