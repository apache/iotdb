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

package org.apache.iotdb.db.mpp.plan.execution.config.executor;

import org.apache.iotdb.common.rpc.thrift.TFlushReq;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.common.rpc.thrift.TSetTTLReq;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.consensus.PartitionRegionId;
import org.apache.iotdb.commons.exception.IoTDBException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.confignode.rpc.thrift.TCountStorageGroupResp;
import org.apache.iotdb.confignode.rpc.thrift.TCreateFunctionReq;
import org.apache.iotdb.confignode.rpc.thrift.TDeleteStorageGroupsReq;
import org.apache.iotdb.confignode.rpc.thrift.TDropFunctionReq;
import org.apache.iotdb.confignode.rpc.thrift.TGetTemplateResp;
import org.apache.iotdb.confignode.rpc.thrift.TSetStorageGroupReq;
import org.apache.iotdb.confignode.rpc.thrift.TShowClusterResp;
import org.apache.iotdb.confignode.rpc.thrift.TShowConfigNodesResp;
import org.apache.iotdb.confignode.rpc.thrift.TShowDataNodesResp;
import org.apache.iotdb.confignode.rpc.thrift.TShowRegionReq;
import org.apache.iotdb.confignode.rpc.thrift.TShowRegionResp;
import org.apache.iotdb.confignode.rpc.thrift.TStorageGroupSchema;
import org.apache.iotdb.confignode.rpc.thrift.TStorageGroupSchemaResp;
import org.apache.iotdb.db.client.ConfigNodeClient;
import org.apache.iotdb.db.client.ConfigNodeInfo;
import org.apache.iotdb.db.client.DataNodeClientPoolFactory;
import org.apache.iotdb.db.localconfignode.LocalConfigNode;
import org.apache.iotdb.db.metadata.template.ClusterTemplateManager;
import org.apache.iotdb.db.metadata.template.Template;
import org.apache.iotdb.db.mpp.plan.execution.config.ConfigTaskResult;
import org.apache.iotdb.db.mpp.plan.execution.config.metadata.CountStorageGroupTask;
import org.apache.iotdb.db.mpp.plan.execution.config.metadata.SetStorageGroupTask;
import org.apache.iotdb.db.mpp.plan.execution.config.metadata.ShowClusterTask;
import org.apache.iotdb.db.mpp.plan.execution.config.metadata.ShowConfigNodesTask;
import org.apache.iotdb.db.mpp.plan.execution.config.metadata.ShowDataNodesTask;
import org.apache.iotdb.db.mpp.plan.execution.config.metadata.ShowRegionTask;
import org.apache.iotdb.db.mpp.plan.execution.config.metadata.ShowStorageGroupTask;
import org.apache.iotdb.db.mpp.plan.execution.config.metadata.ShowTTLTask;
import org.apache.iotdb.db.mpp.plan.execution.config.metadata.template.ShowNodesInSchemaTemplateTask;
import org.apache.iotdb.db.mpp.plan.execution.config.metadata.template.ShowPathSetTemplateTask;
import org.apache.iotdb.db.mpp.plan.execution.config.metadata.template.ShowSchemaTemplateTask;
import org.apache.iotdb.db.mpp.plan.statement.metadata.CountStorageGroupStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.DeleteStorageGroupStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.SetStorageGroupStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.SetTTLStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.ShowDataNodesStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.ShowRegionStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.ShowStorageGroupStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.ShowTTLStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.template.CreateSchemaTemplateStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.template.SetSchemaTemplateStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.template.ShowNodesInSchemaTemplateStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.template.ShowPathSetTemplateStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.template.ShowSchemaTemplateStatement;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.rpc.TSStatusCode;

import com.google.common.util.concurrent.SettableFuture;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ClusterConfigTaskExecutor implements IConfigTaskExecutor {

  private static final Logger LOGGER = LoggerFactory.getLogger(ClusterConfigTaskExecutor.class);

  private static final IClientManager<PartitionRegionId, ConfigNodeClient>
      CONFIG_NODE_CLIENT_MANAGER =
          new IClientManager.Factory<PartitionRegionId, ConfigNodeClient>()
              .createClientManager(new DataNodeClientPoolFactory.ConfigNodeClientPoolFactory());

  private static final class ClusterConfigTaskExecutorHolder {
    private static final ClusterConfigTaskExecutor INSTANCE = new ClusterConfigTaskExecutor();

    private ClusterConfigTaskExecutorHolder() {}
  }

  public static ClusterConfigTaskExecutor getInstance() {
    return ClusterConfigTaskExecutor.ClusterConfigTaskExecutorHolder.INSTANCE;
  }

  @Override
  public SettableFuture<ConfigTaskResult> setStorageGroup(
      SetStorageGroupStatement setStorageGroupStatement) {
    SettableFuture<ConfigTaskResult> future = SettableFuture.create();
    // Construct request using statement
    TStorageGroupSchema storageGroupSchema =
        SetStorageGroupTask.constructStorageGroupSchema(setStorageGroupStatement);
    TSetStorageGroupReq req = new TSetStorageGroupReq(storageGroupSchema);
    try (ConfigNodeClient configNodeClient =
        CONFIG_NODE_CLIENT_MANAGER.borrowClient(ConfigNodeInfo.partitionRegionId)) {
      // Send request to some API server
      TSStatus tsStatus = configNodeClient.setStorageGroup(req);
      // Get response or throw exception
      if (TSStatusCode.SUCCESS_STATUS.getStatusCode() != tsStatus.getCode()) {
        LOGGER.error(
            "Failed to execute set storage group {} in config node, status is {}.",
            setStorageGroupStatement.getStorageGroupPath(),
            tsStatus);
        future.setException(new IoTDBException(tsStatus.message, tsStatus.code));
      } else {
        future.set(new ConfigTaskResult(TSStatusCode.SUCCESS_STATUS));
      }
    } catch (TException | IOException e) {
      future.setException(e);
    }
    return future;
  }

  @Override
  public SettableFuture<ConfigTaskResult> showStorageGroup(
      ShowStorageGroupStatement showStorageGroupStatement) {
    SettableFuture<ConfigTaskResult> future = SettableFuture.create();
    Map<String, TStorageGroupSchema> storageGroupSchemaMap;
    // Construct request using statement
    List<String> storageGroupPathPattern =
        Arrays.asList(showStorageGroupStatement.getPathPattern().getNodes());
    try (ConfigNodeClient client =
        CONFIG_NODE_CLIENT_MANAGER.borrowClient(ConfigNodeInfo.partitionRegionId)) {
      // Send request to some API server
      TStorageGroupSchemaResp resp = client.getMatchedStorageGroupSchemas(storageGroupPathPattern);
      storageGroupSchemaMap = resp.getStorageGroupSchemaMap();
      // build TSBlock
      ShowStorageGroupTask.buildTSBlock(storageGroupSchemaMap, future);
    } catch (TException | IOException e) {
      future.setException(e);
    }
    return future;
  }

  @Override
  public SettableFuture<ConfigTaskResult> countStorageGroup(
      CountStorageGroupStatement countStorageGroupStatement) {
    SettableFuture<ConfigTaskResult> future = SettableFuture.create();
    int storageGroupNum;
    List<String> storageGroupPathPattern =
        Arrays.asList(countStorageGroupStatement.getPartialPath().getNodes());
    try (ConfigNodeClient client =
        CONFIG_NODE_CLIENT_MANAGER.borrowClient(ConfigNodeInfo.partitionRegionId)) {
      TCountStorageGroupResp resp = client.countMatchedStorageGroups(storageGroupPathPattern);
      storageGroupNum = resp.getCount();
      // build TSBlock
      CountStorageGroupTask.buildTSBlock(storageGroupNum, future);
    } catch (TException | IOException e) {
      future.setException(e);
    }
    return future;
  }

  @Override
  public SettableFuture<ConfigTaskResult> createFunction(
      String udfName, String className, List<String> uris) {
    SettableFuture<ConfigTaskResult> future = SettableFuture.create();
    try (ConfigNodeClient client =
        CONFIG_NODE_CLIENT_MANAGER.borrowClient(ConfigNodeInfo.partitionRegionId)) {
      final TSStatus executionStatus =
          client.createFunction(new TCreateFunctionReq(udfName, className, uris));

      if (TSStatusCode.SUCCESS_STATUS.getStatusCode() != executionStatus.getCode()) {
        LOGGER.error(
            "[{}] Failed to create function {}({}) in config node, URI: {}.",
            executionStatus,
            udfName,
            className,
            uris);
        future.setException(new IoTDBException(executionStatus.message, executionStatus.code));
      } else {
        future.set(new ConfigTaskResult(TSStatusCode.SUCCESS_STATUS));
      }
    } catch (TException | IOException e) {
      future.setException(e);
    }
    return future;
  }

  @Override
  public SettableFuture<ConfigTaskResult> deleteStorageGroup(
      DeleteStorageGroupStatement deleteStorageGroupStatement) {
    SettableFuture<ConfigTaskResult> future = SettableFuture.create();
    TDeleteStorageGroupsReq req =
        new TDeleteStorageGroupsReq(deleteStorageGroupStatement.getPrefixPath());
    try (ConfigNodeClient client =
        CONFIG_NODE_CLIENT_MANAGER.borrowClient(ConfigNodeInfo.partitionRegionId)) {
      TSStatus tsStatus = client.deleteStorageGroups(req);
      if (TSStatusCode.SUCCESS_STATUS.getStatusCode() != tsStatus.getCode()) {
        LOGGER.error(
            "Failed to execute delete storage group {} in config node, status is {}.",
            deleteStorageGroupStatement.getPrefixPath(),
            tsStatus);
        future.setException(new IoTDBException(tsStatus.getMessage(), tsStatus.getCode()));
      } else {
        future.set(new ConfigTaskResult(TSStatusCode.SUCCESS_STATUS));
      }
    } catch (TException | IOException e) {
      future.setException(e);
    }
    return future;
  }

  @Override
  public SettableFuture<ConfigTaskResult> dropFunction(String udfName) {
    SettableFuture<ConfigTaskResult> future = SettableFuture.create();
    try (ConfigNodeClient client =
        CONFIG_NODE_CLIENT_MANAGER.borrowClient(ConfigNodeInfo.partitionRegionId)) {
      final TSStatus executionStatus = client.dropFunction(new TDropFunctionReq(udfName));

      if (TSStatusCode.SUCCESS_STATUS.getStatusCode() != executionStatus.getCode()) {
        LOGGER.error("[{}] Failed to drop function {} in config node.", executionStatus, udfName);
        future.setException(new IoTDBException(executionStatus.message, executionStatus.code));
      } else {
        future.set(new ConfigTaskResult(TSStatusCode.SUCCESS_STATUS));
      }
    } catch (TException | IOException e) {
      future.setException(e);
    }
    return future;
  }

  @Override
  public SettableFuture<ConfigTaskResult> setTTL(SetTTLStatement setTTLStatement, String taskName) {
    SettableFuture<ConfigTaskResult> future = SettableFuture.create();
    List<String> storageGroupPathPattern =
        Arrays.asList(setTTLStatement.getStorageGroupPath().getNodes());
    TSetTTLReq setTTLReq = new TSetTTLReq(storageGroupPathPattern, setTTLStatement.getTTL());
    try (ConfigNodeClient configNodeClient =
        CONFIG_NODE_CLIENT_MANAGER.borrowClient(ConfigNodeInfo.partitionRegionId)) {
      // Send request to some API server
      TSStatus tsStatus = configNodeClient.setTTL(setTTLReq);
      // Get response or throw exception
      if (TSStatusCode.SUCCESS_STATUS.getStatusCode() != tsStatus.getCode()) {
        LOGGER.error(
            "Failed to execute {} {} in config node, status is {}.",
            taskName,
            setTTLStatement.getStorageGroupPath(),
            tsStatus);
        future.setException(new IoTDBException(tsStatus.getMessage(), tsStatus.getCode()));
      } else {
        future.set(new ConfigTaskResult(TSStatusCode.SUCCESS_STATUS));
      }
    } catch (TException | IOException e) {
      future.setException(e);
    }
    return future;
  }

  @Override
  public SettableFuture<ConfigTaskResult> merge(boolean isCluster) {
    SettableFuture<ConfigTaskResult> future = SettableFuture.create();
    TSStatus tsStatus = new TSStatus();
    if (isCluster) {
      try (ConfigNodeClient client =
          CONFIG_NODE_CLIENT_MANAGER.borrowClient(ConfigNodeInfo.partitionRegionId)) {
        // Send request to some API server
        tsStatus = client.merge();
      } catch (IOException | TException e) {
        future.setException(e);
      }
    } else {
      tsStatus = LocalConfigNode.getInstance().executeMergeOperation();
    }
    if (tsStatus.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      future.set(new ConfigTaskResult(TSStatusCode.SUCCESS_STATUS));
    } else {
      future.setException(new IoTDBException(tsStatus.message, tsStatus.code));
    }
    return future;
  }

  @Override
  public SettableFuture<ConfigTaskResult> flush(TFlushReq tFlushReq, boolean isCluster) {
    SettableFuture<ConfigTaskResult> future = SettableFuture.create();
    TSStatus tsStatus = new TSStatus();
    if (isCluster) {
      try (ConfigNodeClient client =
          CONFIG_NODE_CLIENT_MANAGER.borrowClient(ConfigNodeInfo.partitionRegionId)) {
        // Send request to some API server
        tsStatus = client.flush(tFlushReq);
      } catch (IOException | TException e) {
        future.setException(e);
      }
    } else {
      tsStatus = LocalConfigNode.getInstance().executeFlushOperation(tFlushReq);
    }
    if (tsStatus.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      future.set(new ConfigTaskResult(TSStatusCode.SUCCESS_STATUS));
    } else {
      future.setException(new IoTDBException(tsStatus.message, tsStatus.code));
    }
    return future;
  }

  @Override
  public SettableFuture<ConfigTaskResult> clearCache(boolean isCluster) {
    SettableFuture<ConfigTaskResult> future = SettableFuture.create();
    TSStatus tsStatus = new TSStatus();
    if (isCluster) {
      try (ConfigNodeClient client =
          CONFIG_NODE_CLIENT_MANAGER.borrowClient(ConfigNodeInfo.partitionRegionId)) {
        // Send request to some API server
        tsStatus = client.clearCache();
      } catch (IOException | TException e) {
        future.setException(e);
      }
    } else {
      tsStatus = LocalConfigNode.getInstance().executeClearCacheOperation();
    }
    if (tsStatus.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      future.set(new ConfigTaskResult(TSStatusCode.SUCCESS_STATUS));
    } else {
      future.setException(new IoTDBException(tsStatus.message, tsStatus.code));
    }
    return future;
  }

  @Override
  public SettableFuture<ConfigTaskResult> loadConfiguration(boolean isCluster) {
    SettableFuture<ConfigTaskResult> future = SettableFuture.create();
    TSStatus tsStatus = new TSStatus();
    if (isCluster) {
      try (ConfigNodeClient client =
          CONFIG_NODE_CLIENT_MANAGER.borrowClient(ConfigNodeInfo.partitionRegionId)) {
        // Send request to some API server
        tsStatus = client.loadConfiguration();
      } catch (IOException | TException e) {
        future.setException(e);
      }
    } else {
      tsStatus = LocalConfigNode.getInstance().executeLoadConfigurationOperation();
    }
    if (tsStatus.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      future.set(new ConfigTaskResult(TSStatusCode.SUCCESS_STATUS));
    } else {
      future.setException(new StatementExecutionException(tsStatus));
    }
    return future;
  }

  @Override
  public SettableFuture<ConfigTaskResult> showCluster() {
    SettableFuture<ConfigTaskResult> future = SettableFuture.create();
    TShowClusterResp showClusterResp = new TShowClusterResp();
    try (ConfigNodeClient client =
        CONFIG_NODE_CLIENT_MANAGER.borrowClient(ConfigNodeInfo.partitionRegionId)) {
      showClusterResp = client.showCluster();
    } catch (TException | IOException e) {
      future.setException(e);
    }
    // build TSBlock
    ShowClusterTask.buildTSBlock(showClusterResp, future);
    return future;
  }

  @Override
  public SettableFuture<ConfigTaskResult> showTTL(ShowTTLStatement showTTLStatement) {
    SettableFuture<ConfigTaskResult> future = SettableFuture.create();
    List<PartialPath> storageGroupPaths = showTTLStatement.getPaths();
    Map<String, Long> storageGroupToTTL = new HashMap<>();
    try (ConfigNodeClient client =
        CONFIG_NODE_CLIENT_MANAGER.borrowClient(ConfigNodeInfo.partitionRegionId)) {
      if (showTTLStatement.isAll()) {
        List<String> allStorageGroupPathPattern = Arrays.asList("root", "**");
        TStorageGroupSchemaResp resp =
            client.getMatchedStorageGroupSchemas(allStorageGroupPathPattern);
        for (Map.Entry<String, TStorageGroupSchema> entry :
            resp.getStorageGroupSchemaMap().entrySet()) {
          storageGroupToTTL.put(entry.getKey(), entry.getValue().getTTL());
        }
      } else {
        for (PartialPath storageGroupPath : storageGroupPaths) {
          List<String> storageGroupPathPattern = Arrays.asList(storageGroupPath.getNodes());
          TStorageGroupSchemaResp resp =
              client.getMatchedStorageGroupSchemas(storageGroupPathPattern);
          for (Map.Entry<String, TStorageGroupSchema> entry :
              resp.getStorageGroupSchemaMap().entrySet()) {
            if (!storageGroupToTTL.containsKey(entry.getKey())) {
              storageGroupToTTL.put(entry.getKey(), entry.getValue().getTTL());
            }
          }
        }
      }
    } catch (TException | IOException e) {
      future.setException(e);
    }
    // build TSBlock
    ShowTTLTask.buildTSBlock(storageGroupToTTL, future);
    return future;
  }

  @Override
  public SettableFuture<ConfigTaskResult> showRegion(ShowRegionStatement showRegionStatement) {
    SettableFuture<ConfigTaskResult> future = SettableFuture.create();
    TShowRegionResp showRegionResp = new TShowRegionResp();
    TShowRegionReq showRegionReq = new TShowRegionReq();
    showRegionReq.setConsensusGroupType(showRegionStatement.getRegionType());
    if (showRegionStatement.getStorageGroups() == null) {
      showRegionReq.setStorageGroups(null);
    } else {
      showRegionReq.setStorageGroups(
          showRegionStatement.getStorageGroups().stream()
              .map(PartialPath::getFullPath)
              .collect(Collectors.toList()));
    }
    try (ConfigNodeClient client =
        CONFIG_NODE_CLIENT_MANAGER.borrowClient(ConfigNodeInfo.partitionRegionId)) {
      showRegionResp = client.showRegion(showRegionReq);
      if (showRegionResp.getStatus().getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        future.setException(
            new IoTDBException(
                showRegionResp.getStatus().message, showRegionResp.getStatus().code));
        return future;
      }
    } catch (TException | IOException e) {
      future.setException(e);
    }
    // build TSBlock
    ShowRegionTask.buildTSBlock(showRegionResp, future);
    return future;
  }

  @Override
  public SettableFuture<ConfigTaskResult> showDataNodes(
      ShowDataNodesStatement showDataNodesStatement) {
    SettableFuture<ConfigTaskResult> future = SettableFuture.create();
    TShowDataNodesResp showDataNodesResp = new TShowDataNodesResp();
    try (ConfigNodeClient client =
        CONFIG_NODE_CLIENT_MANAGER.borrowClient(ConfigNodeInfo.partitionRegionId)) {
      showDataNodesResp = client.showDataNodes();
      if (showDataNodesResp.getStatus().getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        future.setException(
            new IoTDBException(
                showDataNodesResp.getStatus().message, showDataNodesResp.getStatus().code));
        return future;
      }
    } catch (TException | IOException e) {
      future.setException(e);
    }
    // build TSBlock
    ShowDataNodesTask.buildTSBlock(showDataNodesResp, future);
    return future;
  }

  @Override
  public SettableFuture<ConfigTaskResult> showConfigNodes() {
    SettableFuture<ConfigTaskResult> future = SettableFuture.create();
    TShowConfigNodesResp showConfigNodesResp = new TShowConfigNodesResp();
    try (ConfigNodeClient client =
        CONFIG_NODE_CLIENT_MANAGER.borrowClient(ConfigNodeInfo.partitionRegionId)) {
      showConfigNodesResp = client.showConfigNodes();
      if (showConfigNodesResp.getStatus().getCode()
          != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        future.setException(
            new IoTDBException(
                showConfigNodesResp.getStatus().message, showConfigNodesResp.getStatus().code));
        return future;
      }
    } catch (TException | IOException e) {
      future.setException(e);
    }
    // build TSBlock
    ShowConfigNodesTask.buildTSBlock(showConfigNodesResp, future);
    return future;
  }

  @Override
  public SettableFuture<ConfigTaskResult> createSchemaTemplate(
      CreateSchemaTemplateStatement createSchemaTemplateStatement) {
    SettableFuture<ConfigTaskResult> future = SettableFuture.create();
    // Construct request using statement
    try {
      // Send request to some API server
      TSStatus tsStatus =
          ClusterTemplateManager.getInstance().createSchemaTemplate(createSchemaTemplateStatement);
      // Get response or throw exception
      if (TSStatusCode.SUCCESS_STATUS.getStatusCode() != tsStatus.getCode()) {
        LOGGER.error(
            "Failed to execute create schema template {} in config node, status is {}.",
            createSchemaTemplateStatement.getName(),
            tsStatus);
        future.setException(new IoTDBException(tsStatus.getMessage(), tsStatus.getCode()));
      } else {
        future.set(new ConfigTaskResult(TSStatusCode.SUCCESS_STATUS));
      }
    } catch (Exception e) {
      future.setException(e.getCause());
    }
    return future;
  }

  @Override
  public SettableFuture<ConfigTaskResult> showSchemaTemplate(
      ShowSchemaTemplateStatement showSchemaTemplateStatement) {
    SettableFuture<ConfigTaskResult> future = SettableFuture.create();
    try (ConfigNodeClient configNodeClient =
        CONFIG_NODE_CLIENT_MANAGER.borrowClient(ConfigNodeInfo.partitionRegionId)) {
      // Send request to some API server
      List<Template> templateList = ClusterTemplateManager.getInstance().getAllTemplates();
      // build TSBlock
      ShowSchemaTemplateTask.buildTSBlock(templateList, future);
    } catch (Exception e) {
      future.setException(e);
    }
    return future;
  }

  @Override
  public SettableFuture<ConfigTaskResult> showNodesInSchemaTemplate(
      ShowNodesInSchemaTemplateStatement showNodesInSchemaTemplateStatement) {
    SettableFuture<ConfigTaskResult> future = SettableFuture.create();
    String req = showNodesInSchemaTemplateStatement.getTemplateName();
    TGetTemplateResp tGetTemplateResp = new TGetTemplateResp();
    try (ConfigNodeClient configNodeClient =
        CONFIG_NODE_CLIENT_MANAGER.borrowClient(ConfigNodeInfo.partitionRegionId)) {
      // Send request to some API server
      Template template = ClusterTemplateManager.getInstance().getTemplate(req);
      // build TSBlock
      ShowNodesInSchemaTemplateTask.buildTSBlock(template, future);
    } catch (Exception e) {
      future.setException(e);
    }
    return future;
  }

  @Override
  public SettableFuture<ConfigTaskResult> setSchemaTemplate(
      SetSchemaTemplateStatement setSchemaTemplateStatement) {
    SettableFuture<ConfigTaskResult> future = SettableFuture.create();
    String templateName = setSchemaTemplateStatement.getTemplateName();
    PartialPath path = setSchemaTemplateStatement.getPath();
    try {
      // Send request to some API server
      ClusterTemplateManager.getInstance().setSchemaTemplate(templateName, path);
      // build TSBlock
      future.set(new ConfigTaskResult(TSStatusCode.SUCCESS_STATUS));
    } catch (Exception e) {
      future.setException(e.getCause());
    }
    return future;
  }

  @Override
  public SettableFuture<ConfigTaskResult> showPathSetTemplate(
      ShowPathSetTemplateStatement showPathSetTemplateStatement) {
    SettableFuture<ConfigTaskResult> future = SettableFuture.create();
    String templateName = showPathSetTemplateStatement.getTemplateName();
    try (ConfigNodeClient configNodeClient =
        CONFIG_NODE_CLIENT_MANAGER.borrowClient(ConfigNodeInfo.partitionRegionId)) {
      // Send request to some API server
      List<PartialPath> listPath =
          ClusterTemplateManager.getInstance().getPathsSetTemplate(templateName);
      // build TSBlock
      ShowPathSetTemplateTask.buildTSBlock(listPath, future);
    } catch (Exception e) {
      future.setException(e);
    }
    return future;
  }
}
