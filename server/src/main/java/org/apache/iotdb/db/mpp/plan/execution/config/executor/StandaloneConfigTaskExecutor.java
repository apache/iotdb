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
import org.apache.iotdb.commons.exception.IoTDBException;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.udf.service.UDFExecutableManager;
import org.apache.iotdb.commons.udf.service.UDFRegistrationService;
import org.apache.iotdb.confignode.rpc.thrift.TStorageGroupInfo;
import org.apache.iotdb.confignode.rpc.thrift.TStorageGroupSchema;
import org.apache.iotdb.db.localconfignode.LocalConfigNode;
import org.apache.iotdb.db.metadata.mnode.IStorageGroupMNode;
import org.apache.iotdb.db.mpp.plan.execution.config.ConfigTaskResult;
import org.apache.iotdb.db.mpp.plan.execution.config.metadata.CountStorageGroupTask;
import org.apache.iotdb.db.mpp.plan.execution.config.metadata.ShowStorageGroupTask;
import org.apache.iotdb.db.mpp.plan.execution.config.metadata.ShowTTLTask;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class StandaloneConfigTaskExecutor implements IConfigTaskExecutor {

  private static final Logger LOGGER = LoggerFactory.getLogger(StandaloneConfigTaskExecutor.class);

  private static final class StandaloneConfigTaskExecutorHolder {
    private static final StandaloneConfigTaskExecutor INSTANCE = new StandaloneConfigTaskExecutor();

    private StandaloneConfigTaskExecutorHolder() {}
  }

  public static StandaloneConfigTaskExecutor getInstance() {
    return StandaloneConfigTaskExecutorHolder.INSTANCE;
  }

  @Override
  public SettableFuture<ConfigTaskResult> setStorageGroup(
      SetStorageGroupStatement setStorageGroupStatement) {
    SettableFuture<ConfigTaskResult> future = SettableFuture.create();
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
    return future;
  }

  @Override
  public SettableFuture<ConfigTaskResult> showStorageGroup(
      ShowStorageGroupStatement showStorageGroupStatement) {
    SettableFuture<ConfigTaskResult> future = SettableFuture.create();
    Map<String, TStorageGroupInfo> storageGroupInfoMap = new HashMap<>();
    try {
      LocalConfigNode localConfigNode = LocalConfigNode.getInstance();
      List<PartialPath> partialPaths =
          localConfigNode.getMatchedStorageGroups(
              showStorageGroupStatement.getPathPattern(), showStorageGroupStatement.isPrefixPath());
      for (PartialPath storageGroupPath : partialPaths) {
        IStorageGroupMNode storageGroupMNode =
            localConfigNode.getStorageGroupNodeByPath(storageGroupPath);
        String storageGroup = storageGroupMNode.getFullPath();
        TStorageGroupSchema storageGroupSchema = storageGroupMNode.getStorageGroupSchema();

        TStorageGroupInfo storageGroupInfo = new TStorageGroupInfo();
        storageGroupInfo.setName(storageGroup);
        storageGroupInfo.setTTL(storageGroupSchema.getTTL());
        storageGroupInfo.setSchemaReplicationFactor(
            storageGroupSchema.getSchemaReplicationFactor());
        storageGroupInfo.setDataReplicationFactor(storageGroupSchema.getDataReplicationFactor());
        storageGroupInfo.setTimePartitionInterval(storageGroupSchema.getTimePartitionInterval());
        storageGroupInfo.setSchemaRegionNum(1);
        storageGroupInfo.setDataRegionNum(1);
        storageGroupInfoMap.put(storageGroup, storageGroupInfo);
      }
      // build TSBlock
      ShowStorageGroupTask.buildTSBlock(storageGroupInfoMap, future);
    } catch (MetadataException e) {
      future.setException(e);
    }
    return future;
  }

  @Override
  public SettableFuture<ConfigTaskResult> countStorageGroup(
      CountStorageGroupStatement countStorageGroupStatement) {
    SettableFuture<ConfigTaskResult> future = SettableFuture.create();
    int storageGroupNum;
    try {
      storageGroupNum =
          LocalConfigNode.getInstance()
              .getStorageGroupNum(
                  countStorageGroupStatement.getPartialPath(),
                  countStorageGroupStatement.isPrefixPath());
      // build TSBlock
      CountStorageGroupTask.buildTSBlock(storageGroupNum, future);
    } catch (MetadataException e) {
      future.setException(e);
    }
    return future;
  }

  @Override
  public SettableFuture<ConfigTaskResult> createFunction(
      String udfName, String className, List<String> uris) {
    SettableFuture<ConfigTaskResult> future = SettableFuture.create();
    try {
      UDFRegistrationService.getInstance()
          .register(udfName, className, uris, UDFExecutableManager.getInstance(), true);
      future.set(new ConfigTaskResult(TSStatusCode.SUCCESS_STATUS));
    } catch (Exception e) {
      final String message =
          String.format(
              "Failed to create function %s(%s), URI: %s, because %s.",
              udfName, className, uris, e.getMessage());
      LOGGER.error(message, e);
      future.setException(
          new IoTDBException(message, TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode()));
    }
    return future;
  }

  @Override
  public SettableFuture<ConfigTaskResult> deleteStorageGroup(
      DeleteStorageGroupStatement deleteStorageGroupStatement) {
    SettableFuture<ConfigTaskResult> future = SettableFuture.create();
    try {
      List<PartialPath> deletePathList = new ArrayList<>();
      for (String path : deleteStorageGroupStatement.getPrefixPath()) {
        PartialPath prefixPath = new PartialPath(path);
        deletePathList.addAll(
            LocalConfigNode.getInstance().getMatchedStorageGroups(prefixPath, false));
      }
      if (deletePathList.isEmpty()) {
        future.setException(
            new IoTDBException(
                String.format(
                    "Path %s does not exist",
                    Arrays.toString(deleteStorageGroupStatement.getPrefixPath().toArray())),
                TSStatusCode.TIMESERIES_NOT_EXIST.getStatusCode()));
        return future;
      } else {
        LocalConfigNode.getInstance().deleteStorageGroups(deletePathList);
      }
    } catch (MetadataException e) {
      future.setException(e);
      return future;
    }
    future.set(new ConfigTaskResult(TSStatusCode.SUCCESS_STATUS));
    return future;
  }

  @Override
  public SettableFuture<ConfigTaskResult> dropFunction(String udfName) {
    SettableFuture<ConfigTaskResult> future = SettableFuture.create();
    try {
      UDFRegistrationService.getInstance().deregister(udfName);
      future.set(new ConfigTaskResult(TSStatusCode.SUCCESS_STATUS));
    } catch (Exception e) {
      final String message =
          String.format("Failed to drop function %s, because %s.", udfName, e.getMessage());
      LOGGER.error(message, e);
      future.setException(
          new IoTDBException(message, TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode()));
    }
    return future;
  }

  @Override
  public SettableFuture<ConfigTaskResult> setTTL(SetTTLStatement setTTLStatement, String taskName) {
    SettableFuture<ConfigTaskResult> future = SettableFuture.create();
    try {
      LocalConfigNode.getInstance()
          .setTTL(setTTLStatement.getStorageGroupPath(), setTTLStatement.getTTL());
    } catch (MetadataException | IOException e) {
      future.setException(e);
    }
    future.set(new ConfigTaskResult(TSStatusCode.SUCCESS_STATUS));
    return future;
  }

  @Override
  public SettableFuture<ConfigTaskResult> merge(boolean isCluster) {
    SettableFuture<ConfigTaskResult> future = SettableFuture.create();
    TSStatus tsStatus = LocalConfigNode.getInstance().executeMergeOperation();
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
    TSStatus tsStatus = LocalConfigNode.getInstance().executeFlushOperation(tFlushReq);
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
    TSStatus tsStatus = LocalConfigNode.getInstance().executeClearCacheOperation();
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
    TSStatus tsStatus = LocalConfigNode.getInstance().executeLoadConfigurationOperation();
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
    future.setException(
        new IoTDBException(
            "Executing show cluster in standalone mode is not supported",
            TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode()));
    return future;
  }

  @Override
  public SettableFuture<ConfigTaskResult> showTTL(ShowTTLStatement showTTLStatement) {
    SettableFuture<ConfigTaskResult> future = SettableFuture.create();
    List<PartialPath> storageGroupPaths = showTTLStatement.getPaths();
    Map<String, Long> storageGroupToTTL = new HashMap<>();
    try {
      Map<PartialPath, Long> allStorageGroupToTTL =
          LocalConfigNode.getInstance().getStorageGroupsTTL();
      if (showTTLStatement.isAll()) {
        allStorageGroupToTTL
            .entrySet()
            .forEach(
                (entry) -> {
                  storageGroupToTTL.put(entry.getKey().getFullPath(), entry.getValue());
                });
      } else {
        for (PartialPath storageGroupPath : storageGroupPaths) {
          List<PartialPath> matchedStorageGroupPaths =
              LocalConfigNode.getInstance()
                  .getMatchedStorageGroups(storageGroupPath, showTTLStatement.isPrefixPath());
          for (PartialPath matchedStorageGroupPath : matchedStorageGroupPaths) {
            storageGroupToTTL.put(
                matchedStorageGroupPath.getFullPath(),
                allStorageGroupToTTL.get(matchedStorageGroupPath));
          }
        }
      }
    } catch (MetadataException e) {
      future.setException(e);
    }
    // build TSBlock
    ShowTTLTask.buildTSBlock(storageGroupToTTL, future);
    return future;
  }

  @Override
  public SettableFuture<ConfigTaskResult> showRegion(ShowRegionStatement showRegionStatement) {
    SettableFuture<ConfigTaskResult> future = SettableFuture.create();
    future.setException(
        new IoTDBException(
            "Executing show regions in standalone mode is not supported",
            TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode()));
    return future;
  }

  @Override
  public SettableFuture<ConfigTaskResult> showDataNodes(
      ShowDataNodesStatement showDataNodesStatement) {
    SettableFuture<ConfigTaskResult> future = SettableFuture.create();
    future.setException(
        new IoTDBException(
            "Executing show datanodes in standalone mode is not supported",
            TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode()));
    return future;
  }

  @Override
  public SettableFuture<ConfigTaskResult> showConfigNodes() {
    SettableFuture<ConfigTaskResult> future = SettableFuture.create();
    future.setException(
        new IoTDBException(
            "Executing show confignodes in standalone mode is not supported",
            TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode()));
    return future;
  }

  @Override
  public SettableFuture<ConfigTaskResult> createSchemaTemplate(
      CreateSchemaTemplateStatement createSchemaTemplateStatement) {
    SettableFuture<ConfigTaskResult> future = SettableFuture.create();
    future.setException(
        new IoTDBException(
            "Executing create schema template is not supported",
            TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode()));
    return future;
  }

  @Override
  public SettableFuture<ConfigTaskResult> showSchemaTemplate(
      ShowSchemaTemplateStatement showSchemaTemplateStatement) {
    SettableFuture<ConfigTaskResult> future = SettableFuture.create();
    future.setException(
        new IoTDBException(
            "Executing show schema template is not supported",
            TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode()));
    return future;
  }

  @Override
  public SettableFuture<ConfigTaskResult> showNodesInSchemaTemplate(
      ShowNodesInSchemaTemplateStatement showNodesInSchemaTemplateStatement) {
    SettableFuture<ConfigTaskResult> future = SettableFuture.create();
    future.setException(
        new IoTDBException(
            "Executing show nodes in schema template is not supported",
            TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode()));
    return future;
  }

  @Override
  public SettableFuture<ConfigTaskResult> setSchemaTemplate(
      SetSchemaTemplateStatement setSchemaTemplateStatement) {
    SettableFuture<ConfigTaskResult> future = SettableFuture.create();
    future.setException(
        new IoTDBException(
            "Executing set schema template is not supported",
            TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode()));
    return future;
  }

  @Override
  public SettableFuture<ConfigTaskResult> showPathSetTemplate(
      ShowPathSetTemplateStatement showPathSetTemplateStatement) {
    SettableFuture<ConfigTaskResult> future = SettableFuture.create();
    future.setException(
        new IoTDBException(
            "Executing show path set template is not supported",
            TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode()));
    return future;
  }

  @Override
  public SettableFuture<ConfigTaskResult> createPipe() {
    SettableFuture<ConfigTaskResult> future = SettableFuture.create();
    future.setException(
        new IoTDBException(
            "Executing create pipe is not supported",
            TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode()));
    return future;
  }

  @Override
  public SettableFuture<ConfigTaskResult> createPipeSink() {
    SettableFuture<ConfigTaskResult> future = SettableFuture.create();
    future.setException(
        new IoTDBException(
            "Executing create pipesink is not supported",
            TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode()));
    return future;
  }

  @Override
  public SettableFuture<ConfigTaskResult> dropPipe() {
    SettableFuture<ConfigTaskResult> future = SettableFuture.create();
    future.setException(
        new IoTDBException(
            "Executing drop pipe is not supported",
            TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode()));
    return future;
  }

  @Override
  public SettableFuture<ConfigTaskResult> dropPipeSink() {
    SettableFuture<ConfigTaskResult> future = SettableFuture.create();
    future.setException(
        new IoTDBException(
            "Executing drop pipesink is not supported",
            TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode()));
    return future;
  }

  @Override
  public SettableFuture<ConfigTaskResult> showPipe() {
    SettableFuture<ConfigTaskResult> future = SettableFuture.create();
    future.setException(
        new IoTDBException(
            "Executing show pipe is not supported",
            TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode()));
    return future;
  }

  @Override
  public SettableFuture<ConfigTaskResult> showPipeSink() {
    SettableFuture<ConfigTaskResult> future = SettableFuture.create();
    future.setException(
        new IoTDBException(
            "Executing show pipesink is not supported",
            TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode()));
    return future;
  }

  @Override
  public SettableFuture<ConfigTaskResult> startPipe() {
    SettableFuture<ConfigTaskResult> future = SettableFuture.create();
    future.setException(
        new IoTDBException(
            "Executing Start pipe is not supported",
            TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode()));
    return future;
  }

  @Override
  public SettableFuture<ConfigTaskResult> stopPipe() {
    SettableFuture<ConfigTaskResult> future = SettableFuture.create();
    future.setException(
        new IoTDBException(
            "Executing stop pipe is not supported",
            TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode()));
    return future;
  }
}
