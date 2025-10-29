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

package org.apache.iotdb.confignode.consensus.response.pipe.task;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.auth.authorizer.BasicAuthorizer;
import org.apache.iotdb.commons.auth.entity.PrivilegeType;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.exception.pipe.PipeRuntimeException;
import org.apache.iotdb.commons.pipe.agent.plugin.builtin.BuiltinPipePlugin;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeMeta;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeRuntimeMeta;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeStaticMeta;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeTaskMeta;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeTemporaryMetaInCoordinator;
import org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant;
import org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant;
import org.apache.iotdb.commons.pipe.config.constant.SystemConstant;
import org.apache.iotdb.confignode.manager.pipe.source.ConfigRegionListeningFilter;
import org.apache.iotdb.confignode.rpc.thrift.TGetAllPipeInfoResp;
import org.apache.iotdb.confignode.rpc.thrift.TShowPipeInfo;
import org.apache.iotdb.confignode.rpc.thrift.TShowPipeResp;
import org.apache.iotdb.confignode.service.ConfigNode;
import org.apache.iotdb.consensus.common.DataSet;
import org.apache.iotdb.db.utils.DateTimeUtils;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

public class PipeTableResp implements DataSet {

  private final TSStatus status;
  private final List<PipeMeta> allPipeMeta;

  private static final String CONFIG_REGION_ID = "CONFIG_REGION";

  public PipeTableResp(final TSStatus status, final List<PipeMeta> allPipeMeta) {
    this.status = status;
    this.allPipeMeta = allPipeMeta;
  }

  public List<PipeMeta> getAllPipeMeta() {
    return allPipeMeta;
  }

  public PipeTableResp filter(final Boolean whereClause, final String pipeName) {
    if (Objects.isNull(pipeName)) {
      return this;
    }
    if (whereClause == null || !whereClause) {
      return new PipeTableResp(
          status,
          allPipeMeta.stream()
              .filter(pipeMeta -> pipeMeta.getStaticMeta().getPipeName().equals(pipeName))
              .collect(Collectors.toList()));
    } else {
      final String sortedSinkParametersString =
          allPipeMeta.stream()
              .filter(pipeMeta -> pipeMeta.getStaticMeta().getPipeName().equals(pipeName))
              .findFirst()
              .map(pipeMeta -> pipeMeta.getStaticMeta().getSinkParameters().toString())
              .orElse(null);

      return new PipeTableResp(
          status,
          allPipeMeta.stream()
              .filter(
                  pipeMeta ->
                      pipeMeta
                          .getStaticMeta()
                          .getSinkParameters()
                          .toString()
                          .equals(sortedSinkParametersString))
              .collect(Collectors.toList()));
    }
  }

  public PipeTableResp filter(
      final Boolean whereClause,
      final String pipeName,
      final boolean isTableModel,
      final String userName) {
    final PipeTableResp resp = filter(whereClause, pipeName);
    resp.allPipeMeta.removeIf(
        meta ->
            !meta.getStaticMeta().visibleUnder(isTableModel)
                || !isVisible4User(userName, meta.getStaticMeta()));
    return resp;
  }

  public boolean isVisible4User(final String userName, final PipeStaticMeta meta) {
    try {
      return Objects.isNull(userName)
          || BasicAuthorizer.getInstance().getUser(userName).checkSysPrivilege(PrivilegeType.SYSTEM)
          || isVisible4SourceUser(userName, meta.getSourceParameters())
          || isVisible4SinkUser(userName, meta.getSinkParameters());
    } catch (final Exception e) {
      return false;
    }
  }

  private boolean isVisible4SourceUser(
      final String userName, final PipeParameters sourceParameters) {
    final String pluginName =
        sourceParameters
            .getStringOrDefault(
                Arrays.asList(PipeSourceConstant.EXTRACTOR_KEY, PipeSourceConstant.SOURCE_KEY),
                BuiltinPipePlugin.IOTDB_EXTRACTOR.getPipePluginName())
            .toLowerCase();

    if (!pluginName.equals(BuiltinPipePlugin.IOTDB_EXTRACTOR.getPipePluginName())
        && !pluginName.equals(BuiltinPipePlugin.IOTDB_SOURCE.getPipePluginName())) {
      return false;
    }

    return Objects.equals(
        userName,
        sourceParameters.getStringByKeys(
            PipeSourceConstant.EXTRACTOR_IOTDB_USER_KEY,
            PipeSourceConstant.SOURCE_IOTDB_USER_KEY,
            PipeSourceConstant.EXTRACTOR_IOTDB_USERNAME_KEY,
            PipeSourceConstant.SOURCE_IOTDB_USERNAME_KEY));
  }

  private boolean isVisible4SinkUser(final String userName, final PipeParameters sinkParameters) {
    final String pluginName =
        sinkParameters
            .getStringOrDefault(
                Arrays.asList(PipeSinkConstant.CONNECTOR_KEY, PipeSinkConstant.SINK_KEY),
                BuiltinPipePlugin.IOTDB_THRIFT_SINK.getPipePluginName())
            .toLowerCase();

    if (!pluginName.equals(BuiltinPipePlugin.WRITE_BACK_CONNECTOR.getPipePluginName())
        && !pluginName.equals(BuiltinPipePlugin.WRITE_BACK_SINK.getPipePluginName())) {
      return false;
    }

    return Objects.equals(
        userName,
        sinkParameters.getStringByKeys(
            PipeSourceConstant.EXTRACTOR_IOTDB_USER_KEY,
            PipeSourceConstant.SOURCE_IOTDB_USER_KEY,
            PipeSourceConstant.EXTRACTOR_IOTDB_USERNAME_KEY,
            PipeSourceConstant.SOURCE_IOTDB_USERNAME_KEY));
  }

  public TGetAllPipeInfoResp convertToTGetAllPipeInfoResp() throws IOException {
    final List<ByteBuffer> pipeInformationByteBuffers = new ArrayList<>();
    for (final PipeMeta pipeMeta : allPipeMeta) {
      pipeInformationByteBuffers.add(pipeMeta.serialize());
    }
    return new TGetAllPipeInfoResp(status, pipeInformationByteBuffers);
  }

  public TShowPipeResp convertToTShowPipeResp() {
    final List<TShowPipeInfo> showPipeInfoList = new ArrayList<>();

    for (final PipeMeta pipeMeta : allPipeMeta) {
      final Map<String, Set<Integer>> pipeExceptionMessage2RegionIdsMap = new HashMap<>();
      final Map<String, Set<Integer>> pipeExceptionMessage2NodeIdsMap = new HashMap<>();
      final PipeStaticMeta staticMeta = pipeMeta.getStaticMeta();
      final PipeRuntimeMeta runtimeMeta = pipeMeta.getRuntimeMeta();
      final StringBuilder exceptionMessageBuilder = new StringBuilder();

      for (final Map.Entry<Integer, PipeRuntimeException> entry :
          runtimeMeta.getNodeId2PipeRuntimeExceptionMap().entrySet()) {
        final Integer nodeId = entry.getKey();
        final PipeRuntimeException e = entry.getValue();
        final String exceptionMessage =
            DateTimeUtils.convertLongToDate(e.getTimeStamp(), "ms") + ", " + e.getMessage();

        pipeExceptionMessage2NodeIdsMap
            .computeIfAbsent(exceptionMessage, k -> new TreeSet<>())
            .add(nodeId);
      }

      for (final Map.Entry<Integer, PipeTaskMeta> entry :
          runtimeMeta.getConsensusGroupId2TaskMetaMap().entrySet()) {
        final Integer regionId = entry.getKey();
        for (final PipeRuntimeException e : entry.getValue().getExceptionMessages()) {
          final String exceptionMessage =
              DateTimeUtils.convertLongToDate(e.getTimeStamp(), "ms") + ", " + e.getMessage();
          pipeExceptionMessage2RegionIdsMap
              .computeIfAbsent(exceptionMessage, k -> new TreeSet<>())
              .add(regionId);
        }
      }

      for (final Map.Entry<String, Set<Integer>> entry :
          pipeExceptionMessage2NodeIdsMap.entrySet()) {
        final String exceptionMessage = entry.getKey();
        final Set<Integer> nodeIds = entry.getValue();
        exceptionMessageBuilder
            .append("nodeIds: ")
            .append(nodeIds)
            .append(", ")
            .append(exceptionMessage)
            .append("; ");
      }

      final int size = pipeExceptionMessage2RegionIdsMap.size();
      int count = 0;

      for (final Map.Entry<String, Set<Integer>> entry :
          pipeExceptionMessage2RegionIdsMap.entrySet()) {
        final String exceptionMessage = entry.getKey();
        final Set<Integer> regionIds = entry.getValue();
        exceptionMessageBuilder
            .append("regionIds: ")
            .append(
                regionIds.stream()
                    .map(
                        id -> {
                          if (Objects.equals(Integer.MIN_VALUE, id)) {
                            // handle config region id for user experience
                            return CONFIG_REGION_ID;
                          }
                          return id.toString();
                        })
                    .collect(Collectors.toSet()))
            .append(", ")
            .append(exceptionMessage);
        if (++count < size) {
          exceptionMessageBuilder.append("; ");
        }
      }

      final TShowPipeInfo showPipeInfo =
          new TShowPipeInfo(
              staticMeta.getPipeName(),
              staticMeta.getCreationTime(),
              runtimeMeta.getStatus().get().name(),
              SystemConstant.addSystemKeysIfNecessary(staticMeta.getSourceParameters()).toString(),
              staticMeta.getProcessorParameters().toString(),
              staticMeta.getSinkParameters().toString(),
              exceptionMessageBuilder.toString());
      final PipeTemporaryMetaInCoordinator temporaryMeta =
          (PipeTemporaryMetaInCoordinator) pipeMeta.getTemporaryMeta();
      final boolean canCalculateOnLocal = canCalculateOnLocal(pipeMeta);

      showPipeInfo.setRemainingEventCount(
          canCalculateOnLocal ? -1 : temporaryMeta.getGlobalRemainingEvents());
      showPipeInfo.setEstimatedRemainingTime(
          canCalculateOnLocal ? -1 : temporaryMeta.getGlobalRemainingTime());
      showPipeInfoList.add(showPipeInfo);
    }

    // sorted by pipe name
    showPipeInfoList.sort(Comparator.comparing(pipeInfo -> pipeInfo.id));
    return new TShowPipeResp().setStatus(status).setPipeInfoList(showPipeInfoList);
  }

  private boolean canCalculateOnLocal(final PipeMeta pipeMeta) {
    try {
      return ConfigNode.getInstance()
                  .getConfigManager()
                  .getNodeManager()
                  .getRegisteredDataNodeCount()
              == 1
          && ConfigRegionListeningFilter.parseListeningPlanTypeSet(
                  pipeMeta.getStaticMeta().getSourceParameters())
              .isEmpty();
    } catch (final IllegalPathException e) {
      return false;
    }
  }
}
