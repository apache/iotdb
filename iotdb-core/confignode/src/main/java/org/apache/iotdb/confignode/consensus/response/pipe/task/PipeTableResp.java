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
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.exception.pipe.PipeRuntimeException;
import org.apache.iotdb.commons.pipe.task.meta.PipeMeta;
import org.apache.iotdb.commons.pipe.task.meta.PipeRuntimeMeta;
import org.apache.iotdb.commons.pipe.task.meta.PipeStaticMeta;
import org.apache.iotdb.commons.pipe.task.meta.PipeTaskMeta;
import org.apache.iotdb.commons.pipe.task.meta.PipeTemporaryMeta;
import org.apache.iotdb.confignode.manager.pipe.extractor.ConfigRegionListeningFilter;
import org.apache.iotdb.confignode.rpc.thrift.TGetAllPipeInfoResp;
import org.apache.iotdb.confignode.rpc.thrift.TShowPipeInfo;
import org.apache.iotdb.confignode.rpc.thrift.TShowPipeResp;
import org.apache.iotdb.confignode.service.ConfigNode;
import org.apache.iotdb.consensus.common.DataSet;
import org.apache.iotdb.db.utils.DateTimeUtils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

public class PipeTableResp implements DataSet {

  private final TSStatus status;
  private final List<PipeMeta> allPipeMeta;

  public PipeTableResp(final TSStatus status, final List<PipeMeta> allPipeMeta) {
    this.status = status;
    this.allPipeMeta = allPipeMeta;
  }

  public List<PipeMeta> getAllPipeMeta() {
    return allPipeMeta;
  }

  public PipeTableResp filter(final Boolean whereClause, final String pipeName) {
    if (whereClause == null || !whereClause) {
      if (pipeName == null) {
        return this;
      } else {
        return new PipeTableResp(
            status,
            allPipeMeta.stream()
                .filter(pipeMeta -> pipeMeta.getStaticMeta().getPipeName().equals(pipeName))
                .collect(Collectors.toList()));
      }
    } else {
      if (pipeName == null) {
        return this;
      } else {
        final String sortedConnectorParametersString =
            allPipeMeta.stream()
                .filter(pipeMeta -> pipeMeta.getStaticMeta().getPipeName().equals(pipeName))
                .findFirst()
                .map(pipeMeta -> pipeMeta.getStaticMeta().getConnectorParameters().toString())
                .orElse(null);

        return new PipeTableResp(
            status,
            allPipeMeta.stream()
                .filter(
                    pipeMeta ->
                        pipeMeta
                            .getStaticMeta()
                            .getConnectorParameters()
                            .toString()
                            .equals(sortedConnectorParametersString))
                .collect(Collectors.toList()));
      }
    }
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
            .append(regionIds)
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
              staticMeta.getExtractorParameters().toString(),
              staticMeta.getProcessorParameters().toString(),
              staticMeta.getConnectorParameters().toString(),
              exceptionMessageBuilder.toString());
      final PipeTemporaryMeta temporaryMeta = pipeMeta.getTemporaryMeta();
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
                  pipeMeta.getStaticMeta().getExtractorParameters())
              .isEmpty();
    } catch (final IllegalPathException e) {
      return false;
    }
  }
}
