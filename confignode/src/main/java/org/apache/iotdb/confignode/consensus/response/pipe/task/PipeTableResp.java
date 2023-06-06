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
import org.apache.iotdb.commons.pipe.task.meta.PipeMeta;
import org.apache.iotdb.commons.pipe.task.meta.PipeRuntimeMeta;
import org.apache.iotdb.commons.pipe.task.meta.PipeStaticMeta;
import org.apache.iotdb.commons.pipe.task.meta.PipeTaskMeta;
import org.apache.iotdb.confignode.rpc.thrift.TGetAllPipeInfoResp;
import org.apache.iotdb.confignode.rpc.thrift.TShowPipeInfo;
import org.apache.iotdb.confignode.rpc.thrift.TShowPipeResp;
import org.apache.iotdb.consensus.common.DataSet;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class PipeTableResp implements DataSet {

  private final TSStatus status;
  private final List<PipeMeta> allPipeMeta;

  public PipeTableResp(TSStatus status, List<PipeMeta> allPipeMeta) {
    this.status = status;
    this.allPipeMeta = allPipeMeta;
  }

  public List<PipeMeta> getAllPipeMeta() {
    return allPipeMeta;
  }

  public PipeTableResp filter(Boolean whereClause, String pipeName) {
    if (whereClause == null) {
      if (pipeName == null) {
        return this;
      } else {
        final List<PipeMeta> filteredPipeMeta = new ArrayList<>();
        for (PipeMeta pipeMeta : allPipeMeta) {
          if (pipeMeta.getStaticMeta().getPipeName().equals(pipeName)) {
            filteredPipeMeta.add(pipeMeta);
            break;
          }
        }
        return new PipeTableResp(status, filteredPipeMeta);
      }
    } else {
      if (pipeName == null) {
        return this;
      } else {
        String sortedConnectorParametersString = null;
        for (PipeMeta pipeMeta : allPipeMeta) {
          if (pipeMeta.getStaticMeta().getPipeName().equals(pipeName)) {
            sortedConnectorParametersString =
                pipeMeta.getStaticMeta().getConnectorParameters().toString();
            break;
          }
        }

        final List<PipeMeta> filteredPipeMeta = new ArrayList<>();
        for (PipeMeta pipeMeta : allPipeMeta) {
          if (pipeMeta
              .getStaticMeta()
              .getConnectorParameters()
              .toString()
              .equals(sortedConnectorParametersString)) {
            filteredPipeMeta.add(pipeMeta);
          }
        }
        return new PipeTableResp(status, filteredPipeMeta);
      }
    }
  }

  public TGetAllPipeInfoResp convertToTGetAllPipeInfoResp() throws IOException {
    final List<ByteBuffer> pipeInformationByteBuffers = new ArrayList<>();
    for (PipeMeta pipeMeta : allPipeMeta) {
      pipeInformationByteBuffers.add(pipeMeta.serialize());
    }
    return new TGetAllPipeInfoResp(status, pipeInformationByteBuffers);
  }

  public TShowPipeResp convertToTShowPipeResp() {
    final List<TShowPipeInfo> showPipeInfoList = new ArrayList<>();

    for (PipeMeta pipeMeta : allPipeMeta) {
      final PipeStaticMeta staticMeta = pipeMeta.getStaticMeta();
      final PipeRuntimeMeta runtimeMeta = pipeMeta.getRuntimeMeta();
      final StringBuilder exceptionMessageBuilder = new StringBuilder();
      for (PipeTaskMeta pipeTaskMeta : runtimeMeta.getConsensusGroupIdToTaskMetaMap().values()) {
        for (Exception e : pipeTaskMeta.getExceptionMessages()) {
          exceptionMessageBuilder.append(e.getMessage()).append("\n");
        }
      }

      showPipeInfoList.add(
          new TShowPipeInfo(
              staticMeta.getPipeName(),
              staticMeta.getCreationTime(),
              runtimeMeta.getStatus().get().name(),
              staticMeta.getCollectorParameters().toString(),
              staticMeta.getProcessorParameters().toString(),
              staticMeta.getConnectorParameters().toString(),
              exceptionMessageBuilder.toString()));
    }

    return new TShowPipeResp().setStatus(status).setPipeInfoList(showPipeInfoList);
  }
}
