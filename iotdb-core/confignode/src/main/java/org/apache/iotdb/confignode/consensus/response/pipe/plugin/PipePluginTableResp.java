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

package org.apache.iotdb.confignode.consensus.response.pipe.plugin;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.pipe.agent.plugin.meta.PipePluginMeta;
import org.apache.iotdb.commons.pipe.datastructure.visibility.Visibility;
import org.apache.iotdb.commons.pipe.datastructure.visibility.VisibilityUtils;
import org.apache.iotdb.confignode.rpc.thrift.TGetPipePluginTableResp;
import org.apache.iotdb.consensus.common.DataSet;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class PipePluginTableResp implements DataSet {

  private final TSStatus status;
  private final List<PipePluginMeta> allPipePluginMeta;
  private final Map<String, Visibility> pipePluginNameToVisibilityMap;

  public PipePluginTableResp(
      TSStatus status,
      List<PipePluginMeta> allPipePluginMeta,
      Map<String, Visibility> pipePluginNameToVisibilityMap) {
    this.status = status;
    this.allPipePluginMeta = allPipePluginMeta;
    this.pipePluginNameToVisibilityMap = pipePluginNameToVisibilityMap;
  }

  public TGetPipePluginTableResp convertToThriftResponse() throws IOException {
    final List<ByteBuffer> pipePluginInformationByteBuffers = new ArrayList<>();
    for (PipePluginMeta pipePluginMeta : allPipePluginMeta) {
      pipePluginInformationByteBuffers.add(pipePluginMeta.serialize());
    }
    return new TGetPipePluginTableResp(status, pipePluginInformationByteBuffers);
  }

  public PipePluginTableResp filter(final boolean isTableModel) {
    return new PipePluginTableResp(
        status,
        allPipePluginMeta.stream()
            .filter(
                meta -> {
                  final String pipePluginName = meta.getPluginName();
                  final Visibility visibility =
                      pipePluginNameToVisibilityMap.getOrDefault(
                          pipePluginName, Visibility.TREE_ONLY);
                  return VisibilityUtils.isCompatible(visibility, isTableModel);
                })
            .collect(Collectors.toList()),
        pipePluginNameToVisibilityMap);
  }
}
