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
import org.apache.iotdb.commons.pipe.plugin.PipePluginInformation;
import org.apache.iotdb.confignode.rpc.thrift.TGetPipePluginTableResp;
import org.apache.iotdb.consensus.common.DataSet;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class PipePluginTableResp implements DataSet {
  private TSStatus status;

  private List<PipePluginInformation> allPipePluginInformation;

  public PipePluginTableResp() {}

  public PipePluginTableResp(
      TSStatus status, List<PipePluginInformation> allPipePluginInformation) {
    this.status = status;
    this.allPipePluginInformation = allPipePluginInformation;
  }

  public void setStatus(TSStatus status) {
    this.status = status;
  }

  public List<PipePluginInformation> getAllPipePluginInformation() {
    return allPipePluginInformation;
  }

  public void setAllPipePluginInformation(List<PipePluginInformation> allPipePluginInformation) {
    this.allPipePluginInformation = allPipePluginInformation;
  }

  public TGetPipePluginTableResp convertToThriftResponse() throws IOException {
    List<ByteBuffer> pipePluginInformationByteBuffers = new ArrayList<>();

    for (PipePluginInformation pipePluginInformation : allPipePluginInformation) {
      pipePluginInformationByteBuffers.add(pipePluginInformation.serialize());
    }
    return new TGetPipePluginTableResp(status, pipePluginInformationByteBuffers);
  }
}
