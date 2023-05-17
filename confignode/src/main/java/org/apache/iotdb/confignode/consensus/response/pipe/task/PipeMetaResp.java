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
import org.apache.iotdb.confignode.rpc.thrift.TPullPipeMetaResp;
import org.apache.iotdb.consensus.common.DataSet;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class PipeMetaResp implements DataSet {

  private final TSStatus status;
  private final Iterable<PipeMeta> allPipeMeta;

  public PipeMetaResp(TSStatus status, Iterable<PipeMeta> allPipeMeta) {
    this.status = status;
    this.allPipeMeta = allPipeMeta;
  }

  public TPullPipeMetaResp convertToThriftResponse() throws IOException {
    final List<ByteBuffer> pipeMetaByteBuffers = new ArrayList<>();
    for (PipeMeta pipeMeta : allPipeMeta) {
      pipeMetaByteBuffers.add(pipeMeta.serialize());
    }
    return new TPullPipeMetaResp(status, pipeMetaByteBuffers);
  }
}
