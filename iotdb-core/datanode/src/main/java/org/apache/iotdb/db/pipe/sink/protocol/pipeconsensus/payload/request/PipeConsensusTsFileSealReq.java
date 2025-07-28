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

package org.apache.iotdb.db.pipe.sink.protocol.pipeconsensus.payload.request;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.commons.consensus.index.ProgressIndex;
import org.apache.iotdb.commons.pipe.sink.payload.pipeconsensus.request.PipeConsensusRequestType;
import org.apache.iotdb.commons.pipe.sink.payload.pipeconsensus.request.PipeConsensusTransferFileSealReq;
import org.apache.iotdb.consensus.pipe.thrift.TCommitId;
import org.apache.iotdb.consensus.pipe.thrift.TPipeConsensusTransferReq;

import java.io.IOException;

public class PipeConsensusTsFileSealReq extends PipeConsensusTransferFileSealReq {
  private PipeConsensusTsFileSealReq() {
    // Empty constructor
  }

  @Override
  protected PipeConsensusRequestType getPlanType() {
    return PipeConsensusRequestType.TRANSFER_TS_FILE_SEAL;
  }

  /////////////////////////////// Thrift ///////////////////////////////

  public static PipeConsensusTsFileSealReq toTPipeConsensusTransferReq(
      String fileName,
      long fileLength,
      long pointCount,
      TCommitId commitId,
      TConsensusGroupId consensusGroupId,
      ProgressIndex progressIndex,
      int thisDataNodeId)
      throws IOException {
    return (PipeConsensusTsFileSealReq)
        new PipeConsensusTsFileSealReq()
            .convertToTPipeConsensusTransferReq(
                fileName,
                fileLength,
                pointCount,
                commitId,
                consensusGroupId,
                progressIndex,
                thisDataNodeId);
  }

  public static PipeConsensusTsFileSealReq fromTPipeConsensusTransferReq(
      TPipeConsensusTransferReq req) {
    return (PipeConsensusTsFileSealReq)
        new PipeConsensusTsFileSealReq().translateFromTPipeConsensusTransferReq(req);
  }

  /////////////////////////////// Object ///////////////////////////////

  @Override
  public boolean equals(Object obj) {
    return obj instanceof PipeConsensusTsFileSealReq && super.equals(obj);
  }

  @Override
  public int hashCode() {
    return super.hashCode();
  }
}
