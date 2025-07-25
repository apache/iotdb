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
import org.apache.iotdb.commons.pipe.sink.payload.pipeconsensus.request.PipeConsensusTransferFileSealWithModReq;
import org.apache.iotdb.consensus.pipe.thrift.TCommitId;
import org.apache.iotdb.consensus.pipe.thrift.TPipeConsensusTransferReq;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;

public class PipeConsensusTsFileSealWithModReq extends PipeConsensusTransferFileSealWithModReq {

  private PipeConsensusTsFileSealWithModReq() {
    // Empty constructor
  }

  @Override
  protected PipeConsensusRequestType getPlanType() {
    return PipeConsensusRequestType.TRANSFER_TS_FILE_SEAL_WITH_MOD;
  }

  /////////////////////////////// Thrift ///////////////////////////////

  public static PipeConsensusTsFileSealWithModReq toTPipeConsensusTransferReq(
      String modFileName,
      long modFileLength,
      String tsFileName,
      long tsFileLength,
      long tsFilePointCount,
      TCommitId commitId,
      TConsensusGroupId consensusGroupId,
      ProgressIndex progressIndex,
      int thisDataNodeId)
      throws IOException {
    return (PipeConsensusTsFileSealWithModReq)
        new PipeConsensusTsFileSealWithModReq()
            .convertToTPipeConsensusTransferReq(
                Arrays.asList(modFileName, tsFileName),
                Arrays.asList(modFileLength, tsFileLength),
                Arrays.asList(0L, tsFilePointCount),
                new HashMap<>(),
                commitId,
                consensusGroupId,
                progressIndex,
                thisDataNodeId);
  }

  public static PipeConsensusTsFileSealWithModReq fromTPipeConsensusTransferReq(
      TPipeConsensusTransferReq req) {
    return (PipeConsensusTsFileSealWithModReq)
        new PipeConsensusTsFileSealWithModReq().translateFromTPipeConsensusTransferReq(req);
  }

  /////////////////////////////// Object ///////////////////////////////

  @Override
  public boolean equals(Object obj) {
    return obj instanceof PipeConsensusTsFileSealWithModReq && super.equals(obj);
  }

  @Override
  public int hashCode() {
    return super.hashCode();
  }
}
