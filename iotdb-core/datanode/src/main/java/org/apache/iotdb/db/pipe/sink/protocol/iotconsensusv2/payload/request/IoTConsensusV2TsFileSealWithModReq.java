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

package org.apache.iotdb.db.pipe.sink.protocol.iotconsensusv2.payload.request;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.commons.consensus.index.ProgressIndex;
import org.apache.iotdb.commons.pipe.sink.payload.iotconsensusv2.request.IoTConsensusV2RequestType;
import org.apache.iotdb.commons.pipe.sink.payload.iotconsensusv2.request.IoTConsensusV2TransferFileSealWithModReq;
import org.apache.iotdb.consensus.iotconsensusv2.thrift.TCommitId;
import org.apache.iotdb.consensus.iotconsensusv2.thrift.TIoTConsensusV2TransferReq;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;

public class IoTConsensusV2TsFileSealWithModReq extends IoTConsensusV2TransferFileSealWithModReq {

  private IoTConsensusV2TsFileSealWithModReq() {
    // Empty constructor
  }

  @Override
  protected IoTConsensusV2RequestType getPlanType() {
    return IoTConsensusV2RequestType.TRANSFER_TS_FILE_SEAL_WITH_MOD;
  }

  /////////////////////////////// Thrift ///////////////////////////////

  public static IoTConsensusV2TsFileSealWithModReq toTIoTConsensusV2TransferReq(
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
    return (IoTConsensusV2TsFileSealWithModReq)
        new IoTConsensusV2TsFileSealWithModReq()
            .convertToTIoTConsensusV2TransferReq(
                Arrays.asList(modFileName, tsFileName),
                Arrays.asList(modFileLength, tsFileLength),
                Arrays.asList(0L, tsFilePointCount),
                new HashMap<>(),
                commitId,
                consensusGroupId,
                progressIndex,
                thisDataNodeId);
  }

  public static IoTConsensusV2TsFileSealWithModReq fromTIoTConsensusV2TransferReq(
      TIoTConsensusV2TransferReq req) {
    return (IoTConsensusV2TsFileSealWithModReq)
        new IoTConsensusV2TsFileSealWithModReq().translateFromTIoTConsensusV2TransferReq(req);
  }

  /////////////////////////////// Object ///////////////////////////////

  @Override
  public boolean equals(Object obj) {
    return obj instanceof IoTConsensusV2TsFileSealWithModReq && super.equals(obj);
  }

  @Override
  public int hashCode() {
    return super.hashCode();
  }
}
