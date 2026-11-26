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
import org.apache.iotdb.commons.pipe.sink.payload.iotconsensusv2.request.IoTConsensusV2RequestType;
import org.apache.iotdb.commons.pipe.sink.payload.iotconsensusv2.request.IoTConsensusV2TransferFilePieceReq;
import org.apache.iotdb.consensus.iotconsensusv2.thrift.TCommitId;
import org.apache.iotdb.consensus.iotconsensusv2.thrift.TIoTConsensusV2TransferReq;

import java.io.IOException;

public class IoTConsensusV2TsFilePieceWithModReq extends IoTConsensusV2TransferFilePieceReq {

  private IoTConsensusV2TsFilePieceWithModReq() {
    // Empty constructor
  }

  @Override
  protected IoTConsensusV2RequestType getPlanType() {
    return IoTConsensusV2RequestType.TRANSFER_TS_FILE_PIECE_WITH_MOD;
  }

  /////////////////////////////// Thrift ///////////////////////////////

  public static IoTConsensusV2TsFilePieceWithModReq toTIoTConsensusV2TransferReq(
      String fileName,
      long startWritingOffset,
      byte[] filePiece,
      TCommitId commitId,
      TConsensusGroupId consensusGroupId,
      int thisDataNodeId)
      throws IOException {
    return (IoTConsensusV2TsFilePieceWithModReq)
        new IoTConsensusV2TsFilePieceWithModReq()
            .convertToTIoTConsensusV2TransferReq(
                fileName,
                startWritingOffset,
                filePiece,
                commitId,
                consensusGroupId,
                thisDataNodeId);
  }

  public static IoTConsensusV2TsFilePieceWithModReq fromTIoTConsensusV2TransferReq(
      TIoTConsensusV2TransferReq transferReq) {
    return (IoTConsensusV2TsFilePieceWithModReq)
        new IoTConsensusV2TsFilePieceWithModReq()
            .translateFromTIoTConsensusV2TransferReq(transferReq);
  }

  /////////////////////////////// Object ///////////////////////////////

  @Override
  public boolean equals(Object obj) {
    return obj instanceof IoTConsensusV2TsFilePieceWithModReq && super.equals(obj);
  }

  @Override
  public int hashCode() {
    return super.hashCode();
  }
}
