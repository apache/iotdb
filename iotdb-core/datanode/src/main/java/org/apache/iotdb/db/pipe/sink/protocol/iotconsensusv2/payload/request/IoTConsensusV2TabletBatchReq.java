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

import org.apache.iotdb.consensus.iotconsensusv2.thrift.TIoTConsensusV2BatchTransferReq;
import org.apache.iotdb.consensus.iotconsensusv2.thrift.TIoTConsensusV2TransferReq;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class IoTConsensusV2TabletBatchReq extends TIoTConsensusV2BatchTransferReq {
  private final transient List<IoTConsensusV2TabletBinaryReq> binaryReqs = new ArrayList<>();
  private final transient List<IoTConsensusV2TabletInsertNodeReq> insertNodeReqs =
      new ArrayList<>();

  private IoTConsensusV2TabletBatchReq() {
    // do nothing
  }

  /////////////////////////////// Thrift ///////////////////////////////
  public static IoTConsensusV2TabletBatchReq toTIoTConsensusV2BatchTransferReq(
      List<TIoTConsensusV2TransferReq> reqs) throws IOException {
    final IoTConsensusV2TabletBatchReq batchReq = new IoTConsensusV2TabletBatchReq();

    batchReq.batchReqs = reqs;

    return batchReq;
  }

  public static IoTConsensusV2TabletBatchReq fromTIoTConsensusV2BatchTransferReq(
      TIoTConsensusV2BatchTransferReq transferReq) {
    final IoTConsensusV2TabletBatchReq batchReq = new IoTConsensusV2TabletBatchReq();

    for (final TIoTConsensusV2TransferReq req : transferReq.getBatchReqs()) {
      if (req instanceof IoTConsensusV2TabletBinaryReq) {
        batchReq.binaryReqs.add((IoTConsensusV2TabletBinaryReq) req);
      } else {
        batchReq.insertNodeReqs.add((IoTConsensusV2TabletInsertNodeReq) req);
      }
    }

    return batchReq;
  }

  /////////////////////////////// Object ///////////////////////////////

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    IoTConsensusV2TabletBatchReq that = (IoTConsensusV2TabletBatchReq) obj;
    return binaryReqs.equals(that.binaryReqs)
        && insertNodeReqs.equals(that.insertNodeReqs)
        && Objects.equals(batchReqs, that.batchReqs);
  }

  @Override
  public int hashCode() {
    return Objects.hash(binaryReqs, insertNodeReqs, batchReqs);
  }
}
