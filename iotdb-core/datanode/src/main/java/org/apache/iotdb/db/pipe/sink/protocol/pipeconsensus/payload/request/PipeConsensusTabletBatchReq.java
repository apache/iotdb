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

import org.apache.iotdb.consensus.pipe.thrift.TPipeConsensusBatchTransferReq;
import org.apache.iotdb.consensus.pipe.thrift.TPipeConsensusTransferReq;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class PipeConsensusTabletBatchReq extends TPipeConsensusBatchTransferReq {
  private final transient List<PipeConsensusTabletBinaryReq> binaryReqs = new ArrayList<>();
  private final transient List<PipeConsensusTabletInsertNodeReq> insertNodeReqs = new ArrayList<>();

  private PipeConsensusTabletBatchReq() {
    // do nothing
  }

  /////////////////////////////// Thrift ///////////////////////////////
  public static PipeConsensusTabletBatchReq toTPipeConsensusBatchTransferReq(
      List<TPipeConsensusTransferReq> reqs) throws IOException {
    final PipeConsensusTabletBatchReq batchReq = new PipeConsensusTabletBatchReq();

    batchReq.batchReqs = reqs;

    return batchReq;
  }

  public static PipeConsensusTabletBatchReq fromTPipeConsensusBatchTransferReq(
      TPipeConsensusBatchTransferReq transferReq) {
    final PipeConsensusTabletBatchReq batchReq = new PipeConsensusTabletBatchReq();

    for (final TPipeConsensusTransferReq req : transferReq.getBatchReqs()) {
      if (req instanceof PipeConsensusTabletBinaryReq) {
        batchReq.binaryReqs.add((PipeConsensusTabletBinaryReq) req);
      } else {
        batchReq.insertNodeReqs.add((PipeConsensusTabletInsertNodeReq) req);
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
    PipeConsensusTabletBatchReq that = (PipeConsensusTabletBatchReq) obj;
    return binaryReqs.equals(that.binaryReqs)
        && insertNodeReqs.equals(that.insertNodeReqs)
        && Objects.equals(batchReqs, that.batchReqs);
  }

  @Override
  public int hashCode() {
    return Objects.hash(binaryReqs, insertNodeReqs, batchReqs);
  }
}
