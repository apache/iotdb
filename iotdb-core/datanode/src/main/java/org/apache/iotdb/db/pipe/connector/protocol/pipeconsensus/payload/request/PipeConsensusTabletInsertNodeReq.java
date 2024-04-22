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

package org.apache.iotdb.db.pipe.connector.protocol.pipeconsensus.payload.request;

import org.apache.iotdb.commons.pipe.connector.payload.pipeconsensus.request.PipeConsensusRequestType;
import org.apache.iotdb.commons.pipe.connector.payload.pipeconsensus.request.PipeConsensusRequestVersion;
import org.apache.iotdb.consensus.pipe.thrift.TPipeConsensusTransferReq;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertNode;

import java.util.Objects;

public class PipeConsensusTabletInsertNodeReq extends TPipeConsensusTransferReq {
  private transient InsertNode insertNode;

  private PipeConsensusTabletInsertNodeReq() {
    // Do nothing
  }

  /////////////////////////////// WriteBack & Batch ///////////////////////////////

  public static PipeConsensusTabletInsertNodeReq toTPipeConsensusTransferRawReq(
      InsertNode insertNode) {
    final PipeConsensusTabletInsertNodeReq req = new PipeConsensusTabletInsertNodeReq();

    req.insertNode = insertNode;

    return req;
  }

  /////////////////////////////// Thrift ///////////////////////////////

  public static PipeConsensusTabletInsertNodeReq toTPipeConsensusTransferReq(
      InsertNode insertNode) {
    final PipeConsensusTabletInsertNodeReq req = new PipeConsensusTabletInsertNodeReq();

    req.insertNode = insertNode;

    req.version = PipeConsensusRequestVersion.VERSION_1.getVersion();
    req.type = PipeConsensusRequestType.TRANSFER_TABLET_INSERT_NODE.getType();
    req.body = insertNode.serializeToByteBuffer();

    return req;
  }

  public static PipeConsensusTabletInsertNodeReq fromTPipeConsensusTransferReq(
      TPipeConsensusTransferReq transferReq) {
    final PipeConsensusTabletInsertNodeReq insertNodeReq = new PipeConsensusTabletInsertNodeReq();

    insertNodeReq.insertNode = (InsertNode) PlanNodeType.deserialize(transferReq.body);

    insertNodeReq.version = transferReq.version;
    insertNodeReq.type = transferReq.type;
    insertNodeReq.body = transferReq.body;

    return insertNodeReq;
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
    PipeConsensusTabletInsertNodeReq that = (PipeConsensusTabletInsertNodeReq) obj;
    return Objects.equals(insertNode, that.insertNode)
        && version == that.version
        && type == that.type
        && Objects.equals(body, that.body);
  }

  @Override
  public int hashCode() {
    return Objects.hash(insertNode, version, type, body);
  }
}
