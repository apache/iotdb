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

package org.apache.iotdb.db.queryengine.plan.planner.plan.node.write;

import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Similar to <code>PlanNodeType</code>, this class is used to provide serialization and
 * deserialization methods for Delete Ahead Log(DAL) which used for IoTConsensusV2 and pipe.
 */
public enum DeleteNodeType {
  TREE_DELETE_NODE((short) 1),
  RELATIONAL_DELETE_NODE((short) 2),
  ;

  public static final int BYTES = Short.BYTES;

  private final short nodeType;

  DeleteNodeType(short nodeType) {
    this.nodeType = nodeType;
  }

  public short getNodeType() {
    return nodeType;
  }

  public void serialize(ByteBuffer buffer) {
    ReadWriteIOUtils.write(nodeType, buffer);
  }

  public void serialize(DataOutputStream stream) throws IOException {
    ReadWriteIOUtils.write(nodeType, stream);
  }

  public static AbstractDeleteDataNode deserializeFromDAL(ByteBuffer buffer) {
    short nodeType = buffer.getShort();
    switch (nodeType) {
      case 1:
        return DeleteDataNode.deserializeFromDAL(buffer);
      case 2:
        return RelationalDeleteDataNode.deserializeFromDAL(buffer);
      default:
        throw new IllegalArgumentException("Invalid node type: " + nodeType);
    }
  }
}
