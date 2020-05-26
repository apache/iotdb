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

package org.apache.iotdb.cluster.log.logtypes;

import java.io.IOException;
import org.apache.iotdb.cluster.log.Log;
import org.apache.iotdb.cluster.rpc.thrift.Node;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.nio.ByteBuffer;
import org.apache.iotdb.db.utils.SerializeUtils;

public class RemoveNodeLog extends Log {

    private Node removedNode;

    @Override
    public ByteBuffer serialize() {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);
        try {
            dataOutputStream.writeByte(Types.REMOVE_NODE.ordinal());
            dataOutputStream.writeLong(getCurrLogIndex());
            dataOutputStream.writeLong(getCurrLogTerm());
        } catch (IOException e) {
            // ignored
        }
        SerializeUtils.serialize(removedNode, dataOutputStream);
        return ByteBuffer.wrap(byteArrayOutputStream.toByteArray());
    }

    @Override
    public void deserialize(ByteBuffer buffer) {
        setCurrLogIndex(buffer.getLong());
        setCurrLogTerm(buffer.getLong());

        removedNode = new Node();
        SerializeUtils.deserialize(removedNode, buffer);
    }

    public Node getRemovedNode() {
        return removedNode;
    }

    public void setRemovedNode(Node removedNode) {
        this.removedNode = removedNode;
    }
}
