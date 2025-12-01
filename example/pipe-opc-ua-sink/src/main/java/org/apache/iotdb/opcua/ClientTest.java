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

package org.apache.iotdb.opcua;

import org.eclipse.milo.opcua.sdk.client.OpcUaClient;
import org.eclipse.milo.opcua.stack.core.types.builtin.DataValue;
import org.eclipse.milo.opcua.stack.core.types.builtin.NodeId;
import org.eclipse.milo.opcua.stack.core.types.builtin.StatusCode;
import org.eclipse.milo.opcua.stack.core.types.builtin.Variant;
import org.eclipse.milo.opcua.stack.core.types.enumerated.TimestampsToReturn;

import java.util.concurrent.CompletableFuture;

public class ClientTest implements ClientExample {

  public static void main(String[] args) {
    final ClientTest example = new ClientTest();

    new ClientExampleRunner(example).run();
  }

  @Override
  public void run(OpcUaClient client, CompletableFuture<OpcUaClient> future) throws Exception {
    // synchronous connect
    client.connect().get();
    System.out.println("✅ 连接成功");

    // 读取标签值
    NodeId nodeId = new NodeId(2, "chan2.grass.glasia");

    // 1. 先读取当前值确认节点可访问
    DataValue readValue = client.readValue(0, TimestampsToReturn.Both, nodeId).get();
    System.out.println("读取当前值: " + readValue.getValue().getValue());
    System.out.println("读取状态: " + readValue.getStatusCode());

    // 2. 尝试写入新值
    Variant newValue = new Variant(42.0);
    DataValue writeValue = new DataValue(newValue, null, null);

    System.out.println("尝试写入值: " + newValue.getValue());

    StatusCode writeStatus = client.writeValue(nodeId, writeValue).get();
    System.out.println("写入状态: " + writeStatus);
    client.disconnect().get();
  }
}
