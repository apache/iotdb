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

import io.netty.buffer.ByteBuf;
import org.eclipse.milo.opcua.sdk.client.OpcUaClient;
import org.eclipse.milo.opcua.sdk.core.AccessLevel;
import org.eclipse.milo.opcua.sdk.core.ValueRanks;
import org.eclipse.milo.opcua.stack.core.Identifiers;
import org.eclipse.milo.opcua.stack.core.types.builtin.ByteString;
import org.eclipse.milo.opcua.stack.core.types.builtin.DataValue;
import org.eclipse.milo.opcua.stack.core.types.builtin.DateTime;
import org.eclipse.milo.opcua.stack.core.types.builtin.ExtensionObject;
import org.eclipse.milo.opcua.stack.core.types.builtin.LocalizedText;
import org.eclipse.milo.opcua.stack.core.types.builtin.NodeId;
import org.eclipse.milo.opcua.stack.core.types.builtin.QualifiedName;
import org.eclipse.milo.opcua.stack.core.types.builtin.StatusCode;
import org.eclipse.milo.opcua.stack.core.types.builtin.Variant;
import org.eclipse.milo.opcua.stack.core.types.builtin.unsigned.Unsigned;
import org.eclipse.milo.opcua.stack.core.types.enumerated.NodeClass;
import org.eclipse.milo.opcua.stack.core.types.enumerated.TimestampsToReturn;
import org.eclipse.milo.opcua.stack.core.types.structured.AddNodesItem;
import org.eclipse.milo.opcua.stack.core.types.structured.AddNodesResponse;
import org.eclipse.milo.opcua.stack.core.types.structured.DeleteNodesItem;
import org.eclipse.milo.opcua.stack.core.types.structured.VariableAttributes;

import java.util.Collections;
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

    // 读取标签值c
    NodeId nodeId = new NodeId(2, "root/sg/d1/s2");

    // 1. 先读取当前值确认节点可访问
    DataValue readValue = client.readValue(0, TimestampsToReturn.Both, nodeId).get();
    System.out.println("读取当前值: " + readValue.getValue().getValue());
    System.out.println("读取状态: " + readValue.getStatusCode());

    // 2. 尝试写入新值
    Variant newValue = new Variant(42.0);
    DataValue writeValue = new DataValue(newValue, StatusCode.GOOD, new DateTime(), new DateTime());

    System.out.println("尝试写入值: " + newValue.getValue());

    StatusCode writeStatus = client.writeValue(nodeId, writeValue).get();
    System.out.println("写入状态: " + writeStatus);

    client.deleteNodes(Collections.singletonList(new DeleteNodesItem(nodeId, true)));

    AddNodesResponse addStatus =
        client
            .addNodes(
                Collections.singletonList(
                    new AddNodesItem(
                        new NodeId(2, "root/sg/d1").expanded(),
                        Identifiers.Organizes,
                        new NodeId(2, "root/sg/d1/s2").expanded(),
                        new QualifiedName(2, "s2"),
                        NodeClass.Variable,
                        ExtensionObject.encode(
                            client.getStaticSerializationContext(),
                            createPressureSensorAttributes()),
                        Identifiers.BaseDataVariableType.expanded())))
            .get();
    System.out.println("新增节点状态: " + addStatus);
    client.disconnect().get();
  }

  public static VariableAttributes createPressureSensorAttributes() {
    return new VariableAttributes(
        Unsigned.uint(0xFFFF), // specifiedAttributes
        LocalizedText.english("s2"),
        LocalizedText.english("反应釜压力传感器"),
        Unsigned.uint(0), // writeMask
        Unsigned.uint(0), // userWriteMask
        new Variant(101.3), // 初始压力值 101.3 kPa
        Identifiers.Float, // 浮点数类型
        ValueRanks.Scalar, // 标量
        null, // arrayDimensions
        AccessLevel.toValue(AccessLevel.READ_WRITE),
        AccessLevel.toValue(AccessLevel.READ_WRITE),
        500.0, // 500ms 采样间隔
        false // 启用历史记录
        );
  }

  // 方法1：将 ByteBuf 转换为 ByteString
  public static ByteString convertByteBufToByteString(ByteBuf byteBuf) {
    // 确保 ByteBuf 可读
    if (byteBuf == null || byteBuf.readableBytes() == 0) {
      return ByteString.NULL_VALUE; // 返回空 ByteString
    }

    // 创建与 ByteBuf 可读字节数相同的字节数组
    byte[] bytes = new byte[byteBuf.readableBytes()];

    // 将 ByteBuf 数据读取到字节数组
    byteBuf.readBytes(bytes);

    // 使用 ByteString.of() 创建 ByteString
    return ByteString.of(bytes);
  }
}
