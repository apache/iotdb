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

package org.apache.iotdb.db.queryengine.plan.execution.config.executor;

import org.apache.iotdb.common.rpc.thrift.TConfigNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.confignode.rpc.thrift.TNodeVersionInfo;
import org.apache.iotdb.confignode.rpc.thrift.TShowClusterResp;

import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Method;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class ClusterConfigTaskExecutorCQTest {

  @Test
  public void mixedVersionClusterIsRejectedAtDataNodeIngress() throws Exception {
    Method method =
        ClusterConfigTaskExecutor.class.getDeclaredMethod(
            "allClusterNodesSupportCQDurationEncoding", TShowClusterResp.class);
    method.setAccessible(true);
    ClusterConfigTaskExecutor executor = ClusterConfigTaskExecutor.getInstance();

    Assert.assertFalse((Boolean) method.invoke(executor, new TShowClusterResp()));
    Assert.assertFalse((Boolean) method.invoke(executor, (Object) null));

    TConfigNodeLocation configNode =
        new TConfigNodeLocation(
            1, new TEndPoint("127.0.0.1", 10710), new TEndPoint("127.0.0.1", 10720));
    TDataNodeLocation dataNode =
        new TDataNodeLocation(
            11,
            new TEndPoint("127.0.0.1", 6667),
            new TEndPoint("127.0.0.1", 10730),
            new TEndPoint("127.0.0.1", 10740),
            new TEndPoint("127.0.0.1", 10750),
            new TEndPoint("127.0.0.1", 10760));
    TNodeVersionInfo supported =
        new TNodeVersionInfo("2.0.0", "new")
            .setSupportedCQDurationEncodingVersions(Collections.singleton((short) 1));
    TNodeVersionInfo unsupported = new TNodeVersionInfo("1.3.0", "old");

    TShowClusterResp mixed = new TShowClusterResp();
    mixed.setConfigNodeList(Collections.singletonList(configNode));
    mixed.setDataNodeList(Collections.singletonList(dataNode));
    Map<Integer, TNodeVersionInfo> versions = new HashMap<>();
    versions.put(configNode.getConfigNodeId(), supported);
    versions.put(dataNode.getDataNodeId(), unsupported);
    mixed.setNodeVersionInfo(versions);
    Assert.assertFalse((Boolean) method.invoke(executor, mixed));

    versions.put(dataNode.getDataNodeId(), supported);
    Assert.assertTrue((Boolean) method.invoke(executor, mixed));
  }
}
