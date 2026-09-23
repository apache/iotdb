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

package com.timecho.iotdb.service.thrift;

import org.apache.iotdb.common.rpc.thrift.TConfigNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.service.NoopServerContext;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.rpc.thrift.IConfigNodeRPCService;
import org.apache.iotdb.confignode.rpc.thrift.TShowSystemInfoResp;
import org.apache.iotdb.rpc.DeepCopyRpcTransportFactory;
import org.apache.iotdb.rpc.TSStatusCode;

import com.timecho.iotdb.manager.TimechoConfigManager;
import com.timecho.iotdb.manager.node.TimechoNodeManager;
import org.apache.thrift.TException;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TServerEventHandler;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TimechoConfigNodeRPCServiceProcessorTest {

  private final MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
  private TimechoNodeManager nodeManager;
  private TimechoConfigNodeRPCServiceProcessor processor;
  private Set<ObjectName> initialPools;
  private int localNodeId;
  private boolean failLocalSystemInfo;
  private TServer server;
  private Thread serverThread;

  @Before
  public void setUp() throws Exception {
    TimechoConfigManager configManager = mock(TimechoConfigManager.class);
    nodeManager = mock(TimechoNodeManager.class);
    when(configManager.getNodeManager()).thenReturn(nodeManager);
    processor =
        new TimechoConfigNodeRPCServiceProcessor(configManager) {
          @Override
          public String getSystemInfo() throws TException {
            if (failLocalSystemInfo) {
              throw new TException("test local system info failure");
            }
            return "local-system-info";
          }
        };
    localNodeId = ConfigNodeDescriptor.getInstance().getConf().getConfigNodeId();
    initialPools = getRegisteredPools();
  }

  @After
  public void tearDown() throws Exception {
    try {
      if (server != null) {
        server.stop();
        serverThread.join(5000);
        assertFalse("The test RPC server must stop", serverThread.isAlive());
      }
    } finally {
      // Clean up even when running the regression tests against the leaking implementation.
      for (ObjectName pool : getRegisteredPools()) {
        if (!initialPools.contains(pool)) {
          mBeanServer.unregisterMBean(pool);
        }
      }
    }
  }

  @Test
  public void testRepeatedLocalRequestsReleaseClientPools() throws Exception {
    setLocations(new TConfigNodeLocation().setConfigNodeId(localNodeId));

    for (int i = 0; i < 20; i++) {
      TShowSystemInfoResp response = processor.showSystemInfo();
      assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), response.getStatus().getCode());
      assertEquals(Collections.singletonList("local-system-info"), response.getSystemInfoList());
      assertEquals(initialPools, getRegisteredPools());
    }
  }

  @Test
  public void testEmptyClusterReleasesClientPool() throws Exception {
    setLocations();

    TShowSystemInfoResp response = processor.showSystemInfo();

    assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), response.getStatus().getCode());
    assertTrue(response.getSystemInfoList().isEmpty());
    assertEquals(initialPools, getRegisteredPools());
  }

  @Test
  public void testBorrowFailureReleasesClientPool() throws Exception {
    // A missing endpoint fails immediately without relying on a real network timeout.
    setLocations(new TConfigNodeLocation().setConfigNodeId(localNodeId + 1));

    assertLicenseErrorAndNoPoolLeak();
  }

  @Test
  public void testLocalSystemInfoFailureReleasesClientPool() throws Exception {
    setLocations(new TConfigNodeLocation().setConfigNodeId(localNodeId));
    failLocalSystemInfo = true;

    assertLicenseErrorAndNoPoolLeak();
  }

  @Test
  public void testRepeatedRemoteRequestsReleaseClientPoolsAndPreserveOrder() throws Exception {
    IConfigNodeRPCService.Iface remoteProcessor = mock(IConfigNodeRPCService.Iface.class);
    when(remoteProcessor.getSystemInfo()).thenReturn("remote-system-info");
    TEndPoint endpoint = startServer(remoteProcessor);
    setLocations(
        new TConfigNodeLocation().setConfigNodeId(localNodeId + 1).setInternalEndPoint(endpoint),
        new TConfigNodeLocation().setConfigNodeId(localNodeId));

    for (int i = 0; i < 20; i++) {
      TShowSystemInfoResp response = processor.showSystemInfo();
      assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), response.getStatus().getCode());
      assertEquals(
          Arrays.asList("local-system-info", "remote-system-info"), response.getSystemInfoList());
      assertEquals(initialPools, getRegisteredPools());
    }
  }

  @Test
  public void testRemoteRpcFailureReleasesClientPool() throws Exception {
    IConfigNodeRPCService.Iface remoteProcessor = mock(IConfigNodeRPCService.Iface.class);
    when(remoteProcessor.getSystemInfo()).thenThrow(new TException("test RPC failure"));
    TEndPoint endpoint = startServer(remoteProcessor);
    setLocations(
        new TConfigNodeLocation().setConfigNodeId(localNodeId + 1).setInternalEndPoint(endpoint));

    assertLicenseErrorAndNoPoolLeak();
  }

  private void assertLicenseErrorAndNoPoolLeak() throws Exception {
    TShowSystemInfoResp response = processor.showSystemInfo();

    assertEquals(TSStatusCode.LICENSE_ERROR.getStatusCode(), response.getStatus().getCode());
    assertFalse(response.isSetSystemInfoList());
    assertEquals(initialPools, getRegisteredPools());
  }

  private void setLocations(TConfigNodeLocation... locations) {
    when(nodeManager.getRegisteredConfigNodes())
        .thenAnswer(invocation -> new ArrayList<>(Arrays.asList(locations)));
  }

  private Set<ObjectName> getRegisteredPools() throws Exception {
    return mBeanServer.queryNames(
        new ObjectName("org.apache.commons.pool2:type=GenericKeyedObjectPool,*"), null);
  }

  private TEndPoint startServer(IConfigNodeRPCService.Iface remoteProcessor) throws Exception {
    ServerSocket socket = new ServerSocket(0, 50, InetAddress.getByName("127.0.0.1"));
    TServerSocket transport = new TServerSocket(socket);
    server =
        new TThreadPoolServer(
            new TThreadPoolServer.Args(transport)
                .processor(new IConfigNodeRPCService.Processor<>(remoteProcessor))
                .transportFactory(DeepCopyRpcTransportFactory.INSTANCE)
                .minWorkerThreads(1)
                .maxWorkerThreads(2)
                .stopTimeoutVal(1)
                .stopTimeoutUnit(TimeUnit.SECONDS));
    CountDownLatch started = new CountDownLatch(1);
    TServerEventHandler eventHandler = mock(TServerEventHandler.class);
    when(eventHandler.createContext(any(), any())).thenReturn(NoopServerContext.INSTANCE);
    doAnswer(
            invocation -> {
              started.countDown();
              return null;
            })
        .when(eventHandler)
        .preServe();
    server.setServerEventHandler(eventHandler);
    serverThread = new Thread(server::serve, "show-system-info-test-server");
    serverThread.start();
    assertTrue("The test RPC server must start", started.await(5, TimeUnit.SECONDS));
    return new TEndPoint("127.0.0.1", socket.getLocalPort());
  }
}
