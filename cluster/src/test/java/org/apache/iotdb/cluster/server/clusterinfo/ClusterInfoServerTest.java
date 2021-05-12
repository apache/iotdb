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
package org.apache.iotdb.cluster.server.clusterinfo;

import org.apache.iotdb.cluster.config.ClusterDescriptor;
import org.apache.iotdb.cluster.rpc.thrift.ClusterInfoService;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.rpc.RpcTransportFactory;

import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class ClusterInfoServerTest {
  ClusterInfoServiceImplTest test;
  ClusterInfoServer service;

  @Before
  public void setUp() throws Exception {
    test = new ClusterInfoServiceImplTest();
    test.setUp();
    service = new ClusterInfoServer();
    service.start();
  }

  @After
  public void tearDown() throws MetadataException {
    test.tearDown();
    service.stop();
  }

  @Test
  public void testConnect() {
    try {
      TTransport transport =
          RpcTransportFactory.INSTANCE.getTransport(
              new TSocket(
                  IoTDBDescriptor.getInstance().getConfig().getRpcAddress(),
                  ClusterDescriptor.getInstance().getConfig().getClusterInfoRpcPort()));
      transport.open();

      // connection success means OK.
      ClusterInfoService.Client client =
          new ClusterInfoService.Client(new TBinaryProtocol(transport));
      Assert.assertNotNull(client);
      // client's methods have been tested on ClusterInfoServiceImplTest
      transport.close();
    } catch (TTransportException e) {
      Assert.fail(e.getMessage());
    }
  }
}
