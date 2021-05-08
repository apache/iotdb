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

import org.apache.iotdb.cluster.ClusterMain;
import org.apache.iotdb.cluster.rpc.thrift.DataPartitionEntry;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.server.MetaClusterServer;
import org.apache.iotdb.cluster.server.member.MetaGroupMember;
import org.apache.iotdb.cluster.server.member.MetaGroupMemberTest;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.PartialPath;

import org.apache.thrift.TException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

public class ClusterInfoServiceImplTest {

  ClusterInfoServiceImpl impl;

  @Before
  public void setUp() throws Exception {
    MetaGroupMemberTest metaGroupMemberTest = new MetaGroupMemberTest();
    // will create a cluster with 10 nodes, ip: 0,10,20,...100
    metaGroupMemberTest.setUp();
    MetaGroupMember metaGroupMember = metaGroupMemberTest.getTestMetaGroupMember();

    MetaClusterServer metaClusterServer = new MetaClusterServer();
    metaClusterServer.getMember().stop();
    metaClusterServer.setMetaGroupMember(metaGroupMember);

    ClusterMain.setMetaClusterServer(metaClusterServer);

    metaClusterServer.getIoTDB().metaManager.setStorageGroup(new PartialPath("root", "sg"));
    // metaClusterServer.getMember()
    impl = new ClusterInfoServiceImpl();
  }

  @After
  public void tearDown() throws MetadataException {
    ClusterMain.getMetaServer()
        .getIoTDB()
        .metaManager
        .deleteStorageGroups(Collections.singletonList(new PartialPath("root", "sg")));
    ClusterMain.getMetaServer().stop();
  }

  @Test
  public void getRing() throws TException {
    List<Node> nodes = impl.getRing();
    Assert.assertEquals(10, nodes.size());
  }

  @Test
  public void getDataPartition() {
    List<DataPartitionEntry> entries = impl.getDataPartition("root.sg", 0, 100);
    Assert.assertEquals(1, entries.size());
    List<Node> nodes = entries.get(0).getNodes();
    Assert.assertEquals(50, nodes.get(0).getNodeIdentifier());
    Assert.assertEquals(60, nodes.get(1).getNodeIdentifier());
  }

  @Test
  public void getMetaPartition() throws TException {
    List<Node> nodes = impl.getMetaPartition("root.sg");
    Assert.assertEquals(50, nodes.get(0).getNodeIdentifier());
    Assert.assertEquals(60, nodes.get(1).getNodeIdentifier());
  }

  @Test
  public void getInstrumentingInfo() throws TException {
    // hard to test the content of the instrumentInfo.
    Assert.assertNotNull(impl.getInstrumentingInfo());
  }
}
