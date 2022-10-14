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
package org.apache.iotdb.confignode.consensus.request.write;

import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.confignode.consensus.request.write.datanode.RemoveDataNodePlan;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.fail;

public class RemoveDataNodePlanTest {
  private RemoveDataNodePlan req1;
  private RemoveDataNodePlan req2;

  private static String serializedFileName = "./f1.bin";

  @BeforeClass
  public static void beforeClass() {
    File file = new File(serializedFileName);
    if (file.exists()) {
      file.delete();
    }
    try {
      file.createNewFile();
    } catch (IOException e) {
      // do nothing;
    }
  }

  @AfterClass
  public static void afterClass() {
    File file = new File(serializedFileName);
    if (file.exists()) {
      file.delete();
    }
  }

  @Before
  public void before() {
    List<TDataNodeLocation> locations = new ArrayList<>();
    TDataNodeLocation location1 = new TDataNodeLocation();
    location1.setDataNodeId(1);
    location1.setInternalEndPoint(new TEndPoint("192.168.12.1", 6661));
    location1.setClientRpcEndPoint(new TEndPoint("192.168.12.1", 6662));
    location1.setDataRegionConsensusEndPoint(new TEndPoint("192.168.12.1", 6663));
    location1.setSchemaRegionConsensusEndPoint(new TEndPoint("192.168.12.1", 6664));
    location1.setMPPDataExchangeEndPoint(new TEndPoint("192.168.12.1", 6665));
    locations.add(location1);

    TDataNodeLocation location2 = new TDataNodeLocation();
    location2.setDataNodeId(2);
    location2.setInternalEndPoint(new TEndPoint("192.168.12.2", 6661));
    location2.setClientRpcEndPoint(new TEndPoint("192.168.12.2", 6662));
    location2.setDataRegionConsensusEndPoint(new TEndPoint("192.168.12.2", 6663));
    location2.setSchemaRegionConsensusEndPoint(new TEndPoint("192.168.12.2", 6664));
    location2.setMPPDataExchangeEndPoint(new TEndPoint("192.168.12.2", 6665));
    locations.add(location2);

    req1 = new RemoveDataNodePlan(new ArrayList<>(locations));
    req2 = new RemoveDataNodePlan(new ArrayList<>(locations));
  }

  @After
  public void after() {
    req1.getDataNodeLocations().clear();
    req2.getDataNodeLocations().clear();
    req1 = null;
    req2 = null;
  }

  @Test
  public void testDeserializeImpl() {}

  @Test
  public void notNullTest() {
    Assert.assertNotNull(req1);
    Assert.assertNotNull(req2);
  }

  @Test
  public void testEquals() {
    Assert.assertEquals(req1, req2);
  }

  @Test
  public void testNotEquals() {
    TDataNodeLocation d = new TDataNodeLocation();
    d.setDataNodeId(3);
    d.setInternalEndPoint(new TEndPoint("192.168.12.3", 6667));
    req1.getDataNodeLocations().add(d);
    Assert.assertNotEquals(req1, req2);
  }

  @Test
  public void testTestHashCode() {
    int code1 = req1.hashCode();
    int code2 = req2.hashCode();
    Assert.assertEquals(code1, code2);
  }

  @Test
  public void testNotUpdatePlanSerializeAndDeSerialize() {
    try {
      RemoveDataNodePlan deSerializeReq = runPlanSerializeAndDeSerialize(false);
      Assert.assertEquals(req1, deSerializeReq);
    } catch (IOException e) {
      fail(e.getMessage());
    }
  }

  @Test
  public void testUpdatePlanSerializeAndDeSerialize() {
    try {
      RemoveDataNodePlan deSerializeReq = runPlanSerializeAndDeSerialize(true);
      Assert.assertEquals(req1, deSerializeReq);
    } catch (IOException e) {
      fail(e.getMessage());
    }
  }

  private RemoveDataNodePlan runPlanSerializeAndDeSerialize(boolean update) throws IOException {
    try (DataOutputStream outputStream =
        new DataOutputStream(Files.newOutputStream(Paths.get(serializedFileName)))) {
      req1.serializeImpl(outputStream);
    }

    byte[] data = new byte[2048];
    try (DataInputStream inputStream =
        new DataInputStream(Files.newInputStream(Paths.get(serializedFileName)))) {
      inputStream.read(data);
    }
    ByteBuffer buffer = ByteBuffer.wrap(data);
    buffer.rewind();
    ByteBuffer readOnlyBuffer = buffer.asReadOnlyBuffer();

    RemoveDataNodePlan req = new RemoveDataNodePlan();
    readOnlyBuffer.getInt();
    req.deserializeImpl(readOnlyBuffer);
    return req;
  }
}
