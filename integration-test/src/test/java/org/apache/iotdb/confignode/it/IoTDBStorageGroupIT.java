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
package org.apache.iotdb.confignode.it;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.common.rpc.thrift.TSetTTLReq;
import org.apache.iotdb.commons.client.sync.SyncConfigNodeIServiceClient;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.confignode.rpc.thrift.TCountStorageGroupResp;
import org.apache.iotdb.confignode.rpc.thrift.TDeleteStorageGroupsReq;
import org.apache.iotdb.confignode.rpc.thrift.TSetDataReplicationFactorReq;
import org.apache.iotdb.confignode.rpc.thrift.TSetSchemaReplicationFactorReq;
import org.apache.iotdb.confignode.rpc.thrift.TSetStorageGroupReq;
import org.apache.iotdb.confignode.rpc.thrift.TSetTimePartitionIntervalReq;
import org.apache.iotdb.confignode.rpc.thrift.TStorageGroupSchema;
import org.apache.iotdb.confignode.rpc.thrift.TStorageGroupSchemaResp;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.thrift.TException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

@RunWith(IoTDBTestRunner.class)
@Category({ClusterIT.class})
public class IoTDBStorageGroupIT {

  @Before
  public void setUp() throws Exception {
    EnvFactory.getEnv().initBeforeClass();
  }

  @After
  public void tearDown() {
    EnvFactory.getEnv().cleanAfterClass();
  }

  @Test
  public void testSetAndQueryStorageGroup() throws IOException, TException, IllegalPathException {
    TSStatus status;
    final String sg0 = "root.sg0";
    final String sg1 = "root.sg1";

    try (SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) EnvFactory.getEnv().getLeaderConfigNodeConnection()) {
      // set StorageGroup0 by default values
      TSetStorageGroupReq setReq0 = new TSetStorageGroupReq(new TStorageGroupSchema(sg0));
      status = client.setStorageGroup(setReq0);
      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

      // set StorageGroup1 by specific values
      TSetStorageGroupReq setReq1 =
          new TSetStorageGroupReq(
              new TStorageGroupSchema(sg1)
                  .setTTL(1024L)
                  .setSchemaReplicationFactor(5)
                  .setDataReplicationFactor(5)
                  .setTimePartitionInterval(2048L));
      status = client.setStorageGroup(setReq1);
      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

      // test count all StorageGroups
      TCountStorageGroupResp countResp =
          client.countMatchedStorageGroups(Arrays.asList("root", "**"));
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), countResp.getStatus().getCode());
      Assert.assertEquals(2, countResp.getCount());

      // test count one StorageGroup
      countResp = client.countMatchedStorageGroups(Arrays.asList("root", "sg0"));
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), countResp.getStatus().getCode());
      Assert.assertEquals(1, countResp.getCount());

      // test query all StorageGroupSchemas
      TStorageGroupSchemaResp getResp =
          client.getMatchedStorageGroupSchemas(Arrays.asList("root", "**"));
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), getResp.getStatus().getCode());
      Map<String, TStorageGroupSchema> schemaMap = getResp.getStorageGroupSchemaMap();
      Assert.assertEquals(2, schemaMap.size());
      TStorageGroupSchema storageGroupSchema = schemaMap.get(sg0);
      Assert.assertNotNull(storageGroupSchema);
      Assert.assertEquals(sg0, storageGroupSchema.getName());
      Assert.assertEquals(Long.MAX_VALUE, storageGroupSchema.getTTL());
      Assert.assertEquals(1, storageGroupSchema.getSchemaReplicationFactor());
      Assert.assertEquals(1, storageGroupSchema.getDataReplicationFactor());
      Assert.assertEquals(604800000, storageGroupSchema.getTimePartitionInterval());
      storageGroupSchema = schemaMap.get(sg1);
      Assert.assertNotNull(storageGroupSchema);
      Assert.assertEquals(sg1, storageGroupSchema.getName());
      Assert.assertEquals(1024L, storageGroupSchema.getTTL());
      Assert.assertEquals(5, storageGroupSchema.getSchemaReplicationFactor());
      Assert.assertEquals(5, storageGroupSchema.getDataReplicationFactor());
      Assert.assertEquals(2048L, storageGroupSchema.getTimePartitionInterval());

      // test fail by re-register
      status = client.setStorageGroup(setReq0);
      Assert.assertEquals(
          TSStatusCode.STORAGE_GROUP_ALREADY_EXISTS.getStatusCode(), status.getCode());

      // test StorageGroup setter interfaces
      PartialPath patternPath = new PartialPath(sg1);
      status = client.setTTL(new TSetTTLReq(Arrays.asList(patternPath.getNodes()), Long.MAX_VALUE));
      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
      status = client.setSchemaReplicationFactor(new TSetSchemaReplicationFactorReq(sg1, 1));
      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
      status = client.setDataReplicationFactor(new TSetDataReplicationFactorReq(sg1, 1));
      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
      status = client.setTimePartitionInterval(new TSetTimePartitionIntervalReq(sg1, 604800L));
      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

      // test setter results
      getResp = client.getMatchedStorageGroupSchemas(Arrays.asList("root", "sg1"));
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), getResp.getStatus().getCode());
      schemaMap = getResp.getStorageGroupSchemaMap();
      Assert.assertEquals(1, schemaMap.size());
      storageGroupSchema = schemaMap.get(sg1);
      Assert.assertNotNull(storageGroupSchema);
      Assert.assertEquals(sg1, storageGroupSchema.getName());
      Assert.assertEquals(Long.MAX_VALUE, storageGroupSchema.getTTL());
      Assert.assertEquals(1, storageGroupSchema.getSchemaReplicationFactor());
      Assert.assertEquals(1, storageGroupSchema.getDataReplicationFactor());
      Assert.assertEquals(604800, storageGroupSchema.getTimePartitionInterval());
    }
  }

  @Test
  public void testDeleteStorageGroup() throws TException, IOException {
    TSStatus status;
    final String sg0 = "root.sg0";
    final String sg1 = "root.sg1";

    try (SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) EnvFactory.getEnv().getLeaderConfigNodeConnection()) {
      TSetStorageGroupReq setReq0 = new TSetStorageGroupReq(new TStorageGroupSchema(sg0));
      // set StorageGroup0 by default values
      status = client.setStorageGroup(setReq0);
      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
      // set StorageGroup1 by specific values
      TSetStorageGroupReq setReq1 = new TSetStorageGroupReq(new TStorageGroupSchema(sg1));
      status = client.setStorageGroup(setReq1);
      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
      TDeleteStorageGroupsReq deleteStorageGroupsReq = new TDeleteStorageGroupsReq();
      List<String> sgs = Arrays.asList(sg0, sg1);
      deleteStorageGroupsReq.setPrefixPathList(sgs);
      TSStatus deleteSgStatus = client.deleteStorageGroups(deleteStorageGroupsReq);
      TStorageGroupSchemaResp root =
          client.getMatchedStorageGroupSchemas(Arrays.asList("root", "*"));
      Assert.assertTrue(root.getStorageGroupSchemaMap().isEmpty());
      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), deleteSgStatus.getCode());
    }
  }
}
