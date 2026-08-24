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
package org.apache.iotdb.confignode.manager;

import org.apache.iotdb.commons.schema.table.TsTable;
import org.apache.iotdb.commons.schema.table.TsTableInternalRPCUtil;
import org.apache.iotdb.confignode.manager.schema.ClusterSchemaManager;
import org.apache.iotdb.confignode.manager.schema.ClusterSchemaQuotaStatistics;
import org.apache.iotdb.confignode.persistence.schema.ClusterSchemaInfo;
import org.apache.iotdb.confignode.rpc.thrift.TDatabaseSchema;

import org.apache.tsfile.utils.Pair;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ClusterSchemaManagerTest {

  @Test
  public void testCalcMaxRegionGroupNum() {

    // The maxRegionGroupNum should be great or equal to the leastRegionGroupNum
    Assert.assertEquals(100, ClusterSchemaManager.calcMaxRegionGroupNum(100, 1.0, 3, 1, 3, 0));

    // The maxRegionGroupNum should be great or equal to the allocatedRegionGroupCount
    Assert.assertEquals(100, ClusterSchemaManager.calcMaxRegionGroupNum(3, 1.0, 6, 2, 3, 100));

    // (resourceWeight * resource) / (createdStorageGroupNum * replicationFactor)
    Assert.assertEquals(20, ClusterSchemaManager.calcMaxRegionGroupNum(3, 1.0, 120, 2, 3, 5));
  }

  @Test
  public void testNeedLastCacheDefaultsToTrueWhenUnset() {
    final TDatabaseSchema unsetSchema = new TDatabaseSchema();
    Assert.assertTrue(ClusterSchemaManager.isNeedLastCacheEnabled(unsetSchema));

    final TDatabaseSchema enabledSchema = new TDatabaseSchema();
    enabledSchema.setNeedLastCache(true);
    Assert.assertTrue(ClusterSchemaManager.isNeedLastCacheEnabled(enabledSchema));

    final TDatabaseSchema disabledSchema = new TDatabaseSchema();
    disabledSchema.setNeedLastCache(false);
    Assert.assertFalse(ClusterSchemaManager.isNeedLastCacheEnabled(disabledSchema));
  }

  @Test
  public void testGetAllTableInfoForDataNodeActivationWithDeletedDatabase() {
    final IManager configManager = Mockito.mock(IManager.class);
    final ProcedureManager procedureManager = Mockito.mock(ProcedureManager.class);
    final ClusterSchemaInfo clusterSchemaInfo = Mockito.mock(ClusterSchemaInfo.class);

    Mockito.when(configManager.getProcedureManager()).thenReturn(procedureManager);
    Mockito.when(procedureManager.getAllExecutingTables())
        .thenReturn(Collections.singletonMap("test", null));
    Mockito.when(clusterSchemaInfo.getAllUsingTables()).thenReturn(new HashMap<>());
    Mockito.when(clusterSchemaInfo.getAllPreDeleteTables()).thenReturn(new HashMap<>());
    Mockito.when(clusterSchemaInfo.getAllPreCreateTables()).thenReturn(new HashMap<>());

    final ClusterSchemaManager clusterSchemaManager =
        new ClusterSchemaManager(
            configManager, clusterSchemaInfo, Mockito.mock(ClusterSchemaQuotaStatistics.class));

    final Pair<Map<String, List<TsTable>>, Map<String, List<TsTable>>> tableInfo =
        TsTableInternalRPCUtil.deserializeTableInitializationInfo(
            clusterSchemaManager.getAllTableInfoForDataNodeActivation());

    Assert.assertTrue(tableInfo.left.isEmpty());
    Assert.assertEquals(Collections.singleton("test"), tableInfo.right.keySet());
    Assert.assertTrue(tableInfo.right.get("test").isEmpty());
  }
}
