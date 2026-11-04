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

package org.apache.iotdb.confignode.procedure.impl.schema.table;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.schema.table.TsTable;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlanType;
import org.apache.iotdb.confignode.consensus.request.write.database.DatabaseSchemaPlan;
import org.apache.iotdb.confignode.consensus.request.write.table.CommitCreateTablePlan;
import org.apache.iotdb.confignode.consensus.request.write.table.PreCreateTablePlan;
import org.apache.iotdb.confignode.consensus.request.write.table.SetTablePropertiesPlan;
import org.apache.iotdb.confignode.manager.ConfigManager;
import org.apache.iotdb.confignode.manager.consensus.ConsensusManager;
import org.apache.iotdb.confignode.manager.partition.PartitionManager;
import org.apache.iotdb.confignode.manager.schema.ClusterSchemaManager;
import org.apache.iotdb.confignode.manager.schema.ClusterSchemaQuotaStatistics;
import org.apache.iotdb.confignode.persistence.schema.ClusterSchemaInfo;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.store.ProcedureType;
import org.apache.iotdb.confignode.rpc.thrift.TDatabaseSchema;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import static org.apache.iotdb.confignode.procedure.state.schema.SetTablePropertiesState.SET_PROPERTIES;

public class SetTablePropertiesProcedureTest {
  @Test
  public void testRollbackRestoresMissingTableTtlAfterDatabaseDefaultChanges() throws Exception {
    final String database = "ttl_rollback_test";
    final String tableName = "table1";
    final ClusterSchemaInfo schemaInfo = new ClusterSchemaInfo();
    schemaInfo.createDatabase(
        new DatabaseSchemaPlan(
            ConfigPhysicalPlanType.CreateDatabase,
            new TDatabaseSchema(database).setIsTableModel(true).setTTL(86400000L)));
    schemaInfo.preCreateTable(new PreCreateTablePlan(database, new TsTable(tableName)));
    schemaInfo.commitCreateTable(new CommitCreateTablePlan(database, tableName));
    final ConfigManager configManager = Mockito.mock(ConfigManager.class);
    final PartitionManager partitionManager = Mockito.mock(PartitionManager.class);
    final ConsensusManager consensusManager = Mockito.mock(ConsensusManager.class);
    Mockito.when(configManager.getPartitionManager()).thenReturn(partitionManager);
    Mockito.when(partitionManager.isDatabaseExist(database)).thenReturn(true);
    Mockito.when(configManager.getConsensusManager()).thenReturn(consensusManager);
    final ClusterSchemaManager manager =
        new ClusterSchemaManager(
            configManager, schemaInfo, Mockito.mock(ClusterSchemaQuotaStatistics.class));
    Mockito.when(configManager.getClusterSchemaManager()).thenReturn(manager);
    final ConfigNodeProcedureEnv env = Mockito.mock(ConfigNodeProcedureEnv.class);
    Mockito.when(env.getConfigManager()).thenReturn(configManager);
    final Map<String, String> properties = new HashMap<>();
    properties.put(TsTable.TTL_PROPERTY, null);
    final SetTablePropertiesProcedure procedure =
        new SetTablePropertiesProcedure(database, tableName, "0", properties, false);
    procedure.validateTable(env);
    Assert.assertFalse(procedure.isFailed());
    Assert.assertEquals(86400000L, procedure.table.getTableTTL());
    Assert.assertTrue(procedure.originalProperties.containsKey(TsTable.TTL_PROPERTY));
    Assert.assertNull(procedure.originalProperties.get(TsTable.TTL_PROPERTY));
    Assert.assertEquals(
        Long.MAX_VALUE, manager.getTableIfExists(database, tableName).get().getTableTTL());

    // Recover the captured original properties, then fail the write after the default changes.
    final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    procedure.serialize(new DataOutputStream(bytes));
    final ByteBuffer buffer = ByteBuffer.wrap(bytes.toByteArray());
    buffer.getShort();
    final SetTablePropertiesProcedure recovered = new SetTablePropertiesProcedure(false);
    recovered.deserialize(buffer);
    schemaInfo.alterDatabase(
        new DatabaseSchemaPlan(
            ConfigPhysicalPlanType.AlterDatabase,
            new TDatabaseSchema(database).setIsTableModel(true).setTTL(172800000L)));
    Mockito.when(consensusManager.write(Mockito.any(SetTablePropertiesPlan.class)))
        .thenReturn(new TSStatus(TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode()))
        .thenAnswer(invocation -> schemaInfo.setTableProperties(invocation.getArgument(0)));
    recovered.executeFromState(env, SET_PROPERTIES);
    Assert.assertTrue(recovered.isFailed());
    recovered.rollbackState(env, SET_PROPERTIES);
    Assert.assertEquals(
        Long.MAX_VALUE, manager.getTableIfExists(database, tableName).get().getTableTTL());
    Mockito.verify(consensusManager, Mockito.times(2))
        .write(Mockito.any(SetTablePropertiesPlan.class));
  }

  @Test
  public void serializeDeserializeTest() throws IllegalPathException, IOException {
    final SetTablePropertiesProcedure setTablePropertiesProcedure =
        new SetTablePropertiesProcedure(
            "database1",
            "table1",
            "0",
            new HashMap<String, String>() {
              {
                put("prop1", "value1");
                put("ttl", null);
              }
            },
            false);

    final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    final DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);
    setTablePropertiesProcedure.serialize(dataOutputStream);

    final ByteBuffer byteBuffer = ByteBuffer.wrap(byteArrayOutputStream.toByteArray());

    Assert.assertEquals(
        ProcedureType.SET_TABLE_PROPERTIES_PROCEDURE.getTypeCode(), byteBuffer.getShort());

    final SetTablePropertiesProcedure deserializedProcedure =
        new SetTablePropertiesProcedure(false);
    deserializedProcedure.deserialize(byteBuffer);

    Assert.assertEquals(setTablePropertiesProcedure, deserializedProcedure);
  }
}
