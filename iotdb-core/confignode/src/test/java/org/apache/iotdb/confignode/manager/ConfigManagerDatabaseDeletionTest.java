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

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.confignode.manager.schema.ClusterSchemaManager;
import org.apache.iotdb.confignode.rpc.thrift.TDatabaseSchema;
import org.apache.iotdb.confignode.rpc.thrift.TDeleteDatabasesReq;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.apache.iotdb.commons.schema.table.Audit.TABLE_MODEL_AUDIT_DATABASE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ConfigManagerDatabaseDeletionTest {

  @Test
  public void testTableAuditDatabaseIsExcludedFromDeletion() {
    final ConfigManager manager = mock(ConfigManager.class);
    final ClusterSchemaManager schemaManager = mock(ClusterSchemaManager.class);
    when(manager.confirmLeader())
        .thenReturn(new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()));
    when(manager.getClusterSchemaManager()).thenReturn(schemaManager);

    final Map<String, TDatabaseSchema> schemas = new HashMap<>();
    schemas.put(
        TABLE_MODEL_AUDIT_DATABASE,
        new TDatabaseSchema(TABLE_MODEL_AUDIT_DATABASE).setIsTableModel(true));
    when(schemaManager.getMatchedDatabaseSchemasByName(
            Collections.singletonList(TABLE_MODEL_AUDIT_DATABASE), true))
        .thenReturn(schemas);
    final TDeleteDatabasesReq request =
        new TDeleteDatabasesReq(Collections.singletonList(TABLE_MODEL_AUDIT_DATABASE))
            .setIsTableModel(true);
    when(manager.deleteDatabases(request)).thenCallRealMethod();

    assertEquals(
        TSStatusCode.PATH_NOT_EXIST.getStatusCode(), manager.deleteDatabases(request).getCode());
    assertTrue(schemas.isEmpty());
  }
}
