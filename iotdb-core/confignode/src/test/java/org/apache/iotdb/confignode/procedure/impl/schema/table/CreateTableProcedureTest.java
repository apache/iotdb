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

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.exception.IoTDBException;
import org.apache.iotdb.commons.exception.table.TableInDeletionException;
import org.apache.iotdb.commons.schema.table.TableNodeStatus;
import org.apache.iotdb.commons.schema.table.TsTable;
import org.apache.iotdb.commons.schema.table.column.AttributeColumnSchema;
import org.apache.iotdb.commons.schema.table.column.FieldColumnSchema;
import org.apache.iotdb.commons.schema.table.column.TagColumnSchema;
import org.apache.iotdb.confignode.manager.ConfigManager;
import org.apache.iotdb.confignode.manager.schema.ClusterSchemaManager;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.store.ProcedureType;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.utils.Pair;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Optional;

public class CreateTableProcedureTest {
  @Test
  public void testPreDeletedTableIsNotReportedAsAlreadyExisting() throws Exception {
    final ConfigNodeProcedureEnv env = Mockito.mock(ConfigNodeProcedureEnv.class);
    final ConfigManager configManager = Mockito.mock(ConfigManager.class);
    final ClusterSchemaManager schemaManager = Mockito.mock(ClusterSchemaManager.class);
    Mockito.when(env.getConfigManager()).thenReturn(configManager);
    Mockito.when(configManager.getClusterSchemaManager()).thenReturn(schemaManager);
    final TsTable table = new TsTable("table1");
    Mockito.when(schemaManager.getTableAndStatusIfExists("database1", "table1"))
        .thenReturn(Optional.of(new Pair<>(table, TableNodeStatus.PRE_DELETE)));

    final CreateTableProcedure procedure = new CreateTableProcedure("database1", table, false);
    procedure.checkTableExistence(env);

    Assert.assertTrue(procedure.isFailed());
    Assert.assertTrue(procedure.getException().getCause() instanceof TableInDeletionException);
    Assert.assertEquals(
        TSStatusCode.SEMANTIC_ERROR.getStatusCode(),
        ((IoTDBException) procedure.getException().getCause()).getErrorCode());

    Mockito.when(schemaManager.getTableAndStatusIfExists("database1", "table1"))
        .thenReturn(Optional.of(new Pair<>(table, TableNodeStatus.USING)));
    final CreateTableProcedure duplicate = new CreateTableProcedure("database1", table, false);
    duplicate.checkTableExistence(env);
    Assert.assertEquals(
        TSStatusCode.TABLE_ALREADY_EXISTS.getStatusCode(),
        ((IoTDBException) duplicate.getException().getCause()).getErrorCode());
  }

  @Test
  public void serializeDeserializeTest() throws IllegalPathException, IOException {
    final TsTable table = new TsTable("table1");
    table.addColumnSchema(new TagColumnSchema("Id", TSDataType.STRING));
    table.addColumnSchema(new AttributeColumnSchema("Attr", TSDataType.STRING));
    table.addColumnSchema(
        new FieldColumnSchema(
            "Measurement", TSDataType.DOUBLE, TSEncoding.GORILLA, CompressionType.SNAPPY));
    final CreateTableProcedure createTableProcedure =
        new CreateTableProcedure("database1", table, false);

    final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    final DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);
    createTableProcedure.serialize(dataOutputStream);

    final ByteBuffer byteBuffer = ByteBuffer.wrap(byteArrayOutputStream.toByteArray());

    Assert.assertEquals(ProcedureType.CREATE_TABLE_PROCEDURE.getTypeCode(), byteBuffer.getShort());

    final CreateTableProcedure deserializedProcedure = new CreateTableProcedure(false);
    deserializedProcedure.deserialize(byteBuffer);

    Assert.assertEquals(createTableProcedure.getDatabase(), deserializedProcedure.getDatabase());
    Assert.assertEquals(
        createTableProcedure.getTable().getTableName(),
        deserializedProcedure.getTable().getTableName());
    Assert.assertEquals(
        createTableProcedure.getTable().getColumnNum(),
        deserializedProcedure.getTable().getColumnNum());
    Assert.assertEquals(
        createTableProcedure.getTable().getTagNum(), deserializedProcedure.getTable().getTagNum());
  }
}
