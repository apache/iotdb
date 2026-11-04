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

package org.apache.iotdb.db.queryengine.plan.execution.config.metadata;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.confignode.rpc.thrift.TDatabaseSchema;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.DatabaseSchemaStatement;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class DatabaseSchemaTaskTest {

  @Test
  public void testConstructDatabaseSchemaDoesNotSetNeedLastCacheWhenAbsent() throws Exception {
    final DatabaseSchemaStatement statement =
        new DatabaseSchemaStatement(DatabaseSchemaStatement.DatabaseSchemaStatementType.ALTER);
    statement.setDatabasePath(new PartialPath("root.sg"));

    final TDatabaseSchema databaseSchema = DatabaseSchemaTask.constructDatabaseSchema(statement);

    assertFalse(databaseSchema.isSetNeedLastCache());
  }

  @Test
  public void testConstructDatabaseSchemaSetsNeedLastCacheWhenPresent() throws Exception {
    final DatabaseSchemaStatement statement =
        new DatabaseSchemaStatement(DatabaseSchemaStatement.DatabaseSchemaStatementType.ALTER);
    statement.setDatabasePath(new PartialPath("root.sg"));
    statement.setNeedLastCache(false);

    final TDatabaseSchema databaseSchema = DatabaseSchemaTask.constructDatabaseSchema(statement);

    assertTrue(databaseSchema.isSetNeedLastCache());
    assertEquals(false, databaseSchema.isNeedLastCache());
  }
}
