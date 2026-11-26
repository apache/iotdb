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

package org.apache.iotdb.db.queryengine.plan.relational.sql.parser;

import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Statement;
import org.apache.iotdb.db.protocol.session.IClientSession;
import org.apache.iotdb.db.protocol.session.InternalClientSession;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.MigrateRegion;

import org.junit.Before;
import org.junit.Test;

import java.time.ZoneId;
import java.util.Arrays;
import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Parsing tests for the table-model SQL that lets MIGRATE REGION move multiple regions from a
 * single source DataNode to a single destination DataNode in one statement.
 */
public class MigrateRegionMultiRegionStatementTest {

  private SqlParser sqlParser;
  private IClientSession clientSession;

  @Before
  public void setUp() {
    sqlParser = new SqlParser();
    clientSession = new InternalClientSession("testClient");
  }

  private Statement parse(String sql) {
    return sqlParser.createStatement(sql, ZoneId.systemDefault(), clientSession);
  }

  @Test
  public void testMigrateSingleRegion() {
    Statement statement = parse("migrate region 1 from 2 to 3");
    assertTrue(statement instanceof MigrateRegion);
    MigrateRegion migrateRegion = (MigrateRegion) statement;
    assertEquals(Collections.singletonList(1), migrateRegion.getRegionIds());
    assertEquals(2, migrateRegion.getFromId());
    assertEquals(3, migrateRegion.getToId());
  }

  @Test
  public void testMigrateMultipleRegions() {
    Statement statement = parse("migrate region 1, 2, 3 from 4 to 5");
    assertTrue(statement instanceof MigrateRegion);
    MigrateRegion migrateRegion = (MigrateRegion) statement;
    assertEquals(Arrays.asList(1, 2, 3), migrateRegion.getRegionIds());
    assertEquals(4, migrateRegion.getFromId());
    assertEquals(5, migrateRegion.getToId());
  }
}
