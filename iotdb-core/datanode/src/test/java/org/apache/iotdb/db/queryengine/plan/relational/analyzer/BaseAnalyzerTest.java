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

package org.apache.iotdb.db.queryengine.plan.relational.analyzer;

import org.apache.iotdb.commons.schema.table.TsTable;
import org.apache.iotdb.db.schemaengine.table.DataNodeTableCache;

import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.util.Collections;
import java.util.Map;

public abstract class BaseAnalyzerTest {

  protected static final String testDb = "testdb";
  protected static final String testTable = "table1";

  @BeforeClass
  public static void setup() {
    TsTable tsTable = new TsTable(testTable);
    // disable alter table name to use full deviceId key for compatibility
    Map<String, String> properties =
        Collections.singletonMap(TsTable.ALLOW_ALTER_NAME_PROPERTY, "false");
    tsTable.setProps(properties);
    DataNodeTableCache.getInstance().preUpdateTable(testDb, tsTable, null, null);
    DataNodeTableCache.getInstance().commitUpdateTable(testDb, testTable, null);
  }

  @AfterClass
  public static void tearDown() {
    DataNodeTableCache.getInstance().invalid(testDb);
  }
}
