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

package org.apache.iotdb.db.queryengine.plan.parser;

import org.apache.iotdb.db.queryengine.plan.statement.Statement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.RemoveDataNodeStatement;

import org.junit.Test;

import java.time.ZoneId;
import java.util.Arrays;
import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Parsing tests for the tree-model SQL that lets REMOVE DATANODE remove multiple DataNodes in a
 * single statement.
 */
public class RemoveDataNodeMultiNodeParseTest {

  private static Statement parse(String sql) {
    return StatementGenerator.createStatement(sql, ZoneId.systemDefault());
  }

  @Test
  public void testRemoveSingleDataNode() {
    Statement statement = parse("remove datanode 3");
    assertTrue(statement instanceof RemoveDataNodeStatement);
    assertEquals(Collections.singleton(3), ((RemoveDataNodeStatement) statement).getNodeIds());
  }

  @Test
  public void testRemoveMultipleDataNodes() {
    Statement statement = parse("remove datanode 3, 4, 5");
    assertTrue(statement instanceof RemoveDataNodeStatement);
    RemoveDataNodeStatement removeDataNodeStatement = (RemoveDataNodeStatement) statement;
    assertEquals(3, removeDataNodeStatement.getNodeIds().size());
    assertTrue(removeDataNodeStatement.getNodeIds().containsAll(Arrays.asList(3, 4, 5)));
  }
}
