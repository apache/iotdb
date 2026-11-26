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
import org.apache.iotdb.db.queryengine.plan.statement.metadata.RemoveConfigNodeStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.RemoveDataNodeStatement;

import org.antlr.v4.runtime.misc.ParseCancellationException;
import org.junit.Test;

import java.time.ZoneId;
import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/** Parsing tests for tree-model REMOVE DATANODE and REMOVE CONFIGNODE statements. */
public class RemoveNodeSingleNodeParseTest {

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
  public void testRemoveSingleConfigNode() {
    Statement statement = parse("remove confignode 3");
    assertTrue(statement instanceof RemoveConfigNodeStatement);
    assertEquals(3, ((RemoveConfigNodeStatement) statement).getNodeId().intValue());
  }

  @Test
  public void testRejectRemovingMultipleNodes() {
    assertThrows(ParseCancellationException.class, () -> parse("remove datanode 3, 4"));
    assertThrows(ParseCancellationException.class, () -> parse("remove confignode 3, 4"));
  }
}
