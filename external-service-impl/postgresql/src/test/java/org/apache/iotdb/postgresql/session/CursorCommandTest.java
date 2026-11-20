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

package org.apache.iotdb.postgresql.session;

import org.apache.iotdb.postgresql.protocol.PgException;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class CursorCommandTest {

  private static CursorCommand parse(String sql) throws PgException {
    return CursorCommand.parse(sql).orElseThrow(() -> new AssertionError("not a cursor: " + sql));
  }

  private static void refused(String sql, String sqlState) {
    try {
      CursorCommand.parse(sql);
      fail("refused: " + sql);
    } catch (PgException e) {
      assertEquals(e.getMessage(), sqlState, e.sqlState());
    }
  }

  @Test
  public void declare() throws Exception {
    CursorCommand c = parse("DECLARE \"big\" CURSOR FOR SELECT * FROM refdb.events");
    assertEquals(CursorCommand.Kind.DECLARE, c.kind());
    assertEquals("big", c.name());
    assertEquals("SELECT * FROM refdb.events", c.query());
    assertFalse(c.hold());
    c = parse("declare Cur binary no scroll cursor with hold for\nselect 1");
    assertEquals("cur", c.name());
    assertTrue(c.hold());
    assertTrue(c.binary());
    assertEquals("select 1", c.query());
    refused("DECLARE c SCROLL CURSOR FOR SELECT 1", "0A000");
  }

  @Test
  public void fetchAndMove() throws Exception {
    CursorCommand c = parse("FETCH FORWARD 5000 FROM \"big\"");
    assertEquals(CursorCommand.Kind.FETCH, c.kind());
    assertEquals(5000, c.count());
    assertEquals("big", c.name());
    assertEquals(1, parse("FETCH big").count());
    assertEquals(1, parse("FETCH NEXT FROM big").count());
    assertEquals(0, parse("FETCH ALL IN big").count());
    assertEquals(0, parse("fetch forward all from big").count());
    assertEquals(10, parse("FETCH 10 big").count());
    c = parse("MOVE FORWARD 3 IN c");
    assertEquals(CursorCommand.Kind.MOVE, c.kind());
    assertEquals(3, c.count());
    refused("FETCH BACKWARD 1 FROM c", "0A000");
    refused("FETCH PRIOR FROM c", "0A000");
    refused("FETCH ABSOLUTE 5 FROM c", "0A000");
    refused("FETCH -1 FROM c", "0A000");
    refused("FETCH 0 FROM c", "0A000");
  }

  @Test
  public void close() throws Exception {
    assertEquals("big", parse("CLOSE \"big\"").name());
    assertNull(parse("close all").name());
  }

  @Test
  public void otherStatementsAreNotCursors() throws Exception {
    assertFalse(CursorCommand.parse("SELECT 1").isPresent());
    assertFalse(CursorCommand.parse("SET x = 1").isPresent());
  }
}
