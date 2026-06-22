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

package org.apache.iotdb.db.pipe.sink;

import org.apache.iotdb.commons.pipe.datastructure.pattern.TablePattern;
import org.apache.iotdb.db.pipe.receiver.visitor.PipeStatementTablePatternParseVisitor;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.CreateOrUpdateDevice;

import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;

public class PipeStatementTablePatternParseVisitorTest {

  private static final String MATCH_DATABASE = "db1";
  private static final String MISMATCH_DATABASE = "da";
  private static final String MATCH_TABLE = "ab";
  private static final String MISMATCH_TABLE = "ac";

  private final TablePattern tablePattern = new TablePattern(true, "^db[0-9]", "a.*b");
  private final PipeStatementTablePatternParseVisitor visitor =
      new PipeStatementTablePatternParseVisitor();

  @Test
  public void testCreateOrUpdateDevice() {
    final CreateOrUpdateDevice matchedInput = createOrUpdateDevice(MATCH_DATABASE, MATCH_TABLE);

    Assert.assertEquals(
        matchedInput, visitor.process(matchedInput, tablePattern).orElseThrow(AssertionError::new));
    assertPatternFilters(createOrUpdateDevice(MATCH_DATABASE, MISMATCH_TABLE));
    assertPatternFilters(createOrUpdateDevice(MISMATCH_DATABASE, MATCH_TABLE));
  }

  @Test
  public void testCreateOrUpdateDeviceMatchesDefaultPattern() {
    final CreateOrUpdateDevice input = createOrUpdateDevice(MISMATCH_DATABASE, MISMATCH_TABLE);

    Assert.assertEquals(
        input,
        visitor
            .process(input, new TablePattern(true, null, null))
            .orElseThrow(AssertionError::new));
  }

  private void assertPatternFilters(final CreateOrUpdateDevice input) {
    Assert.assertFalse(visitor.process(input, tablePattern).isPresent());
  }

  private CreateOrUpdateDevice createOrUpdateDevice(final String database, final String table) {
    return new CreateOrUpdateDevice(
        database, table, Collections.emptyList(), Collections.emptyList(), Collections.emptyList());
  }
}
