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

  private final TablePattern tablePattern = new TablePattern(true, "^db[0-9]", "a.*b");

  @Test
  public void testCreateOrUpdateDevice() {
    final CreateOrUpdateDevice trueInput =
        new CreateOrUpdateDevice(
            "db1", "ab", Collections.emptyList(), Collections.emptyList(), Collections.emptyList());
    final CreateOrUpdateDevice falseInput1 =
        new CreateOrUpdateDevice(
            "db1", "ac", Collections.emptyList(), Collections.emptyList(), Collections.emptyList());
    final CreateOrUpdateDevice falseInput2 =
        new CreateOrUpdateDevice(
            "da", "a2b", Collections.emptyList(), Collections.emptyList(), Collections.emptyList());

    Assert.assertEquals(
        trueInput,
        new PipeStatementTablePatternParseVisitor()
            .process(trueInput, tablePattern)
            .orElseThrow(AssertionError::new));
    Assert.assertFalse(
        new PipeStatementTablePatternParseVisitor().process(falseInput1, tablePattern).isPresent());
    Assert.assertFalse(
        new PipeStatementTablePatternParseVisitor().process(falseInput2, tablePattern).isPresent());
  }
}
