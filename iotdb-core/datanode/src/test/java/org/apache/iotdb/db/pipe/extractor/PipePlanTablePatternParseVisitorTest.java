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

package org.apache.iotdb.db.pipe.extractor;

import org.apache.iotdb.commons.pipe.datastructure.pattern.TablePattern;
import org.apache.iotdb.db.pipe.extractor.schemaregion.IoTDBSchemaRegionExtractor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.schema.CreateOrUpdateTableDeviceNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.schema.TableDeviceAttributeUpdateNode;

import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;

public class PipePlanTablePatternParseVisitorTest {
  private final TablePattern tablePattern = new TablePattern(true, "^db[0-9]", "a.*b");

  @Test
  public void testCreateOrUpdateTableDevice() {
    testInput(
        new CreateOrUpdateTableDeviceNode(
            new PlanNodeId(""),
            "db1",
            "ab",
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.emptyList()),
        new CreateOrUpdateTableDeviceNode(
            new PlanNodeId(""),
            "db1",
            "ac",
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.emptyList()),
        new CreateOrUpdateTableDeviceNode(
            new PlanNodeId(""),
            "da",
            "a2b",
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.emptyList()));
  }

  @Test
  public void testTableDeviceAttributeUpdate() {
    testInput(
        new TableDeviceAttributeUpdateNode(
            new PlanNodeId(""), "db1", "ab", null, null, null, null, null, null),
        new TableDeviceAttributeUpdateNode(
            new PlanNodeId(""), "db1", "ac", null, null, null, null, null, null),
        new TableDeviceAttributeUpdateNode(
            new PlanNodeId(""), "da", "a2b", null, null, null, null, null, null));
  }

  private void testInput(
      final PlanNode trueInput, final PlanNode falseInput1, final PlanNode falseInput2) {
    Assert.assertEquals(
        trueInput,
        IoTDBSchemaRegionExtractor.TABLE_PATTERN_PARSE_VISITOR
            .process(trueInput, tablePattern)
            .orElseThrow(AssertionError::new));
    Assert.assertFalse(
        IoTDBSchemaRegionExtractor.TABLE_PATTERN_PARSE_VISITOR
            .process(falseInput1, tablePattern)
            .isPresent());
    Assert.assertFalse(
        IoTDBSchemaRegionExtractor.TABLE_PATTERN_PARSE_VISITOR
            .process(falseInput2, tablePattern)
            .isPresent());
  }
}
