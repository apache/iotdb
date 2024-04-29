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

package org.apache.iotdb.confignode.manager.pipe.extractor;

import org.apache.iotdb.commons.pipe.pattern.IoTDBPipePattern;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlanType;
import org.apache.iotdb.confignode.consensus.request.write.database.DatabaseSchemaPlan;
import org.apache.iotdb.confignode.rpc.thrift.TDatabaseSchema;

import org.junit.Assert;
import org.junit.Test;

public class PipeConfigPhysicalPlanPatternParseVisitorTest {

  private final IoTDBPipePattern pattern = new IoTDBPipePattern("root.db.device.**");

  @Test
  public void testCreateDatabase() {
    final DatabaseSchemaPlan createDatabasePlan =
        new DatabaseSchemaPlan(
            ConfigPhysicalPlanType.CreateDatabase, new TDatabaseSchema("root.db"));
    final DatabaseSchemaPlan filteredCreateDatabasePlan =
        new DatabaseSchemaPlan(
            ConfigPhysicalPlanType.CreateDatabase, new TDatabaseSchema("root.db1"));

    Assert.assertEquals(
        createDatabasePlan,
        IoTDBConfigRegionExtractor.PATTERN_PARSE_VISITOR
            .visitCreateDatabase(createDatabasePlan, pattern)
            .orElseThrow(AssertionError::new));
    Assert.assertFalse(
        IoTDBConfigRegionExtractor.PATTERN_PARSE_VISITOR
            .visitCreateDatabase(filteredCreateDatabasePlan, pattern)
            .isPresent());
  }
}
